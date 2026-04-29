module DirWalkers

using Base.Iterators: takewhile

export run_dirwalker
export DirQueue, FileQueue, OutQueue
export RemoteDirQueue, RemoteFileQueue, RemoteOutQueue

const WORK_QUEUE_SIZE = 5 # 1 should be enough, but for now 5 seems a litte safer

# Abstract queue channel types
const AbstractDirQueue = AbstractChannel{String}
const AbstractFileQueue = AbstractChannel{String}
const AbstractOutQueue{T} = AbstractChannel{Union{Nothing,T}}

# Concrete queue channel types
const DirQueue = Channel{String}
const FileQueue = Channel{String}
const OutQueue{T} = Channel{Union{Nothing,T}}

"""
    _process_dirs(filepred, dirq, fileq, agentq, workq, id)

Takes directory names from `workq` until it gets an empty directory name, which
causes the function to return `(; host=hostname, id, t=elapsed_time, n=ndirs)`.
For each directory taken from `workq` the contents are `put!` into `dirq` or
`fileq`, as appropriate, and then put!'s `id` in `agentq` and an empty string in
`dirq`.  Directory entries that are symlinks are ignored.
"""
function _process_dirs(filepred, dirq, fileq, agentq, workq, id)
try
    start = time()
    ndirs = 0
    @debug "dagent $id starting at $start"
    for dir in takewhile(!isempty, workq)
        ndirs += 1
        @debug "dagent $id processing dir $dir"
        try
            # TODO add check for readability (once a v1.10 way is known!)
            paths = readdir(dir; join=true, sort=false)
            @debug "dagent $id found $(length(paths)) in $dir"

            # For each iten in dir
            for item in paths
                @debug "dagent $id processing $item"
                islink(item) && continue # skip symlinks
                if isdir(item)
                    # Add subdir item to dirq
                    #put!(dirq, (; id, item))
                    @debug "dagent $id adding directory $item to dirq"
                    put!(dirq, item)
                elseif isfile(item) && filepred(item)
                    # Add filepred-matching file path to fileq
                    @debug "dagent $id adding matching file $item to fileq"
                    put!(fileq, item)
                else
                    @debug "dagent $id ignoring non-matching file $item"
                end
            end
        catch ex
            # TODO Make this @warn or @error?
            @debug "dagent $id error processing directory $dir\n$ex"
        finally
            # TODO Does the ordering of these two put! statements matter?
            # Put id back into agentq
            @debug "dagent $id putting id back in agentq"
            put!(agentq, id)
            @debug """dagent $id putting "agent done" in dirq"""
            # Indicate "agent done"
            put!(dirq, "")
        end
        @debug "dagent $id end of dagent iteration"
    end

    return (; host=gethostname(), id, t=time()-start, n=ndirs)
catch ex
@show ex
    (; ex)
end
end

function _process_files(filefunc, fileq, outq, id, args...; kwargs...)
try
    start = time()
    nfiles = 0

    # Take from fileq until we get an empty string
    for file in takewhile(!isempty, fileq)
        try
            @debug "processing file $file"
            for item in filefunc(file, args...; kwargs...)
                put!(outq, item)
            end
            nfiles += 1
        catch ex
            @warn "got exception processing $file" ex
        end
    end

    return (; host=gethostname(), id, t=time()-start, n=nfiles)
catch ex
    (; ex)
end
end

function start_dagents(filepred, dirq, fileq, agentspec; process_dirs=_process_dirs)
    # Create queue for dagents
    agentq = Channel{Int}(agentspec)

    # Start dagent tasks and create agent_id=>(; workq, fetchable) map
    agentidmap = map(1:agentspec) do agent_id
        workq = Channel{String}(WORK_QUEUE_SIZE)
        fetchable = errormonitor(
            Threads.@spawn process_dirs(filepred, dirq, fileq, agentq, workq, agent_id)
        )
        # Add agent_id to agentq
        put!(agentq, agent_id)
        # Pair mapping agent_id to NamedTuple of workq and fetchable (i.e. Task)
        agent_id => (; workq, fetchable)
    end |> Dict

    # Reurn agentidmap and agentq
    agentidmap, agentq
end

function start_fagents(filefunc, fileq, outq, agentspec, args...; process_files=_process_files, kwargs...)
    map(1:agentspec) do agent_id
        errormonitor(
            Threads.@spawn process_files(filefunc, fileq, outq, agent_id, args...; kwargs...)
        )
    end
end

function run_dirwalker(filefunc, dirq, fileq, outq, topdirs, args...;
    filepred=_->true, dagentspec=1, fagentspec=1, extraspec=nothing,
    process_dirs=_process_dirs, process_files=_process_files, kwargs...
)
    any(isempty, topdirs) && error("topdirs cannot contain empty names")

    # Start dir agents.  start_dagents handles creation of agentq because
    # its sizing depends on how dagentspec in interpretted (i.e. as a
    # (local) dagent task count vs a list of (distributed) dagent workers).
    dagentmap, dagentq = start_dagents(filepred, dirq, fileq, dagentspec; process_dirs)

    # Start file agents
    fagents = start_fagents(filefunc, fileq, outq, fagentspec, args...; process_files, kwargs...)

    # Populate dirq.  Currently, processing of dirq happens synchronously after
    # dirq is populated.  This can lead to a deadlock if dirq is not deep enough
    # to hold all topdirs.
    #
    # TODO Process dirq in an async Task that is started before populating dirq.
    for item in topdirs
        # We can't do `isdir` checks on `topdirs` entries here because the
        # current process may be running on a system (e.g. a head node) that
        # doesn't have access to the relevant filesystem (e.g. `/datag`).
        put!(dirq, item)
    end

    # Process dirq (TODO: make this a function)
    @debug "processing dirq"
    npending = 0
    keep_running = true
    while keep_running
        for item in Iterators.takewhile(!isempty, dirq)
            @debug "got item" item
            # Got a work item, get an available dagent
            @debug "getting dagent from dagentq"
            id = take!(dagentq)
            @debug "got dagent $id"
            # Get dagent's workq from dagentmap
            workq = dagentmap[id].workq
            # put! work item into dagent's work queue
            @debug "putting item into workq" workq
            put!(workq, item)
            npending += 1
            @debug "put item into workq" npending
        end

        # A work request completed, decrement npending
        npending -= 1
        # Keep running if any work requests are still pending
        keep_running = (npending > 0)
        @debug "got empty string from dirq" npending keep_running
    end

    # Put empty string into workqs to signify end of input and then wait for
    # dagents to finish by fetching results.
    @debug "signaling completion to dir agents"
    for workq in first.(values(dagentmap))
        put!(workq, "")
    end
    @info "waiting for dir agents to complete"
    dagent_results = fetch.(last.(values(dagentmap)))

    # Startup extra file agents, if any
    if extraspec !== nothing
        append!(fagents, start_fagents(
            filefunc, fileq, outq, extraspec, args...; process_files, kwargs...
        ))
    end

    # Put an empty string into fileq for each fagent
    for _ in fagents
        put!(fileq, "")
    end

    # Wait for file agents to complete by fetching results
    @info "waiting for file agents to complete"
    fagent_results = fetch.(fagents)

    # Put nothing into outq.  We don't know how many output handlers are
    # processing `outq` (that's up to the user), so we just put one `nothing`
    # into `outq` and then return.  If the user runs multiple handlers for
    # `outq`, they should recycle the `nothing` back into `outq` before
    # returning.
    put!(outq, nothing)

    @info "run_dirwalker done"

    # "Return" dagent results and fagent results
    dagent_results, fagent_results
end

include("DistributedDirWalkers.jl")

end # module DirWalkers
