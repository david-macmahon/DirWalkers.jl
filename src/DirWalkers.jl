module DirWalkers

using Base.Iterators: takewhile

export run_dirwalker
export DirQueue, WorkQueue, FileQueue, OutQueue
export RemoteDirQueue, RemoteWorkQueue, RemoteFileQueue, RemoteOutQueue

# One million should be enough, but OK to block if a queue gets full
DEFAULT_QUEUE_SIZE = 10^6

# Queue channel types
const DirQueue = Channel{String}
const WorkQueue = Channel{String}
const FileQueue = Channel{String}
const OutQueue{T} = Channel{Union{Nothing,T}}

"""
Return the number of items available in `q`.
"""
nitems(q::Channel) = Base.n_avail(q)

"""
Return the maximum number of items that `q` can hold.
"""
qsize(q::Channel) = q.sz_max

"""
    _control_loop(dirq, workq)

Runs a loop that takes `String`s from `dirq` and puts non-empty `String`s into
`workq`.  For each `String` put into `workq`, increment a "posted" counter.  For
each empty `String` taken from `dirq` increment a "completed" counter.  Exit the
loop when "completed" counter equals the "posted" counter and return number of
items put into workq (i.e. completed or posted counter).
"""
function _control_loop(dirq, workq)
    @debug "control agent starting"
    nposted = 0
    ncompleted = 0
    keep_running = true
    while keep_running
        for item in takewhile(!isempty, dirq)
            @debug "got item" item
            # Got a work item, put it in workq.  We can't do `isdir` check on
            # `item` here because the control agent may be running on a system
            # (e.g. a head node) that doesn't have access to the relevant
            # filesystem (e.g. `/datag`).
            @debug "putting item into workq"
            put!(workq, item)
            nposted += 1
            @debug "put item into workq" nposted ncompleted
        end

        # A work request completed, increment ncompleted
        ncompleted += 1
        @debug "got empty string from dirq" nposted ncompleted

        # Keep running if ncompleted is less than nposted
        keep_running = (ncompleted < nposted)
    end

    ncompleted
end

"""
    _process_dirs(id, dirq, workq, fileq; dirpred=_->true, filepred=_->true)

Takes directory names from `workq` until it gets an empty directory name, which
causes the function to return `(; host=hostname, id, t=elapsed_time, n=ndirs)`.
For each directory taken from `workq` its subdirectory entries are `put!` into
`dirq` if `dirpred` returns true and its files entries are `put!` into `fileq`
if `filepred` returns `true`.  `dirpred` and `filepred` are expected to be
functions that accept the directory or file name and return a boolean.  Symbolic
links are always ignored.
"""
function _process_dirs(id, dirq, workq, fileq;
    dirpred=_->true, filepred=_->true
)
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
                    # Add subdir item to dirq if dirpred returns true
                    if dirpred(item)
                        @debug "dagent $id adding directory $item to dirq"
                        put!(dirq, item)
                    else
                        @debug "dagent $id ignoring dir $item"
                    end
                elseif isfile(item)
                    # Add file path to fileq if filepred returns true
                    if filepred(item)
                        @debug "dagent $id adding file $item to fileq"
                        put!(fileq, item)
                    else
                        @debug "dagent $id ignoring file $item"
                    end
                else
                    @debug "dagent $id ignoring non-dir non-file $item"
                end
            end
        catch ex
            # TODO Make this @warn or @error?
            @debug "dagent $id error processing directory $dir\n$ex"
        finally
            # Indicate "agent done"
            @debug "dagent $id putting empty string (agent done) in dirq"
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

function _process_files(filefunc, id, fileq, outq, args...; kwargs...)
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

"""
    start_dagents(dirq, workq, fileq, agentspec::Integer;
        dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
    )

TBW
"""
function start_dagents(dirq, workq, fileq, agentspec::Integer;
    dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
)
    # Start dagent tasks
    map(1:agentspec) do id
        errormonitor(
            Threads.@spawn process_dirs(id, dirq, workq, fileq; dirpred, filepred)
        )
    end
end

function start_fagents(filefunc, fileq, outq, agentspec, args...;
    process_files=_process_files, kwargs...
)
    map(1:agentspec) do id
        errormonitor(
            Threads.@spawn process_files(filefunc, id, fileq, outq, args...; kwargs...)
        )
    end
end

function run_dirwalker(filefunc, dirq, workq, fileq, outq, topdirs, args...;
    dirpred=_->true, filepred=_->true, dagentspec=1, fagentspec=1, extraspec=nothing,
    process_dirs=_process_dirs, process_files=_process_files, kwargs...
)
    # topdirs cannot contain empty strings
    any(isempty, topdirs) && error("topdirs cannot contain empty names")

    # dirq must be able to hold all of topdirs
    qsize(dirq) < length(topdirs) && error("dirq is not large enough for topdirs")

    # Start dir agents
    dagents = start_dagents(dirq, workq, fileq, dagentspec; dirpred, filepred, process_dirs)

    # Start file agents
    fagents = start_fagents(filefunc, fileq, outq, fagentspec, args...; process_files, kwargs...)

    # Populate dirq.  This can lead to a deadlock if dirq is not deep enough
    # to hold all topdirs so we have an explicit check for that above.
    for item in topdirs
        # We can't do `isdir` checks on `topdirs` entries here because the
        # current process may be running on a system (e.g. a head node) that
        # doesn't have access to the relevant filesystem (e.g. `/datag`).
        put!(dirq, item)
    end

    # Start control agent
    cagent = errormonitor(Threads.@spawn _control_loop(dirq, workq))

    # Everything has been started!

    # Wait for control agent to finish
    @info "waiting for control agent to finish"
    ndirs = fetch(cagent)

    @debug "signaling completion to dir agents"
    for _ in dagents
        put!(workq, "")
    end

    @info "waiting for dir agents to complete"
    dagent_results = fetch.(dagents)

    # Startup extra file agents, if any
    if extraspec !== nothing
        append!(fagents, start_fagents(
            filefunc, fileq, outq, extraspec, args...; process_files, kwargs...
        ))
    end

    @debug "signaling completion to file agents"
    for _ in fagents
        put!(fileq, "")
    end

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
    ndirs, dagent_results, fagent_results
end

include("DistributedDirWalkers.jl")

end # module DirWalkers
