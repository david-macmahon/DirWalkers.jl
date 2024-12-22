module DirWalkers

export DirWalker, start_dirwalker

const WORK_QUEUE_SIZE = 5 # 1 should be enough, but for now 5 seems a litte safer

#=
struct DirWalkerOld{T,C<:Channel}
    dirq::C{String}
    fileq::C{String}
    outq::C{Union{Nothing,T}}
end
=#
abstract type AbstractDirWalker{T} end

struct DirWalker{T} <: AbstractDirWalker{T}
    dirq::Channel{String}
    fileq::Channel{String}
    outq::Channel{Union{Nothing,T}}
end

#=
function DirWalker{T,C}(d,f,o) where {T, C<:AbstractChannel}
    DirWalker{T,C{String},C{Union{Nothing,T}}}(d,f,o)
end
=#

function DirWalker{T}(::Type{Channel}; dqsize=10_000, fqsize=10_000, oqsize=10_000)::AbstractDirWalker where {T}
    dirq = Channel{String}(dqsize)
    fileq = Channel{String}(fqsize)
    outq = Channel{Union{Nothing,T}}(oqsize)
    DirWalker(dirq, fileq, outq)
end

function DirWalker{T}(; dqsize=10_000, fqsize=10_000, oqsize=10_000)::AbstractDirWalker where T
    DirWalker{T}(Channel; dqsize, fqsize, oqsize)
#=
    dirq = Channel{String}(dqsize)
    fileq = Channel{String}(fqsize)
    outq = Channel{Union{Nothing,T}}(oqsize)
    DirWalker{T,Channel{String},Channel{Union{Nothing,String}}}(dirq, fileq, outq)
=#
end

DirWalker(; dqsize=10_000, fqsize=10_000, oqsize=10_000)::AbstractDirWalker = DirWalker{Any}(; dqsize, fqsize, oqsize)

# TODO Maybe create getters for dirq,fileq,outq?
Base.put!(dw::AbstractDirWalker, dir) = put!(dw.dirq, dir)
Base.take!(dw::AbstractDirWalker) = take!(dw.outq)
Base.isready(dw::AbstractDirWalker) = isready(dw.outq)

# Iteration for AbstractDirWalker `take`s from `outq` until getting `nothing`.

Base.IteratorSize(::Type{<:AbstractDirWalker}) = Base.SizeUnknown()
Base.eltype(_::AbstractDirWalker{T}) where T = T

function Base.iterate(dw::AbstractDirWalker, _=nothing)
    e = take!(dw)
    ifelse(e===nothing, nothing, (e, nothing))
end

"""
Return a tuple of the number of available items in the `dirq`, `fileq`, `outq`
channels of `dw`.
"""
function Base.n_avail(dw::AbstractDirWalker)
    Base.n_avail(dw.dirq), Base.n_avail(dw.fileq), Base.n_avail(dw.outq)
end

"""
    _process_dirs(filepred, dw::AbstractDirWalker, id, agentq, workq)

Takes directory names from `workq` until it gets an empty directory name, which
causes the function to return `(; t=elapsed_time, n=ndirs)`.  For each directory
taken from `workq` the contents are `put!` into `dw.dirq` or `dw.fileq`, as
appropriate, and then put!'s `id` in `agentq` and an empty string in `dw.dirq`.
Directory entries that are symlinks are ignored.
"""
function _process_dirs(filepred, dw::AbstractDirWalker, id, agentq, workq)
try
    start = time()
    ndirs = 0
    while true
        dir = take!(workq)
        if isempty(dir)
            return (; t=time()-start, n=ndirs)
        end

        ndirs += 1
        @debug "dagent $id processing dir $dir"
        try
            # TODO add check for readability (once a v1.10 way is known!)
            paths = readdir(dir; join=true, sort=false)

            # For each iten in dir
            for item in paths
                islink(item) && continue # skip symlinks
                if isdir(item)
                    # Add subdir item to dw.dirq
                    #put!(dw.dirq, (; id, item))
                    put!(dw.dirq, item)
                elseif isfile(item) && filepred(item)
                    # Add filepred-matching file path to dw.filerq
                    put!(dw.fileq, item)
                end
            end
        catch ex
            # TODO Make this @warn or @error?
            @debug "error processing directory $dir\n$ex"
        finally
            # TODO Does the ordering of these two put! statements matter?
            # Put id back into agentq
            put!(agentq, id)
            # Indicate "agent done"
            put!(dw.dirq, "")
        end
    end
catch ex
@show ex
    (; ex)
end
end

function _process_files(filefunc, dw::AbstractDirWalker, args...; kwargs...)
try
    start = time()
    nfiles = 0
    while true
        file = take!(dw.fileq)
        if isempty(file)
            # Recycle empty value for other tasks processing fileq (if any)
            put!(dw.fileq, file)
            return (; t=time()-start, n=nfiles)
        end

        try
            @debug "processing file $file"
            put!(dw.outq, filefunc(file, args...; kwargs...))
            nfiles += 1
        catch ex
            @warn "got exception processing $file" ex
        end
    end
catch ex
    (; ex)
end
end

function start_dagents(filepred, dw::DirWalker, agentspec)
    # Create queue for dagents
    agentq = Channel{Int}(agentspec)

    # Start dagent tasks and create agent_id=>(; workq, fetchable) map
    agentidmap = map(1:agentspec) do agent_id
        workq = Channel{String}(WORK_QUEUE_SIZE)
        fetchable = errormonitor(
            Threads.@spawn _process_dirs(filepred, dw, agent_id, agentq, workq)
        )
        # Add agent_id to agentq
        put!(agentq, agent_id)
        # Pair mapping agent_id to NamedTuple of workq and fetchable (i.e. Task)
        agent_id => (; workq, fetchable)
    end |> Dict

    # Reurn agentidmap and agentq
    agentidmap, agentq
end

function start_fagents(filefunc, dw::DirWalker, agentspec, args...; kwargs...)
    map(1:agentspec) do _
        errormonitor(
            Threads.@spawn _process_files(filefunc, dw, args...; kwargs...)
        )
    end
end

function start_dirwalker(filefunc, dw::AbstractDirWalker, topdirs, args...;
    filepred=_->true, dagentspec=1, fagentspec=1, kwargs...
)
    runtask = Threads.@spawn begin
        # Start dir agents.  start_dagents handles creation of agentq because
        # its sizing depends on how dagentspec in interpretted (i.e. as a
        # (local) dagent task count vs a list of (distributed) dagent workers).
        dagentmap, dagentq = start_dagents(filepred, dw, dagentspec)

        # Start file agents
        fagents = start_fagents(filefunc, dw, fagentspec, args...; kwargs...)

        # Populate dw.dirq.  It is important to do this after starting agents to
        # avoid blocking on a full channel before agents are started.
        for item in topdirs
            #isdir(item) && !isempty(item) && put!(dw, (; id=0, item))
            isdir(item) && !isempty(item) && put!(dw, item)
        end

        # Process dirq (TODO: make this a function)
        @info "processing dirq"
        npending = 0
        while true
            item = take!(dw.dirq)

            # If item is empty, work request complete
            if isempty(item)
                npending -= 1
                if npending <= 0
                    # No more pending work requests, so no more potential work
                    # In other words, we're done!
                    break
                else
                    # Keep processing dirq
                    continue
                end
            else
                # Got a work item, get an available dagent
                id = take!(dagentq)
                # Get dagent's workq from dagentmap
                workq = dagentmap[id].workq
                # put! work item into dagent's work queue
                put!(workq, item)
                npending += 1
            end
        end

        # Put empty string into workqs to signify end of input and then wait for
        # dagent to finish.
        @info "waiting for dir agents to complete"
        for (workq, fetchable) in values(dagentmap)
            put!(workq, "")
            wait(fetchable)
        end

        # Put empty string into dw.fileq
        put!(dw.fileq, "")

        # Wait for file agents to complete
        @info "waiting for file agents to complete"
        foreach(wait, fagents)

        # take! empty string out of dw.fileq
        take!(dw.fileq)

        # Put nothing into dw.outq
        put!(dw.outq, nothing)

        @info "done"

        # "Return" dagent fetchables and fagent fetchable
        last.(values(dagentmap)), fagents
    end

    runtask
end

end # module DirWalkers
