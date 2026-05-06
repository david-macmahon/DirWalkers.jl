module DirWalkers

using Base.Iterators: takewhile

export run_dirwalker
export TopQueue, DirQueue, FileQueue, OutQueue
export RemoteTopQueue, RemoteDirQueue, RemoteFileQueue, RemoteOutQueue

# One million should be enough, but OK to block if a queue gets full
DEFAULT_QUEUE_SIZE = 10^6

# Queue channel types
const TopQueue = Channel{String}
const DirQueue = Channel{String}
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
    _control_loop(topq, dirq)

Runs a loop that takes `String`s from `topq` and puts non-empty `String`s into
`dirq`.  For each `String` put into `dirq` a "posted" counter is incremented.
For each empty `String` taken from `topq` a "completed" counter is incremented.
The loop exits when the "completed" counter is no longer less than the "posted"
counter.  The number of items completed items is returned.
"""
function _control_loop(topq, dirq)
    @debug "control agent starting"
    nposted = 0
    ncompleted = 0
    keep_running = true
    while keep_running
        # Keep forwarding items from topq to dirq until we get an empty item
        for item in takewhile(!isempty, topq)
            @debug "got item" item
            # Got a work item, put it in dirq.  We can't do `isdir` check on
            # `item` here because the control agent may be running on a system
            # (e.g. a head node) that doesn't have access to the relevant
            # filesystem (e.g. `/datag`).
            @debug "putting item into dirq"
            put!(dirq, item)
            nposted += 1
            @debug "put item into dirq" nposted ncompleted
        end

        # An empty item means work request was completed, increment ncompleted
        ncompleted += 1
        @debug "got empty string from topq" nposted ncompleted

        # Keep running if ncompleted is less than nposted
        keep_running = (ncompleted < nposted)
    end

    ncompleted
end

"""
    _process_dirs(id, topq, dirq, fileq; dirpred=_->true, filepred=_->true)

Takes directory names from `dirq` until it gets an empty directory name, which
causes the function to return `(; host=hostname, id, t=elapsed_time, n=ndirs)`.
For each directory taken from `dirq` its subdirectory entries are `put!` into
`topq` if `dirpred` returns true and its files entries are `put!` into `fileq`
if `filepred` returns `true`.  `dirpred` and `filepred` are expected to be
functions that accept the directory or file name and return a boolean.  Symbolic
links are always ignored.
"""
function _process_dirs(id, topq, dirq, fileq;
    dirpred=_->true, filepred=_->true
)
try
    start = time()
    ndirs = 0
    @debug "dagent $id starting at $start"
    for dir in takewhile(!isempty, dirq)
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
                    # Add subdir item to topq if dirpred returns true
                    if dirpred(item)
                        @debug "dagent $id adding directory $item to topq"
                        put!(topq, item)
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
                    @debug "dagent $id ignoring unhandled item $item"
                end
            end
        catch ex
            # TODO Make this @warn or @error?
            @debug "dagent $id error processing directory $dir\n$ex"
        finally
            # Indicate "work completion" in topq
            @debug "dagent $id putting empty string (work completion) in topq"
            put!(topq, "")
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
    start_dagents(topq, dirq, fileq, agentspec::Integer;
        dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
    )

TBW
"""
function start_dagents(topq, dirq, fileq, agentspec::Integer;
    dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
)
    # Start dagent tasks
    map(1:agentspec) do id
        errormonitor(
            Threads.@spawn process_dirs(id, topq, dirq, fileq; dirpred, filepred)
        )
    end
end

function start_fagents(filefunc, fileq, outq, agentspec::Integer, args...;
    idoffset=0, process_files=_process_files, kwargs...
)
    map(1:agentspec) do id
        errormonitor(
            Threads.@spawn process_files(filefunc, id+idoffset, fileq, outq, args...; kwargs...)
        )
    end
end

function run_dirwalker(filefunc, topq, dirq, fileq, outq, topdirs, args...;
    dirpred=_->true, filepred=_->true, dagentspec=1, fagentspec=1, extraspec=nothing,
    process_dirs=_process_dirs, process_files=_process_files, kwargs...
)
    # topdirs cannot contain empty strings
    any(isempty, topdirs) && error("topdirs cannot contain empty names")

    # topq must be able to hold all of topdirs
    qsize(topq) < length(topdirs) && error("topq is not large enough for topdirs")

    # Start dir agents
    dagents = start_dagents(topq, dirq, fileq, dagentspec; dirpred, filepred, process_dirs)

    # Start file agents
    fagents = start_fagents(filefunc, fileq, outq, fagentspec, args...; process_files, kwargs...)

    # Populate topq.  This can lead to a deadlock if topq is not deep enough
    # to hold all topdirs so we have an explicit check for that above.
    for item in topdirs
        # We can't do `isdir` checks on `topdirs` entries here because the
        # current process may be running on a system (e.g. a head node) that
        # doesn't have access to the relevant filesystem (e.g. `/datag`).
        put!(topq, item)
    end

    # Start control agent
    cagent = errormonitor(Threads.@spawn _control_loop(topq, dirq))

    # Everything has been started!

    # Wait for control agent to finish
    @info "waiting for control agent to finish"
    ndirs = fetch(cagent)

    @debug "signaling completion to dir agents"
    for _ in dagents
        put!(dirq, "")
    end

    @info "waiting for dir agents to complete"
    dagent_results = fetch.(dagents)

    # Startup extra file agents, if any
    if extraspec !== nothing
        idoffset = length(fagents)
        append!(fagents, start_fagents(
            filefunc, fileq, outq, extraspec, args...;
            idoffset, process_files, kwargs...
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
