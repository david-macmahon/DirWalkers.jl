module DirWalkers

using Base.Iterators: takewhile
using Distributed: RemoteChannel, @spawnat, call_on_owner, channel_from_id,
    myid, remotecall_fetch

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

include("utils.jl")

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

include("process_dirs.jl")
include("process_files.jl")

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
    # If we populate topq after starting the control and directory agents, then
    # there is a possibile race condition of the first topdir getting processed
    # and its "work completion" empty string getting "counted" before the second
    # topdir ever makes it into topq thereby terminating the whole walk early!
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
