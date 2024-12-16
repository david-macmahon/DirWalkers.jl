module DistributedDirWalkersExt

using DirWalkers: AbstractDirWalker, _process_dirs, _process_files, WORK_QUEUE_SIZE
import DirWalkers: DirWalker, start_dagents, start_fagents
import Base.n_avail

using Distributed: RemoteChannel, @spawnat, call_on_owner, channel_from_id

struct RemoteDirWalker{T} <: AbstractDirWalker{T}
    dirq::RemoteChannel{Channel{String}}
    fileq::RemoteChannel{Channel{String}}
    outq::RemoteChannel{Channel{Union{Nothing,T}}}
end

function DirWalker{T}(::Type{RemoteChannel}; dqsize=10_000, fqsize=10_000, oqsize=10_000)::AbstractDirWalker where {T}
    dirq = RemoteChannel(()->Channel{String}(dqsize))
    fileq = RemoteChannel(()->Channel{String}(fqsize))
    outq = RemoteChannel(()->Channel{Union{Nothing,T}}(oqsize))
    RemoteDirWalker(dirq, fileq, outq)
end

"""
Return the number of items available in `remotechannel`.
"""
function Base.n_avail(remotechannel::RemoteChannel)
    call_on_owner(Base.n_avail∘channel_from_id, remotechannel)
end

function start_dagents(filepred, dw::RemoteDirWalker, agentspec)
    # Create queue for dagents
    agentq = RemoteChannel(()->Channel{Int}(length(agentspec)))

    # Start dagent tasks and create agent_id=>(; workq, fetchable) map
    agentidmap = map(agentspec) do agent_id
        # Create workq on/in agent rather than "current" proc (does it matter?)
        workq = RemoteChannel(()->Channel{String}(WORK_QUEUE_SIZE), agent_id)
        fetchable = @spawnat agent_id _process_dirs(filepred, dw, agent_id, agentq, workq)
        # Add agent_id to agentq
        put!(agentq, agent_id)
        # Pair mapping agent_id to NamedTuple of workq and fetchable (i.e. Future)
        agent_id => (; workq, fetchable)
    end |> Dict

    # Reurn agentidmap and agentq
    agentidmap, agentq
end

function start_fagents(filefunc, dw::RemoteDirWalker, agentspec, args...; kwargs...)
    map(agentspec) do w
        @spawnat w _process_files(filefunc, dw, args...; kwargs...)
    end
end

end # module DistributedDirWalkersExt