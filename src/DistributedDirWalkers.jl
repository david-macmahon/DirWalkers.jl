using Distributed: RemoteChannel, @spawnat, call_on_owner, channel_from_id, myid

# Concrete queue channel types
const RemoteDirQueue = RemoteChannel{Channel{String}}
const RemoteFileQueue = RemoteChannel{Channel{String}}
const RemoteOutQueue{T} = RemoteChannel{Channel{Union{Nothing,T}}}

# Simplified constructors
RemoteDirQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
#RemoteFileQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
RemoteOutQueue{T}(pid=myid(); sz=0) where T = RemoteChannel(()->Channel{Union{Nothing,T}}(sz), pid)

"""
Return the number of items available in `remotechannel`.
"""
function Base.n_avail(remotechannel::RemoteChannel)
    call_on_owner(Base.n_avail∘channel_from_id, remotechannel)
end

function start_dagents(filepred, dirq::RemoteDirQueue, fileq, agentspec)
    # Create queue for dagents
    agentq = RemoteChannel(()->Channel{Int}(length(agentspec)))

    # Use tasks to spawn remote agents in parallel
    agentid_workq_spawntasks = map(agentspec) do agent_id
        # OLD Create workq on/in agent rather than "current" proc (does it matter?)
        # NEW Create workq on/in "current" proc rather than agent (does it matter?)
        workq = RemoteChannel(()->Channel{String}(WORK_QUEUE_SIZE))
        spawntask = Threads.@spawn @spawnat(agent_id, _process_dirs(filepred, dirq, fileq, agentq, workq, agent_id))
        (agent_id, workq, spawntask)
    end

    # Map agentid_workq_spawntasks to Dict(agent_id=>(; workq, fetchable))
    agentidmap = map(agentid_workq_spawntasks) do (agent_id, workq, spawntask)
        agent_id => (; workq, fetchable=fetch(spawntask))
    end|>Dict

    # Put agent_ids from agentspec into agentq
    for agent_id in agentspec
        put!(agentq, agent_id)
    end

    # Reurn agentidmap and agentq
    agentidmap, agentq
end

function start_fagents(filefunc, fileq::RemoteFileQueue, outq::RemoteOutQueue, agentspec, args...; kwargs...)
    # Use tasks to spawn remote agents in parallel
    spawntasks = map(agentspec) do w
        Threads.@spawn @spawnat(w, _process_files(filefunc, fileq, outq, w, args...; kwargs...))
    end
    fetch.(spawntasks)
end
