using Distributed: RemoteChannel, @spawnat, call_on_owner, channel_from_id, myid

# Concrete queue channel types
const RemoteDirQueue = RemoteChannel{Channel{String}}
const RemoteWorkQueue = RemoteChannel{Channel{String}}
const RemoteFileQueue = RemoteChannel{Channel{String}}
const RemoteOutQueue{T} = RemoteChannel{Channel{Union{Nothing,T}}}


# Simplified constructors
RemoteDirQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
#RemoteFileQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
RemoteOutQueue{T}(pid=myid(); sz=0) where T = RemoteChannel(()->Channel{Union{Nothing,T}}(sz), pid)

nitems(q::RemoteChannel) = call_on_owner(nitems∘channel_from_id, q)
qsize(q::RemoteChannel) = call_on_owner(qsize∘channel_from_id, q)

function start_dagents(dirq::RemoteDirQueue, workq, fileq, agentspec;
    dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
)
    # Spawn remote directory agents
    spawntasks = map(agentspec) do w
        Threads.@spawn @spawnat(w, process_dirs(w, dirq, workq, fileq; dirpred, filepred))
    end
    fetch.(spawntasks)
end

function start_fagents(filefunc, fileq::RemoteFileQueue, outq::RemoteOutQueue,
    agentspec, args...; process_files=_process_files, kwargs...
)
    # Use tasks to spawn remote agents in parallel
    spawntasks = map(agentspec) do w
        Threads.@spawn @spawnat(w, process_files(filefunc, w, fileq, outq, args...; kwargs...))
    end
    fetch.(spawntasks)
end
