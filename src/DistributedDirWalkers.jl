# Concrete queue channel types
const RemoteTopQueue = RemoteChannel{Channel{String}}
const RemoteDirQueue = RemoteChannel{Channel{String}}
const RemoteFileQueue = RemoteChannel{Channel{String}}
const RemoteOutQueue{T} = RemoteChannel{Channel{Union{Nothing,T}}}


# Simplified constructors
RemoteDirQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
#RemoteFileQueue(pid=myid(); sz=0) = RemoteChannel(()->Channel{String}(sz), pid)
RemoteOutQueue{T}(pid=myid(); sz=0) where T = RemoteChannel(()->Channel{Union{Nothing,T}}(sz), pid)

# If dagents are remote workers then topq, dirq, and fileq must all be remote
# queues.
function start_dagents(topq::RemoteTopQueue, dirq::RemoteDirQueue,
    fileq::RemoteFileQueue, agentspec::AbstractVector;
    dirpred=_->true, filepred=_->true, process_dirs=_process_dirs
)
    # Spawn remote directory agents
    spawntasks = map(agentspec) do w
        Threads.@spawn @spawnat(w, process_dirs(w, topq, dirq, fileq; dirpred, filepred))
    end
    fetch.(spawntasks)
end

# If fagents are remote workers then fileq and outq must both be remote queues.
function start_fagents(filefunc, fileq::RemoteFileQueue, outq::RemoteOutQueue,
    agentspec::AbstractVector, args...;
    idoffset=0, process_files=_process_files, kwargs...
)
    # Use tasks to spawn remote agents in parallel
    spawntasks = map(agentspec) do w
        Threads.@spawn @spawnat(w, process_files(filefunc, w, fileq, outq, args...; kwargs...))
    end
    fetch.(spawntasks)
end
