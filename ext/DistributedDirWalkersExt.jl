module DistributedDirWalkersExt

using DirWalkers: AbstractDirWalker, _process_dirs, _process_files
import DirWalkers: DirWalker, start_dagents, start_fagents

using Distributed: RemoteChannel, @spawnat

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

function start_dagents(filepred, dw::RemoteDirWalker, agentspec)
    map(agentspec) do w
        @spawnat w _process_dirs(filepred, dw)
    end
end

function start_fagents(filefunc, dw::RemoteDirWalker, agentspec, args...; kwargs...)
    map(agentspec) do w
        @spawnat w _process_files(filefunc, dw, args...; kwargs...)
    end
end

end # module DistributedDirWalkersExt