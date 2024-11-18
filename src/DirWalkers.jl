module DirWalkers

export DirWalker, start_dirwalker

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
Takes directory names from `dirq` until it gets an empty directory name, which
causes the function re-`put!` the empty string into `dirq` and then return
(return value TBD).  Each non-empty directory name is iterated via `walkdir` to
completion.  Directory names with symlink components (i.e. if `readlink(d)!=d`)
are ignored.  At each `walkdir` iteration the files that are not symlinks are
`put!` into `fileq` if `filepred(filepath)` returns `true`.
"""
function _process_dirs(filepred, dw::AbstractDirWalker)
try
    start = time()
    ndirs = 0
    while true
        dir = take!(dw.dirq)
        if isempty(dir)
            # Recycle empty value for other tasks processing dirq (if any)
            put!(dw.dirq, dir)
            return (; t=time()-start, n=ndirs)
        end

        @debug "processing dir $dir"
        for (filedir, _, files) in walkdir(dir; onerror=_->nothing)
            ndirs += 1
            for file in files
                path = joinpath(filedir, file)
                if !islink(path) && filepred(path)
                    put!(dw.fileq, path)
                end
            end
        end
    end
catch ex
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
    map(1:agentspec) do _
        errormonitor(
            Threads.@spawn _process_dirs(filepred, dw)
        )
    end
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
        # Start dir agents
        dagents = start_dagents(filepred, dw, dagentspec)

        # Start file agents
        fagents = start_fagents(filefunc, dw, fagentspec, args...; kwargs...)

        # Populate dw.dirq.  It is important to do this after starting agents to
        # avoid blocking on a full channel before agents are started.
        for d in topdirs
            isdir(d) && put!(dw, d)
        end
        # Put emoty string into dw.dirq to signify end of input to dirtasks
        put!(dw, "")

        # Wait for dir tasks to complete
        @info "waiting for dir tasks to complete"
        foreach(wait, dagents)

        # Put empty string into dw.fileq
        put!(dw.fileq, "")

        # Wait for file tasks to complete
        @info "waiting for file tasks to complete"
        foreach(wait, fagents)

        # Put nothing into dw.outq
        put!(dw.outq, nothing)

        @info "done"

        dagents, fagents
    end

    runtask
end

end # module DirWalkers
