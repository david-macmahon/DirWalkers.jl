module DirWalkers

export DirWalker, start_dirwalker

struct DirWalker{T}
    dirq::Channel{String}
    fileq::Channel{String}
    outq::Channel{Union{Nothing,T}}
end

function DirWalker{T}(; dqsize=10_000, fqsize=10_000, oqsize=10_000) where T
    dirq = Channel{String}(dqsize)
    fileq = Channel{String}(fqsize)
    outq = Channel{Union{Nothing,T}}(oqsize)
    DirWalker{T}(dirq, fileq, outq)
end

DirWalker(; dqsize=10_000, fqsize=10_000, oqsize=10_000) = DirWalker{Any}(; dqsize, fqsize, oqsize)

Base.put!(dw::DirWalker, dir) = put!(dw.dirq, dir)
Base.take!(dw::DirWalker) = take!(dw.outq)
Base.isready(dw::DirWalker) = isready(dw.outq)

# Iteration for dir walker `take`s from `outq` until getting `nothing`.

Base.IteratorSize(::Type{<:DirWalker}) = Base.SizeUnknown()
Base.eltype(_::DirWalker{T}) where T = T

function Base.iterate(dw::DirWalker, _=nothing)
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
function _process_dirs(filepred, dw::DirWalker)
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

function _process_files(filefunc, dw::DirWalker, args...; kwargs...)
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
        catch
            @warn "got exception processing $file"
        end
    end
catch ex
    (; ex)
end
end

function start_dirwalker(filefunc, dw::DirWalker, topdirs, args...;
    filepred=_->true, ndirtasks=1, nfiletasks=1, kwargs...
)
    dirtasks = Task[]
    filetasks = Task[]

    runner = Threads.@spawn begin
        # Start dir tasks

        dirtasks = [errormonitor(Threads.@spawn _process_dirs(filepred, dw)) for _=1:ndirtasks]

        # Start file tasks
        filetasks = [errormonitor(Threads.@spawn _process_files(filefunc, dw, args...; kwargs...)) for _=1:nfiletasks]

        # Populate dw.dirq.  It is important to do this after starting tasks to
        # avoid blocking before tasks are started.
        for d in topdirs
            isdir(d) && put!(dw, d)
        end
        # Put emoty string into dw.dirq to signify end of input to dirtasks
        put!(dw, "")

        # Wait for dir tasks to complete
        @info "waiting for dir tasks to complete"
        foreach(wait, dirtasks)

        # Put empty string into dw.fileq
        put!(dw.fileq, "")

        # Wait for file tasks to complete
        @info "waiting for file tasks to complete"
        foreach(wait, filetasks)

        # Put nothing into dw.outq
        put!(dw.outq, nothing)

        @info "done"

        dirtasks, filetasks
    end

    runner, dirtasks, filetasks
end

end # module DirWalkers
