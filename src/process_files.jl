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
