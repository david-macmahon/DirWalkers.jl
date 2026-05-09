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
            @warn "dagent $id error processing directory $dir\n$ex"
        finally
            # Indicate "work completion" in topq
            @debug "dagent $id putting empty string (work completion) in topq"
            put!(topq, "")
        end
        @debug "dagent $id end of dagent iteration"
    end

    return (; host=gethostname(), id, t=time()-start, n=ndirs)
catch ex
    showerror(stderr, ex, catch_backtrace())
    rethrow()
end
end
