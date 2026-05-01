# example_parallel.jl
using DirWalkers

# Create queues
topq = TopQueue(Inf)
dirq = DirQueue(Inf)
fileq = FileQueue(Inf)
outq = OutQueue{Base.Filesystem.StatStruct}(Inf)

# Start the directory walker running in a separate Task
@info "spawning directory walker task"
dwtask = Threads.@spawn run_dirwalker(tuple∘stat, topq, dirq, fileq, outq, [@__DIR__])

# Process output queue until we get `nothing`
for ss in Iterators.takewhile(!isnothing, outq)
    println(ss)
end

# Get run_dirwalker return values by fetching from runtask
ndirs, dstats, fstats = fetch(dwtask)

@show ndirs dstats fstats
