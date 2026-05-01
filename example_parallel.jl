# example_parallel.jl
using DirWalkers

# Create queues
dirq = DirQueue(Inf)
workq = WorkQueue(Inf)
fileq = FileQueue(Inf)
outq = OutQueue{Base.Filesystem.StatStruct}(Inf)

# Start the directory walker running in a separate Task
runtask = Threads.@spawn run_dirwalker(tuple∘stat, dirq, workq, fileq, outq, [@__DIR__])

# Process output queue until we get `nothing`
for ss in Iterators.takewhile(!isnothing, outq)
    println(ss)
end

# Get run_dirwalker return values by fetching from runtask
ndirs, dstats, fstats = fetch(runtask)

@show ndirs dstats fstats
