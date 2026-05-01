# example_parallel_distributed.jl
using Distributed
using DirWalkers

# Start workers
@info "starting workers"
ws = addprocs(4)
dagentspec = ws[1:2]
fagentspec = ws[3:4]

@info "@everywhere using DirWalkers"
@everywhere using DirWalkers

# Create queues
@info "creating queues"
topq = RemoteTopQueue(sz=Inf)
dirq = RemoteDirQueue(sz=Inf)
fileq = RemoteFileQueue(sz=Inf)
outq = RemoteOutQueue{Base.Filesystem.StatStruct}(sz=Inf)

# Start the directory walker running in a separate Task
@info "spawning directory walker task"
dwtask = Threads.@spawn run_dirwalker(tuple∘stat, topq, dirq, fileq, outq, [@__DIR__];
    dagentspec, fagentspec
)

# Process output queue until we get `nothing`
for ss in Iterators.takewhile(!isnothing, outq)
    println(ss)
end

# Get run_dirwalker return values by fetching from runtask
ndirs, dstats, fstats = fetch(dwtask)

@show ndirs dstats fstats
