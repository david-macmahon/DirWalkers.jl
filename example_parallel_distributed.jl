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
dirq = RemoteDirQueue(sz=Inf)
workq = RemoteWorkQueue(sz=Inf)
fileq = RemoteFileQueue(sz=Inf)
outq = RemoteOutQueue{Base.Filesystem.StatStruct}(sz=Inf)

# Start the directory walker running in a separate Task
@info "spawning run_dirwalker task"
runtask = Threads.@spawn run_dirwalker(tuple∘stat, dirq, workq, fileq, outq, [@__DIR__];
    dagentspec, fagentspec
)

# Process output queue until we get `nothing`
for ss in Iterators.takewhile(!isnothing, outq)
    println(ss)
end

# Get run_dirwalker return values by fetching from runtask
ndirs, dstats, fstats = fetch(runtask)

@show ndirs dstats fstats
