# example_serial.jl
using DirWalkers

# Create queues
topq = TopQueue(Inf)
dirq = DirQueue(Inf)
fileq = FileQueue(Inf)
outq = OutQueue{Base.Filesystem.StatStruct}(Inf)

# Run the directory walker
ndirs, dstats, fstats = run_dirwalker(tuple∘stat, topq, dirq, fileq, outq, [@__DIR__])

# Process output queue until we get `nothing`
for ss in Iterators.takewhile(!isnothing, outq)
    println(ss)
end

@show ndirs dstats fstats
