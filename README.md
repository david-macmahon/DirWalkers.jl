# DirWalkers.jl - Parallelized directory walking and processing

`DirWalkers.jl` provides parallelized recursive walking of directory trees and
processing of files found along the way.  One of its primarily use cases is
collecting file inventories.  Most of the plumbing is handled automatically by
this package.  As a minimum, the user only needs to supply four easily created
*queues*, the top level directories to search, a *file function* that will
produce one or more output objects for each file processed, and code to take
output objects from the output queue.  Additional details may be provided by the
user to control directory and file selection and the number and nature of the
directory and file agents used internally.

# DirWalker architecture

`DirWalker` makes use of *agents* that pass intermediate and final results
through *queues*.  Agents can be in-process `Task`s (which will run on multiple
threads, if available) or external worker processes setup by the user via
`Distributed.jl`.  Queues can be in-process `Channel`s (for use with in-process
`Task`-based agents) or `RemoteChannel`s (for use with agents running in
external worker processes).  A `DirWalkers` uses three types of agents and four
queues.  The user can take the output from the output queue.  A block diagram of
the `DirWalkers` architecture is shown here and described below.

![DirWalker block diagram](docs/src/images/dirwalker.drawio.svg)

## Agents

The three types of agents are:

1. Control agent
2. Directory agent
3. File agent

Agents perform actions.  The control agent is a single internal `Task` that runs
in the main process.  Directory and file agents can be in-process `Task`s or
external worker processes.  All agents run concurrently.  Usually multiple
directory and file agents are used to process the directory walk in parallel.
Agents run a loop until the agent has no more work to perform at which point the
directory walk is done.

## Queues

The four queues are:

1. Top queue (TQ)
2. Directory queue (DQ)
2. File queue (FQ)
3. Output queue (OQ)

The queues can be `Channel`s or `RemoteChannel`s depending on whether the agents
are running as local tasks or on remote workers.  The top, directory, and file
queues contain `String`s.  The output queue holds a user-supplied type or
`Nothing`, which is used to signal the end of the data.

## Theory of operation

The `run_dirwalker` function orchestrates the life cycle of the directory
walker.  It is responsible for starting the agents, detecting completion of the
directory walk, stopping the agents, and fetching runtime statistics from the
agents.  The user must pass one or more names of top level directories to be
walked.

### Control agent

The control agent is managed internally by `run_dirwalker`.  It is the primary
driver of the directory walking process.  The control agent takes directory
names from the top queue and puts them into the directory queue.  A "posted"
counter is incremented for each directory name put into the directory queue.
Getting an empty string from the top queue signifies the completion of an
earlier posted directory.  For each empty string taken from the top queue a
"completed" counter is incremented.  The control agent ends when the "completed"
counter is no longer less than the "posted" counter.  The top queue is initially
populated with the top level directory names, but additional directories are
added to the top queue by the directory agents as they process the directories
taken from the directory queue.

### Directory agents

Directory agents run a loop.  For each iteration, they take a directory name
from the directory queue.  The filesystem entries of the directory are read.
Each entry that is a directory is added to the directory queue.  Each entry that
is a file is added to the file queue.  To avoid loops and other potential
problems, symbolic links are also ignored.  You can provide directory and file
*predicate functions* (i.e. functions that returns `true` or `false`) to process
only directories and files for which the corresponding predicate function
returns `true`.  By default, all directories and files are processed.  Directory
agents run until they take an empty String from the directory queue, which is
how `run_dirwalker` signals the overall completion of the directory walk to the
directory agents.

### File agents

File agents run a loop.  For each iteration they take a filename from the file
queue.  The file name is passed to a user-supplied *file function* that is
expected to do something with the file and return an iterator that yields data
from or about the file.  Even if a single object is derived from the file, it
must be returned as an iterator (e.g. wrapped in a `tuple`).  For example,
`stat`, which returns a `StatStruct` object, is not directly suitable as a
DirWalker file function, but the anonymous function `f->tuple(stat(f))` or the
composed function `tuple∘stat` would be.  The values yielded by the returned
iterator are put into the output queue.

The output queue must be created to hold a type that is compatible with the type
of data yielded by the iterator returned by the file function.  The motivation
for returning an iterator from the file function is to allow file functions the
option of partitioning large per-file data objects (e.g. large Vectors) into
smaller pieces to limit memory consumption in the output queue.  File agents run
until they take an empty String from the file queue, which is how
`run_dirwalker` signals the overall completion of the directory walk to the file
agents.

### Output handling

You are responsible for taking items out of the output queue.  What you do with
them is up to you, but presumably you will save the output to a file or
database.  The overall completion of the directory walk can be detected by
receiving `nothing` from the output queue.  Using `Iterators.takewhile` is
recommended way to loop through all the output from the output queue.  See the
examples below for more details.  The output handler can run in the main task or
in a separate task or even on a separate host if the output queue is a
`RemoteChannel`.  You are free to handle the output how ever you want to.

Be aware that `run_dirwalker` only puts a single `nothing` into the output
queue, so if you use multiple output handlers each one should recycle the
`nothing` back into the output queue before ending so that the other handlers
will see it.  This will leave one `nothing` in the output queue after the last
handler has ended so be sure to take it out after all the handlers have stopped
if you will be reusing the output queue.

# Running a directory walker

A directory walk is performed by calling the `run_dirwalker` function:

    run_dirwalker(filefunc, topq, dirq, fileq, outq, topdirs, args...;
        dirpred=_->true, filepred=_->true, dagentspec=1, fagentspec=1,
        extraspec=nothing, kwargs...)

## Arguments

- `filefunc` - The function that will produce an output value for each file.
  Its first argument must take the filename.  Any additional `args` and `kwargs`
  passed to `run_dirwalker` will be passed to `filefunc` as well.
- `topq` - The top queue
- `dirq` - The directory queue
- `fileq` - The file queue
- `outq` - The output queue
- `topdirs` - A Vector of directory names to be walked
- `dirpred` - The directory predicate function (default matches all directories)
- `filepred` - The file predicate function (default matches all files)
- `dagentspec` - This is the directory agent specification, see below
- `fagentspec` - This is the file agent specification, see below
- `extraspec` - This is an optional extra file specification, see below

### Agent specifications

The agent specifications can be given in two forms.  If given as a single
integer, the agent specification is treated as the number of (in-process)
`Task`s to run as agents.  If given as a Vector of integers, they are treated as
worker process IDs as returned by `Distributed.addprocs`.  It is important that
the agent specification is compatible with the corresponding queues.  If the
directory agents are to run on external worker processes, then `dagentspec` must
be given as a Vector of worker IDs and `topq`, `dirq`, and `fileq` must be
`RemoteChannel`s.

A similar constraint applies For `fagentspec` and the file agent queues `fileq`
and `outq`.

`extraspec` is an optional specification for additional file agents that will be
started after the directory agents finish.  It must be in the same format as
`fagentspec`.  This can be useful to recycle the directory agent resources for
file agents after the directory agents have completed.

### Distributed considerations

When using remote workers, it is imperative that they all load `DirWalkers` and
all have the directory and file predicates and file function defined.  Usually
this is accomplished using `@everywhere` after the workers have been started.

When using extra out-of-process worker processes via `extraspec`, these worker
processes must be started alongside the other file agent worker processes before
calling `run_dirwalker` even though they will not be active until the directory
agents finish.  One strategy is to pass the same workers for both `dagentspec`
and `extraspec`.

Often the directory and file agents have access to the same (possibly
distributed) filesystem(s).  In this case any file agent can process a file from
any directory agent.  In other cases, not all directory and file agents will
have equal access to the same filesystem (e.g. local filesystem on remote worker
hosts).  In these cases, `run_dirwalker` may be run in parallel on multiple
remote workers (or less likely in Tasks).  When operating in this "silo" mode,
be sure to use a separate `topq`, `dirq`, `fileq`, and `topdirs` for each
`run_dirwalker` call and that the directory and file agents all have access to
the same filesystem(s).  When running in "silo" mode with a single common `outq`
shared across silos, be sure to keep taking items out of `outq` until you get
one `nothing` value for each `run_diralker` call.  This is not compatible with
multiple output handlers because recycling the `nothing` will not do the right
thing.

## Return value

The `run_dirwalker` function returns the total number of directories walked
(i.e. the completed counter of the control agent) and two Vectors of named
tuples, one for the directory agents and one for the file agents.  Each named
tuple has fields `host`, `id`, `n` and `t`, where `host` and `id` are the
host name and ID of the agent, `n` is the number of directories/files processed
by the corresponding agent, and `t` is the elapsed time in seconds that the
agent took to run.

## Processing output

If `outq` can store all the results internally, the processing of the output
can be performed after `run_dirwalker` returns, but generally it is desirable
to process the output from `outq` in parallel with the running `run_dirwalker`
call.  In either case, you can get the output values by repeatedly calling
`take!(outq)` until it returns `nothing`, which indicates the end of data (and
further `take!` calls on `outq` will block).

To process the output in parallel with `run_dirwalker`, either `run_dirwalker`
or the output processing code (or both) must be run in a separate `Task` or
remote worker.  One common approach is to run `run_dirwalker` in a separate
`Task` with `Threads.@spawn`.  See the "parallel processing" example below.

# Examples

## Serial processing

```jl
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
```

## Parallel processing

```jl
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
```

## Parallel distributed processing

```jl
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
```
