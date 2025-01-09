# DirWalkers - Parallelized directory walking and processing

DirWalkers allows parallelized walking and processing of directory trees.  It is
primarily intended for collecting file inventories.  The work is performed by
"agents" with intermediate and final results passed through "queues".  Agents
can be in-process `Task`s (which will run on multiple threads, if available) or
external worker processes setup via `Distributed.jl`.  Queues can be in-process
`Channel`s (for use with in-process `Task`-based agents) or `RemoteChannel`s
(for use with external worker process agents).

# Theory of operation

`DirWalkers` requires three user-supplied queues and two types of agents.  Taken
together, these queues and agents are known as a directory walker.

## Queues

1. A directory queue
2. A file queue
3. An output queue

The directory and file queues are `Channel`s or `RemoteChannel`s that contain
Strings.  The output queue can hold a user-supplied type or Nothing (to signal
the end of data).

## Agents

1. Directory agent
2. File agent

The agents can be in-process `Task`s or external worker processes.  These
agents run functions defined within `DirWalkers`, but the user can pass
functions to customize their actions.  The agents all run in parallel (i.e.
concurrently).

### Directory agents

Effectively, directory agents run a loop.  For each iteration, they take a
directory name from the directory queue.  The entries of the directory are read.
Each entry that is a directory is added to the directory queue.  Each entry that
is a file is passed to a user-supplied *predicate function* (i.e. a function
that returns `true` or `false`).  If the predicate returns `true`, the filename
is added to the file queue, otherwise it is ignored.  To avoid loops and other
potential problems, symbolic links are also ignored.  Directory agents run
until they take an empty String from the directory queue.

### File agents

File agents also run a loop.  For each iteration they take a filename from the
file queue.  The file name is passed to a user-supplied *file function* that is
expected to do something with the file and return some data.  One simple example
of a suitable file function is `Base.stat`.  This returned data is then put into
the output queue.  It is important that the output queue be created to hold the
type of data returned by the file function.  File agents run until they take an
empty String from the file queue.

# Running a directory walker

A directory walk is performed by calling the `run_dirwalker` function:

    run_dirwalker(filefunc, dirq, fileq, outq, topdirs, args...;
        filepred=_->true, dagentspec=1, fagentspec=1, extraspec=0, kwargs...)

## Arguments

- `filefunc` - The user-supplied function that will produce an output
  value for each file.  Its first argument must take the filename.  Any
  additional `args` and `kwargs` passed to `start_dirwalker` will be passed to
  `filefunc` as well.
- `dirq` - The user-supplied directory queue
- `fileq` - The user-supplied file queue
- `outq` - The user-supplied output queue
- `topdirs` - A Vector of directory names to be walked
- `filepred` - The file predicate function (defaults to `true`, i.e. match all
  files)
- `dagentspec` - This is the directory agent specification, see below
- `fagentspec` - This is the file agent specification, see below
- `extraspec` - This is an optional extra file specification, see below

### Agent specifications

The agent specifications can be given in two forms.  If given as a single
integer, the agent specification is treated as the number of (in-process)
`Task`s to run as agents.  If given as a Vector of integers, they are treated
as worker process IDs as returned by `Distributed.addprocs`.  It is important
that the agent specification is compatible with the corresponding queue.  If the
directory agents are to run as in-process Tasks, then `dagentspec` must be given
as a single integer and `dirq` must be a `DirQueue` (i.e. a `Channel{String}`).
If the directory agents are to run as external worker processes, then
`dagentspec` must be given as a Vector of the workers' process IDs (i.e.
integers) and `dirq` must be a `RemoteDirQueue`.  For `fagentspec`, the same
constraints apply for `fileq` and `outq`.

`extraspec` is an optional specification for additional file agents that will be
started after the directory agents start.  Ir must be in the same format as
`fagentspec`.  This is useful when one host will be running the directory agents
in-process and other hosts will be running the file agents remotely.  To utilize
the host resources that the directory agents had been using, additional
out-of-process-but-still-on-the-same-host file agents can be activated.

### Distributed considerations

When using remote workers, it is imperative that they all load `DirWalkers` and
all have the file predicate and file function defined.  Usually this can be
accomplished using `@everywhere` after the workers have been started.

When using extra out-of-process worker processes via `extraspec`, these worker
processes must be started alongside the other file agent worker processes before
calling `run_dirwalker` even though they will not be active until the directory
agents finish.

Often the directory and file agents have access to the same (possibly
distributed) filesystem(s).  In this case any file agent can process a file from
any directory agent.  In other cases, not all directory and file agents will
have equal access to the same filesystem (e.g. local filesystem on remote worker
hosts).  In these cases, `run_dirwalker` may be run in parallel on multiple
remote workers (or less likely in Tasks).  When operating in this "silo" mode,
be sure to use a separate `dirq`, `fileq`, and `topdirs` for each
`run_dirwalker` call and that the directory and file agents all have access to
the same filesystem(s).  When running in "silo" mode with a single common
`outq`, be sure to keep taking items out of `outq` until you get one `nothing`
value for each `run_diralker` call.

## Return value

The `run_dirwalker` function returns two Vectors of named tuples, one for the
directory agents and one for the file agents.  Each named tuple has fields
`host`, `id`, `n` and `t`, where `host` and `id` are the hostname and ID of the
agent, `n` is the number of directories/files processed by the corresponding
agent, and `t` is the elapsed time in seconds that the agent took to run.

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

# Example (non-parallel processing)

    using DirWalkers

    # Create queues
    dirq = DirQueue(Inf)
    fileq = FileQueue(Inf)
    outq = OutQueue{Base.Filesystem.StatStruct}(Inf)

    # Run the directory walker
    dstats, fstats = run_dirwalker(stat, dirq, fileq, outq, [@__DIR__])

    # Process outputs
    for ss in outq
        ss === nothing && break
        println(ss)
    end

# Example (parallel processing)

    using DirWalkers

    # Create queues
    dirq = DirQueue(Inf)
    fileq = FileQueue(Inf)
    outq = OutQueue{Base.Filesystem.StatStruct}(Inf)

    # Start the directory walker running in a separate Task
    runtask = Threads.@spawn run_dirwalker(stat, dirq, fileq, outq, [@__DIR__])

    # Process outputs
    for ss in outq
        ss === nothing && break
        println(ss)
    end

    # Get run_dirwalker return values by fetching from runtask
    dstats, fstats = fetch(runtask)
