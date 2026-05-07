"""
    nitems(q::Channel) -> number of items in `q`
    nitems(q::RemoteChannel) -> number of items in `q`
    nitems(q) -> `q`

Return the number of items available in `q` if it is a `Channel` or
`RemoteChannel`, otherwise just returns `q`.
"""
nitems(q) = q
nitems(q::Channel) = Base.n_avail(q)
nitems(q::RemoteChannel) = call_on_owner(channel_from_id, q) |> nitems


"""
    qsize(q::Channel) -> number of items that `q` can hold
    qsize(q::RemoteChannel) -> number of items that `q` can hold

Return the maximum number of items that `q` can hold.
"""
qsize(q::Channel) = q.sz_max
qsize(q::RemoteChannel) = call_on_owner(channel_from_id, q) |> qsize

"""
    unixms()

Define function to get timestamped number of items in all 4 queues
"""
unixms() = trunc(Int, 1e3*time())

"""
    qstatus(; kwargs...)

Return a NmaedTuple copy of `kwargs`, but with `Channel` or `RemoteChannel`
values replaced with the number of items they contain.

```jl
julia> qstatus(; time=1234, topq, dirq, fileq, outq)
(time=1234, topq=0, dirq=12, fileq=34, outq=56)
```
"""
function qstatus(; kwargs...)
    NamedTuple(
        (k => nitems(v) for (k,v) in kwargs)
    )
end

"""
    workerhostpid(w) -> "host HOSTNAME pid PID"

Returns a string showing the hostname and PID of a worker process.
"""
function workerhostpid(w)
    remotecall_fetch(()->"host $(gethostname()) pid $(getpid())", w)
end
