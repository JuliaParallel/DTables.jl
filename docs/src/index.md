```@meta
CurrentModule = DTables
```

# Distributed table

The `DTable`, or "distributed table", is an abstraction layer on top of Dagger
that allows loading table-like structures into a distributed environment.  The
main idea is that a Tables.jl-compatible source provided by the user gets
partitioned into several parts and stored as `Chunk`s.  These can then be
distributed across worker processes by the scheduler as operations are
performed on the containing `DTable`.

Operations performed on a `DTable` leverage the fact that the table is
partitioned, and will try to apply functions per-partition first, afterwards
merging the results if needed.

The distributed table is backed by Dagger's Eager API (`Dagger.@spawn` and
`Dagger.spawn`).  To provide a familiar usage pattern you can call `fetch` on a
`DTable` instance, which returns an in-memory instance of the underlying table
type (such as a `DataFrame`, `TypedTable`, etc).

