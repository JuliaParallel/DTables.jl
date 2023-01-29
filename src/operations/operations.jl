"""
    map(f, d::DTable) -> DTable

Applies `f` to each row of `d`.
The applied function needs to return a `Tables.Row` compatible object (e.g. `NamedTuple`).

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> m = map(x -> (r = x.a + x.b,), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r = [2, 3, 4],)

julia> m = map(x -> (r1 = x.a + x.b, r2 = x.a - x.b), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r1 = [2, 3, 4], r2 = [0, 1, 2])
```
"""
function map(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        return if isnonempty(_chunk)
            sink = materializer(_chunk)
            sink(TableOperations.map(_f, _chunk))
        else
            _chunk
        end
    end
    chunks = map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks)
    return DTable(chunks, d.tabletype)
end

"""
    map(f, gd::GDTable) -> GDTable

Applies `f` to each row of `gd`.
The applied function needs to return a `Tables.Row` compatible object (e.g. `NamedTuple`).

# Examples
```julia
julia> g = DTables.groupby(DTable((a=repeat('a':'c', inner=2),b=1:6), 2), :a)
GDTable with 3 partitions and 3 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> m = map(r -> (a = r.a, b = r.b, c = r.a + r.b), g)
GDTable with 3 partitions and 3 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(m)
(a = ['a', 'a', 'c', 'c', 'b', 'b'], b = [1, 2, 5, 6, 3, 4], c = ['b', 'c', 'h', 'i', 'e', 'f'])
```
"""
function map(f, gd::GDTable)
    d = map(f, gd.dtable)
    return GDTable(d, gd.cols, gd.index)
end

"""
    reduce(f, d::DTable; cols=nothing, [init]) -> NamedTuple

Reduces `d` using function `f` applied on all columns of the DTable.

By providing the kwarg `cols` as a `Vector{Symbol}` object it's possible
to restrict the reduction to the specified columns.
The reduced values are provided in a NamedTuple under names of reduced columns.

For the `init` kwarg please refer to `Base.reduce` documentation,
as it follows the same principles.

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> r1 = reduce(+, d)
Dagger.EagerThunk (running)

julia> fetch(r1)
(a = 6, b = 3)

julia> r2 = reduce(*, d, cols=[:a])
Dagger.EagerThunk (running)

julia> fetch(r2)
(a = 6,)
```
"""
function reduce(
    f, d::DTable; cols=nothing::Union{Nothing,Vector{Symbol}}, init=Base._InitialValue()
)
    # handle empty dtables
    nchunks(d) == 0 && return Dagger.@spawn NamedTuple()

    columns = cols === nothing ? columnnames_svector(d) : cols

    chunk_reduce_results = _reduce_chunks(f, d.chunks, columns; init=init)

    construct_single_column = (_col, _chunk_results) -> getindex.(fetch.(_chunk_results), _col)
    result_columns = [
        Dagger.@spawn construct_single_column(c, chunk_reduce_results) for c in columns
    ]

    reduce_result_column = (_f, _c, _init) -> reduce(_f, _c; init=_init)
    reduce_chunks = [
        Dagger.@spawn reduce_result_column(f, c, deepcopy(init)) for c in result_columns
    ]

    construct_result = (_cols, _vals) -> (; zip(_cols, fetch.(_vals))...)
    Dagger.@spawn construct_result(columns, reduce_chunks)
end

function _reduce_chunks(
    f,
    chunks::Vector{Union{Dagger.EagerThunk,Dagger.Chunk}},
    columns::Vector{Symbol};
    init=Base._InitialValue(),
)
    col_in_chunk_reduce = (_f, _c, _init, _chunk) -> reduce(_f, getcolumn(_chunk, _c); init=_init)

    chunk_reduce =
        (_f, _chunk, _cols, _init) -> begin
            # TODO: potential speedup enabled by commented code below by reducing the columns in parallel
            v = [col_in_chunk_reduce(_f, c, deepcopy(_init), _chunk) for c in _cols]
            (; zip(_cols, v)...)

            # TODO: uncomment and define a good threshold for parallelization when this get's resolved
            # https://github.com/JuliaParallel/Dagger.jl/issues/267
            # This piece of code (else option) below is causing the issue above
            # when reduce is repeatedly executed or @btime is used.
            # if length(_cols) <= 1
            #     v = [col_in_chunk_reduce(_f, c, _init, _chunk) for c in _cols]
            # else
            #     values = [Dagger.spawn(col_in_chunk_reduce, _f, c, _init, _chunk) for c in _cols]
            #     v = fetch.(values)
            # end
            # (; zip(_cols, v)...)
        end
    chunk_reduce_spawner =
        (_d, _f, _columns, _init) ->
            [Dagger.@spawn chunk_reduce(_f, c, _columns, _init) for c in _d]
    Dagger.@spawn chunk_reduce_spawner(chunks, f, columns, init)
end

"""
    reduce(f, gd::GDTable; cols=nothing, prefix="result_", [init]) -> Dagger.EagerThunk -> NamedTuple

Reduces `gd` using function `f` applied on all columns of the DTable.
Returns results per group in columns with names prefixed with the `prefix` kwarg.
For more information on kwargs see `reduce(f, d::DTable)`

# Examples
```julia
julia> g = DTables.groupby(DTable((a=repeat('a':'d', inner=2),b=1:8), 2), :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(reduce(*, g))
(a = ['a', 'c', 'd', 'b'], result_a = ["aa", "cc", "dd", "bb"], result_b = [2, 30, 56, 12])
```
"""
function reduce(
    f,
    gd::GDTable;
    cols=nothing::Union{Nothing,Vector{Symbol}},
    prefix::String="result_",
    init=Base._InitialValue(),
)

    # handle empty dtables
    nchunks(gd) == 0 && return Dagger.@spawn NamedTuple()

    columns = cols === nothing ? columnnames_svector(gd) : cols

    chunk_reduce_results = _reduce_chunks(f, gd.dtable.chunks, columns; init=init)

    reduce_result_column = (_f, _c, _init) -> reduce(_f, _c; init=deepcopy(_init))
    construct_single_column =
        (_col, _chunk_results, _index, _f, _init) -> begin
            r = getindex.(fetch.(_chunk_results), _col)
            [
                reduce_result_column(_f, getindex.(Ref(r), chunk_indices), _init) for
                (_, chunk_indices) in _index
            ]
        end
    result_columns = [
        Dagger.@spawn construct_single_column(c, chunk_reduce_results, gd.index, f, init) for
        c in columns
    ]

    construct_result =
        (_keys::Base.KeySet, _gcols, _columns, _results, _prefix) -> begin
            ks = if eltype(_keys) <: NamedTuple # many keys in groupby
                [col => getindex.(_keys, col) for col in _gcols]
            else # single key in groupby
                [col => collect(_keys) for col in _gcols]
            end
            rs = [
                Symbol(_prefix * string(r)) => fetch(_results[i]) for (i, r) in enumerate(_columns)
            ]
            (; ks..., rs...)
        end

    Dagger.@spawn construct_result(keys(gd), grouped_cols(gd), columns, result_columns, prefix)
end

"""
    filter(f, d::DTable) -> DTable

Filter `d` using `f`.
Returns a filtered `DTable` that can be processed further.

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> f = filter(x -> x.a < 3, d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])

julia> f = filter(x -> (x.a < 3) .& (x.b > 0), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])
```
"""
function filter(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        materializer(_chunk)(m)
    end
    return DTable(map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks), d.tabletype, d.schema)
end

"""
    filter(f, gd::GDTable) -> GDTable

Filter 'gd' using 'f', returning a filtered `GDTable`.
Calling `trim!` on a filtered `GDTable` will clean up the empty keys and partitions.

# Examples
```julia
julia> g = DTables.groupby(DTable((a=repeat('a':'d', inner=2),b=1:8), 2), :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> f = filter(x -> x.a ∈ ['a', 'b'], g)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(f)
(a = ['a', 'a', 'b', 'b'], b = [1, 2, 3, 4])

julia> trim!(f)
GDTable with 2 partitions and 2 keys
Tabletype: NamedTuple
Grouped by: [:a]
```
"""
function filter(f, gd::GDTable)
    d = filter(f, gd.dtable)
    return GDTable(d, gd.cols, gd.index)
end

"""
    mapreduce(f, op, d::DTable; init=Base._InitialValue())

Perform a `mapreduce` operation where `f` is the mapping function applied to the table row
and `op` is the reduce function applied to the results of the mapping operation.

# Examples

julia> using DTables, OnlineStats

julia> fetch(DTables.mapreduce(sum, fit!, d1, init = Mean()))
Mean: n=100 | value=1.50573
"""

function mapreduce(f, op, d::DTable; init=Base._InitialValue())
    nchunks(d) == 0 && return Dagger.@spawn NamedTuple()
    chunk_reduce_results = _mapreduce_rows_in_chunks(f, op, d.chunks; init=init)

    reduce_result_column = (_f, _c, _init) -> reduce(_f, fetch.(_c); init=deepcopy(_init))
    Dagger.@spawn reduce_result_column(op, chunk_reduce_results, deepcopy(init))
end

function _mapreduce_rows_in_chunks(
    fmap, f, chunks::Vector{Union{Dagger.EagerThunk,Dagger.Chunk}}; init=Base._InitialValue()
)
    col_in_chunk_reduce =
        (_f, _init, _chunk) -> reduce(_f, TableOperations.map(fmap, _chunk); init=deepcopy(_init))
    chunk_reduce_spawner =
        (_d, _f, _init) -> [Dagger.@spawn col_in_chunk_reduce(_f, _init, c) for c in _d]
    Dagger.@spawn chunk_reduce_spawner(chunks, f, init)
end
