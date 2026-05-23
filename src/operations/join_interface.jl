# A set of kwargs that can be provided by the user.
# Used for deciding whether to use the `DTables` join implementation directly
# or to attempt using an external join function.
const JOINKWARGS = Set([:l_sorted, :r_sorted, :r_unique, :lookup])

"""
    leftjoin(d1::DTable, d2; on=nothing, l_sorted=false, r_sorted=false, r_unique=false, lookup=nothing)

Perform a left join of `d1` with any `Tables.jl` compatible table type.
Returns a `DTable` with the result.

If the underlying table type happens to have a `leftjoin` implementation
and none of the below `DTable` related kwargs will be provided the specialized function will be used.
A good example of that is calling `leftjoin` on a `DTable` with a `DataFrame` underlying type
and a `d2` of `DataFrame` type.

# Keyword arguments

- `on`: Column symbols to join on. Can be provided as a symbol or a pair of symbols in case the column names differ. For joins on multiple columns a vector of the previously mentioned can be provided.
- `l_sorted`: To indicate the left table is sorted - only useful if the `r_sorted` is set to `true` as well.
- `r_sorted`: To indicate the right table is sorted.
- `r_unique`: To indicate the right table only contains unique keys.
- `lookup`: To provide a dict-like structure that will allow for direct matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function leftjoin(d1::DTable, d2; kwargs...)
    f = (l, r, ks) -> _leftjoinwrapper(l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    return DTable(v, d1.tabletype)
end

function leftjoin(d1::DTable, d2::DTable; kwargs...)
    l_chunks = d1.chunks
    r_chunks = d2.chunks
    n_l = length(l_chunks)
    n_r = length(r_chunks)
    n_r == 0 && return DTable([], d1.tabletype)

    r0 = first(r_chunks)

    # Phase 1: n_l × n_r pair tasks, all spawned at the top level (no nesting)
    pair_tasks = [
        Dagger.@spawn _leftjoin_pair(lc, rc, kwargs)
        for lc in l_chunks, rc in r_chunks
    ]

    # Phase 2: one merge task per l-chunk, each takes n_r Phase 1 results as Dagger inputs
    result_tasks = [
        Dagger.spawn(_merge_leftjoin_pairs, l_chunks[i], r0, kwargs, pair_tasks[i, :]...)
        for i in 1:n_l
    ]

    return DTable(result_tasks, d1.tabletype)
end

function leftjoin(d1::GDTable, d2; kwargs...)
    d = leftjoin(d1.dtable, d2; kwargs...)
    return GDTable(d, d1.cols, d1.index)
end

function _leftjoinwrapper(l, r; kwargs...)
    if !any(k in JOINKWARGS for k in keys(kwargs)) && use_dataframe_join(typeof(l), typeof(r))
        leftjoin(l, r; kwargs...)
    else
        _join(:leftjoin, l, r; kwargs...)
    end
end

"""
    innerjoin(d1::DTable, d2; on=nothing, l_sorted=false, r_sorted=false, r_unique=false, lookup=nothing)

Perform an inner join of `d1` with any `Tables.jl` compatible table type.
Returns a `DTable` with the result.

If the underlying table type happens to have a `innerjoin` implementation
and none of the below `DTable` related kwargs will be provided the specialized function will be used.
A good example of that is calling `innerjoin` on a `DTable` with a `DataFrame` underlying type
and a `d2` of `DataFrame` type.

# Keyword arguments

- `on`: Column symbols to join on. Can be provided as a symbol or a pair of symbols in case the column names differ. For joins on multiple columns a vector of the previously mentioned can be provided.
- `l_sorted`: To indicate the left table is sorted - only useful if the `r_sorted` is set to `true` as well.
- `r_sorted`: To indicate the right table is sorted.
- `r_unique`: To indicate the right table only contains unique keys.
- `lookup`: To provide a dict-like structure that will allow for direct matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function innerjoin(d1::DTable, d2; kwargs...)
    f = (l, r, ks) -> _innerjoinwrapper(l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    return DTable(v, d1.tabletype)
end

function innerjoin(d1::DTable, d2::DTable; kwargs...)
    l_chunks = d1.chunks
    r_chunks = d2.chunks
    n_l = length(l_chunks)
    n_r = length(r_chunks)
    n_r == 0 && return DTable([], d1.tabletype)

    f_pair = (l, r, ks) -> _join(:innerjoin, l, r; ks...)

    # Phase 1: n_l × n_r pair tasks, all spawned at the top level (no nesting)
    pair_tasks = [
        Dagger.@spawn f_pair(lc, rc, kwargs)
        for lc in l_chunks, rc in r_chunks
    ]

    # Phase 2: one merge task per l-chunk, each takes n_r Phase 1 results as Dagger inputs
    result_tasks = [
        Dagger.spawn(_merge_inner_pairs, pair_tasks[i, :]...)
        for i in 1:n_l
    ]

    return DTable(result_tasks, d1.tabletype)
end

function innerjoin(d1::GDTable, d2; kwargs...)
    d = innerjoin(d1.dtable, d2; kwargs...)
    return GDTable(d, d1.cols, d1.index)
end

function _innerjoinwrapper(l, r; kwargs...)
    if !any(k in JOINKWARGS for k in keys(kwargs)) && use_dataframe_join(typeof(l), typeof(r))
        innerjoin(l, r; kwargs...)
    else
        _join(:innerjoin, l, r; kwargs...)
    end
end

"""
    match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)

Function responsible for picking the optimal method of joining inner indices depending on the
additional information about the tables provided by the user.
"""
function match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
    if lookup !== nothing
        match_inner_indices_lookup(l, lookup, cmp_l) # uses the `lookup` to find indices
    elseif r_sorted && l_sorted
        match_inner_indices_lsorted_rsorted(l, r, cmp_l, cmp_r, r_unique) # loop through r once
    elseif r_unique
        match_inner_indices_runique(l, r, cmp_l, cmp_r) # break on first match
    elseif r_sorted
        match_inner_indices_rsorted(l, r, cmp_l, cmp_r) # break on last match
    else # generic fallback, no optimization
        match_inner_indices(l, r, cmp_l, cmp_r)
    end
end

"""
    _join(type::Symbol, l_chunk, r; kwargs...)

Low level join method for `DTable` joins using the generic implementation.
It joins an `l_chunk` with `r` assuming `r` is a continuous table.
"""
function _join(
    type::Symbol,
    l_chunk,
    r;
    on=nothing,
    l_sorted=false,
    r_sorted=false,
    r_unique=false,
    lookup=nothing,
)
    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l_chunk, r, on)

    inner_l, inner_r = match_inner_indices(
        l_chunk, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique
    )

    outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l_chunk, inner_l)
    return build_joined_table(type, names, l_chunk, r, inner_l, inner_r, outer_l, other_r)
end

"""
    _leftjoin_pair(l_chunk, r_chunk, kwargs)

Joins a single `l_chunk` against a single `r_chunk` for a left join.
Returns `(inner_df, outer_l)` where `outer_l` is the set of l-row indices
not matched by this particular r-chunk.
"""
function _leftjoin_pair(l_chunk, r_chunk, kwargs)
    on = get(kwargs, :on, nothing)
    l_sorted = get(kwargs, :l_sorted, false)
    r_sorted = get(kwargs, :r_sorted, false)
    r_unique = get(kwargs, :r_unique, false)
    lookup = get(kwargs, :lookup, nothing)
    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l_chunk, r_chunk, on)
    inner_l, inner_r = match_inner_indices(l_chunk, r_chunk, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
    inner_df = build_joined_table(:leftjoin, names, l_chunk, r_chunk, inner_l, inner_r, Set{UInt}(), other_r)
    outer_l = find_outer_indices(l_chunk, inner_l)
    return (inner_df, outer_l)
end

"""
    _merge_inner_pairs(dfs...)

Merges multiple inner-join DataFrames (one per r-chunk pair) into a single table.
"""
function _merge_inner_pairs(dfs...)
    chunks = [Dagger.tochunk(df) for df in dfs]
    return merge_chunks(materializer(first(dfs)), chunks)
end

"""
    _merge_leftjoin_pairs(l_chunk, r_chunk, kwargs, pairs...)

Merges multiple `(inner_df, outer_l)` pairs from Phase 1 into the final left-join result
for one l-chunk. Computes truly-unmatched l-rows as the intersection of all outer_l sets.
"""
function _merge_leftjoin_pairs(l_chunk, r_chunk, kwargs, pairs...)
    on = get(kwargs, :on, nothing)
    names, _, other_r, _, _ = resolve_colnames(l_chunk, r_chunk, on)
    inner_dfs = [p[1] for p in pairs]
    outer_l_sets = [p[2] for p in pairs]
    truly_outer_l = intersect(outer_l_sets...)
    empty_inds = Vector{UInt}()
    outer_df = build_joined_table(:leftjoin, names, l_chunk, r_chunk, empty_inds, empty_inds, truly_outer_l, other_r)
    all_chunks = [Dagger.tochunk(df) for df in inner_dfs]
    push!(all_chunks, Dagger.tochunk(outer_df))
    return merge_chunks(materializer(l_chunk), all_chunks)
end

"""
    use_dataframe_join(d1type, d2type)

Determines whether to use the DataAPI join function, which leads to usage of DataFrames join function if both types are `DataFrame`.
Remove this function and it's usage once a generic Tables.jl compatible join function becomes available.
Porting the Dagger join functions to TableOperations is an option to achieve that.
"""
function use_dataframe_join(d1type, d2type)
    return :DataFrame == d1type.name.name == d2type.name.name
end
