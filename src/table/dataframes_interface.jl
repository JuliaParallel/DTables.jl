struct DTablesWrapper <: AbstractDataFrame
    d::DTable
end

broadcast_pair(df::DTable, p) = broadcast_pair(DTablesWrapper(df), p)

function manipulate(
    df::DTable, @nospecialize(cs...); copycols::Bool, keeprows::Bool, renamecols::Bool
)
    cs_vec = []
    for v in cs
        if v isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    normalized_cs = Any[
        normalize_selection(index(df), make_pair_concrete(c), renamecols) for c in cs_vec
    ]
    return _manipulate(df, normalized_cs, copycols, keeprows)
end

# Not copied - full custom implementation
function _manipulate(df::DTable, normalized_cs::Vector{Any}, copycols::Bool, keeprows::Bool)
    #########
    # STAGE 1: Spawning full column thunks - also multicolumn when needed (except identity)
    # These get saved later and used in last stages.
    #########
    colresults = Dict{Int,Any}()
    for (i, (colidx, (f, _))) in enumerate(normalized_cs)
        if !(colidx isa AsTable) && !(f isa ByRow) && f != identity
            if length(colidx) > 0
                cs = DTableColumn.(Ref(df), [colidx...])
                colresults[i] = Dagger.@spawn f(cs...)
            else
                colresults[i] = Dagger.@spawn f() # case of select(d, [] => fun)
            end
        end
    end

    #########
    # STAGE 2: Fetching full column thunks with result of length 1
    # These will be just injected as values in the mapping, because it's a vector full of these values
    #########

    colresults = Dict{Int,Any}(
        k => fetch(Dagger.spawn(length, v)) == 1 ? fetch(v) : v for (k, v) in colresults
    )

    mapmask = [
        haskey(colresults, x) && colresults[x] isa Dagger.EagerThunk for
        (x, _) in enumerate(normalized_cs)
    ]

    mappable_part_of_normalized_cs = filter(x -> !mapmask[x[1]], collect(enumerate(normalized_cs)))

    #########
    # STAGE 3: Mapping function (need to ensure this is compiled only once)
    # It's awful right now, but it covers all cases
    # Essentially we skip all the non-mappable stuff here
    #########

    rd = map(x -> select_rowfunction(x, mappable_part_of_normalized_cs, colresults), df)

    #########
    # STAGE 4: Preping for last stage - getting all the full column thunks with not 1 lengths
    #########
    cpcolresults = Dict{Int,Any}()

    for (k, v) in colresults
        if v isa Dagger.EagerThunk
            cpcolresults[k] = v
        end
    end

    for (_, v) in colresults
        if v isa Dagger.EagerThunk
            if fetch(Dagger.spawn(length, v)) != length(df)
                throw("result column is not the size of the table")
            end
        end
    end
    #########
    # STAGE 5: Fill columns - meaning the previously omitted full column tasks
    # will be now merged into the final DTable
    #########

    rd = fillcolumns(rd, cpcolresults, normalized_cs, chunk_lengths(df))

    return rd
end

# Not copied - full custom implementation
# There's a copymetadata here now
function manipulate(
    dt::DTable, args::AbstractVector{Int}; copycols::Bool, keeprows::Bool, renamecols::Bool
)
    colidx = first(args)
    colname = columnnames(columns(dt))[colidx]
    return map(r -> (; colname => getcolumn(r, colidx)), dt)
end

# Copied as is from DataFrames.jl
function manipulate(
    df::DTable, c::MultiColumnIndex; copycols::Bool, keeprows::Bool, renamecols::Bool
)
    if c isa AbstractVector{<:Pair}
        return manipulate(df, c...; copycols=copycols, keeprows=keeprows, renamecols=renamecols)
    else
        return manipulate(
            df, index(df)[c]; copycols=copycols, keeprows=keeprows, renamecols=renamecols
        )
    end
end

# Copied as is from DataFrames.jl
function manipulate(df::DTable, c::ColumnIndex; copycols::Bool, keeprows::Bool, renamecols::Bool)
    return manipulate(
        df, Int[index(df)[c]]; copycols=copycols, keeprows=keeprows, renamecols=renamecols
    )
end

"""
    select(df::DTable, args...; copycols::Bool=true, renamecols::Bool=true)

Create a new DTable that contains columns from `df` specified by `args` and return it.
The result is guaranteed to have the same number of rows as df, except when no columns
are selected (in which case the result has zero rows).

This operation is supposed to provide the same functionality and syntax as `DataFrames.select`,
but for DTable input. Most cases should be covered and the output should be exactly the
same as one obtained using DataFrames. In case of output differences or `args` causing errors
please file an issue with reproduction steps and data.

Please refer to DataFrames documentation for more details on usage.
"""
function select(df::DTable, @nospecialize(args...); copycols::Bool=true, renamecols::Bool=true)
    return manipulate(
        df,
        map(x -> broadcast_pair(df, x), args)...;
        copycols=copycols,
        keeprows=true,
        renamecols=renamecols,
    )
end
