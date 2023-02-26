
function select_rowfunction(row, mappable_part_of_normalized_cs)
    _cs = [
        begin
            kk = result_colname === AsTable ? Symbol("AsTable$(i)") : result_colname
            vv = begin
                args = if colidx isa AsTable
                    (;
                        [
                            k => getcolumn(row, k) for
                            k in getindex.(Ref(columnnames(row)), colidx.cols)
                        ]...
                    )
                else
                    getcolumn.(Ref(row), colidx)
                end

                if f isa ByRow && !(colidx isa AsTable) && length(colidx) == 0
                    f.fun()
                elseif f isa ByRow
                    f.fun(args)
                elseif f == identity
                    args
                else
                    throw(ErrorException("Weird unhandled stuff"))
                end
            end
            kk => vv
        end for (i, (colidx, (f, result_colname))) in mappable_part_of_normalized_cs
    ]
    return (; _cs...)
end

function fillcolumn(
    chunk,
    csymbols::Union{Vector{DataType},Vector{Symbol},Vector{Union{DataType,Symbol}}},
    colfragments::Union{Vector{Dagger.EagerThunk},Vector{Any}},
    expected_chunk_length::Int,
    normalized_cs::Vector{Any},
)
    col_vecs_fetched = fetch.(colfragments)
    colnames = Vector{Symbol}()
    cols = Vector{Any}()
    last_astable = 0

    for (idx, (_, (_, sym))) in enumerate(normalized_cs)
        if sym !== AsTable
            col = if sym in csymbols
                index = findfirst(x -> x === sym, csymbols)
                if col_vecs_fetched[index] isa AbstractVector
                    col_vecs_fetched[index]
                else
                    repeat([col_vecs_fetched[index]], expected_chunk_length)
                end
            else
                getcolumn(chunk, sym)
            end
            push!(colnames, sym)
            push!(cols, col)
        elseif sym === AsTable
            i = findfirst(x -> x === AsTable, csymbols[(last_astable + 1):end])
            c = if i === nothing
                getcolumn(chunk, Symbol("AsTable$(idx)"))
            else
                last_astable = i
                if col_vecs_fetched[i] isa AbstractVector
                    col_vecs_fetched[i]
                else
                    repeat([col_vecs_fetched[i]], expected_chunk_length)
                end
            end

            push!.(Ref(colnames), columnnames(columns(c)))
            push!.(Ref(cols), getcolumn.(Ref(columns(c)), columnnames(columns(c))))
        else
            throw(ErrorException("something is off"))
        end
    end
    return materializer(chunk)(
        merge(NamedTuple(), (; [e[1] => e[2] for e in zip(colnames, cols)]...))
    )
end

function fillcolumns(
    dt::Union{Nothing,DTable},
    normalized_cs_results::Dict{Int,Dagger.EagerThunk},
    normalized_cs::Vector{Any},
    new_chunk_lengths::Vector{Int},
    fullcolumn_ops_result_lengths::Vector{Int},
)
    fullcolumn_ops_indices_in_normalized_cs = collect(keys(normalized_cs_results))::Vector{Int}
    fullcolumn_ops_results_ordered = map(
        x -> normalized_cs_results[x], fullcolumn_ops_indices_in_normalized_cs
    )::Union{Vector{Any},Vector{Dagger.EagerThunk}}

    colfragment = (column, s, e) -> Dagger.@spawn getindex(column, s:e)
    result_column_symbols =
        getindex.(Ref(map(x -> x[2][2], normalized_cs)), fullcolumn_ops_indices_in_normalized_cs)

    dtchunks = if dt === nothing
        [Dagger.spawn(() -> nothing) for _ in 1:length(new_chunk_lengths)]
    else
        dt.chunks
    end
    dtchunks_filled = [
        x <= length(dtchunks) ? dtchunks[x] : Dagger.spawn(() -> nothing) for
        x in 1:length(new_chunk_lengths)
    ]

    chunks = Dagger.EagerThunk[
        Dagger.spawn(
            fillcolumn,
            chunk,
            result_column_symbols,
            [
                if len > 1
                    colfragment(
                        column, 1 + sum(new_chunk_lengths[1:(i - 1)]), sum(new_chunk_lengths[1:i])
                    )
                else
                    column
                end for
                (column, len) in zip(fullcolumn_ops_results_ordered, fullcolumn_ops_result_lengths)
            ],
            new_chunk_length,
            normalized_cs,
        ) for
        (i, (chunk, new_chunk_length)) in enumerate(zip(dtchunks_filled, new_chunk_lengths)) if
        new_chunk_length > 0
    ]
    if dt === nothing
        return DTable(chunks, nothing)
    else
        return DTable(chunks, dt.tabletype)
    end
end
