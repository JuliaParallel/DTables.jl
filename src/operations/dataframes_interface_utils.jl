
function select_rowfunction(row, mappable_part_of_normalized_cs, colresults)
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
                elseif length(colresults[i]) == 1
                    colresults[i]
                else
                    throw(ErrorException("Weird unhandled stuff"))
                end
            end
            kk => vv
        end for (i, (colidx, (f, result_colname))) in mappable_part_of_normalized_cs
    ]
    return (; _cs...)
end

function fillcolumns(
    dt::DTable,
    ics::Dict{Int,Any},
    normalized_cs::Vector{Any},
    chunk_lengths_of_original_dt::Vector{Int},
    col_lengths::Vector{Int},
)
    col_keys_indices = collect(keys(ics))::Vector{Int}
    col_vecs = map(x -> ics[x], col_keys_indices)::Union{Vector{Any},Vector{Dagger.EagerThunk}}

    f =
        (ch, csymbols, colfragments, expected_chunk_length) -> begin
            col_vecs_fetched = fetch.(colfragments)
            colnames = Vector{Symbol}()
            cols = Vector{Any}()
            last_astable = 0

            # if any(length.(col_vecs_fetched) .== 1)
            #     @warn "skip"
            #     return NamedTuple()
            # end
            # return NamedTuple()
            for (idx, (_, (_, sym))) in enumerate(normalized_cs)
                if sym !== AsTable
                    col = if sym in csymbols
                        index = something(indexin(csymbols, [sym])...)
                        if col_vecs_fetched[index] isa AbstractVector
                            col_vecs_fetched[index]
                        else
                            repeat([col_vecs_fetched[index]], expected_chunk_length)
                        end

                    else
                        getcolumn(ch, sym)
                    end
                    push!(colnames, sym)
                    push!(cols, col)
                elseif sym === AsTable
                    i = findfirst(x -> x === AsTable, csymbols[(last_astable + 1):end])
                    if i === nothing
                        c = getcolumn(ch, Symbol("AsTable$(idx)"))
                    else
                        last_astable = i
                        c = if col_vecs_fetched[i] isa AbstractVector
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
            materializer(ch)(
                merge(NamedTuple(), (; [e[1] => e[2] for e in zip(colnames, cols)]...))
            )
        end

    colfragment = (column, s, e) -> Dagger.@spawn getindex(column, s:e)
    clenghts = chunk_lengths_of_original_dt
    result_column_symbols = getindex.(Ref(map(x -> x[2][2], normalized_cs)), col_keys_indices)
    chunks = [
        begin
            cfrags = [
                begin
                    if len > 1
                        colfragment(column, 1 + sum(clenghts[1:(i - 1)]), sum(clenghts[1:i]))
                    else
                        column
                    end
                end for (column, len) in zip(col_vecs, col_lengths)
            ]
            Dagger.@spawn f(ch, result_column_symbols, cfrags, lens)
        end for (i, (ch, lens)) in enumerate(zip(dt.chunks, clenghts))
    ]
    return DTable(chunks, dt.tabletype)
end
