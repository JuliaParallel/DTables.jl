mutable struct DTableColumn{T,TT}
    dtable::DTable
    col::Int
    colname::Symbol
    chunk_lengths::Vector{Int}
    _chunk::Int
    _iter::Union{Nothing,TT}
    _chunkstore::Union{Nothing,Vector{T}}
end

function getcolumn_chunk(chunk_contents, col::Int)
    return Tables.getcolumn(Tables.columns(chunk_contents), col)
end

function DTableColumn(d::DTable, col::Int)
    column_eltype = Tables.schema(Tables.columns(d)).types[col]
    iterator_type = fetch(Dagger.spawn(
        (ch, _col) -> typeof(iterate(getcolumn_chunk(ch, _col))),
        d.chunks[1],
        col
    ))

    DTableColumn{column_eltype,iterator_type}(
        d,
        col,
        _columnnames_svector(d)[col],
        chunk_lengths(d),
        0,
        nothing,
        nothing,
    )
end


DTableColumn(d::DTable, col::String) =
    DTableColumn(d, only(indexin([col], string.(_columnnames_svector(d)))))
DTableColumn(d::DTable, col::Symbol) = DTableColumn(d, string(col))

length(dtc::DTableColumn) = sum(dtc.chunk_lengths)


# function getindex(dtablecolumn::DTableColumn, idx::Int)
#     chunk_idx = 0
#     s = 1
#     for (i, e) in enumerate(dtablecolumn.chunk_lengths)
#         if s <= idx < s + e
#             chunk_idx = i
#             break
#         end
#         s = s + e
#     end
#     chunk_idx == 0 && throw(BoundsError())
#     offset = idx - s + 1
#     chunk = fetch(Dagger.spawn(getcolumn_chunk, dtablecolumn.dtable.chunks[chunk_idx], dtablecolumn.col))

#     row, iter = iterate(Tables.rows(chunk))
#     for _ in 1:(offset-1)
#         row, iter = iterate(Tables.rows(chunk), iter)
#     end
#     Tables.getcolumn(row, dtablecolumn.col)
# end


function pull_next_chunk(dtc::DTableColumn, c_idx::Int)
    # find first non-empty chunk
    while dtc._iter === nothing
        c_idx += 1
        if c_idx <= nchunks(dtc.dtable)
            dtc._chunkstore = fetch(Dagger.spawn(
                getcolumn_chunk,
                dtc.dtable.chunks[c_idx],
                dtc.col
            ))
        else
            dtc._chunk = c_idx
            return nothing
        end
        # iterate in case this chunk is empty
        dtc._iter = iterate(dtc._chunkstore)
    end
    dtc._chunk = c_idx
    return nothing
end


function iterate(dtc::DTableColumn)
    length(dtc) == 0 && return nothing

    # on every iteration start reset the cache
    dtc._chunkstore = nothing
    dtc._iter = nothing
    dtc._chunk = 0

    # pull the first chunk
    pull_next_chunk(dtc, 0)

    return dtc._iter
end

function iterate(dtc::DTableColumn, iter)
    dtc._chunkstore === nothing && return nothing
    dtc._iter = iterate(dtc._chunkstore, iter)
    pull_next_chunk(dtc, dtc._chunk)
    return dtc._iter
end
