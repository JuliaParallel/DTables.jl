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

    iterator_type = Nothing
    c_idx = 1
    while iterator_type === Nothing && c_idx <= nchunks(d)
        iterator_type = fetch(
            Dagger.spawn(
                (ch, _col) -> typeof(iterate(getcolumn_chunk(ch, _col))), d.chunks[c_idx], col
            ),
        )
        c_idx += 1
    end

    return DTableColumn{column_eltype,iterator_type}(
        d, col, _columnnames_svector(d)[col], chunk_lengths(d), 0, nothing, nothing
    )
end

function DTableColumn(d::DTable, col::String)
    return DTableColumn(d, only(indexin([col], string.(_columnnames_svector(d)))))
end
DTableColumn(d::DTable, col::Symbol) = DTableColumn(d, string(col))

length(dtc::DTableColumn) = sum(dtc.chunk_lengths)

function pull_next_chunk!(dtc::DTableColumn)
    # find first non-empty chunk
    while dtc._iter === nothing
        dtc._chunk += 1
        if dtc._chunk <= nchunks(dtc.dtable)
            dtc._chunkstore = fetch(
                Dagger.spawn(getcolumn_chunk, dtc.dtable.chunks[dtc._chunk], dtc.col)
            )
        else
            return nothing
        end
        # iterate in case this chunk is empty
        dtc._iter = iterate(dtc._chunkstore)
    end
    return nothing
end

function iterate(dtc::DTableColumn)
    length(dtc) == 0 && return nothing

    # on every iteration start reset the cache
    dtc._chunkstore = nothing
    dtc._iter = nothing
    dtc._chunk = 0

    # pull the first chunk
    pull_next_chunk!(dtc)

    return dtc._iter
end

function iterate(dtc::DTableColumn, iter)
    dtc._chunkstore === nothing && return nothing
    dtc._iter = iterate(dtc._chunkstore, iter)
    pull_next_chunk!(dtc)
    return dtc._iter
end
