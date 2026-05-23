#######################################
# Iterator structures

struct DTableRowIterator
    d::DTable
end

struct DTableColumnIterator
    d::DTable
end

struct DTablePartitionIterator
    d::DTable
end

#######################################
# DTable functions

istable(table::DTable) = true
rowaccess(table::DTable) = true
rows(table::DTable) = DTableRowIterator(table)
columnaccess(table::DTable) = true
columns(table::DTable) = DTableColumnIterator(table)

function determine_schema(table::DTable)
    table.schema !== nothing && return table.schema
    # Acquire per-table lock so that concurrent tasks (e.g. multiple outer join
    # tasks sharing the same DTable) don't race on reading/writing table.schema
    # and don't each spawn redundant nested Dagger tasks.
    lock(table.schema_lock) do
        table.schema !== nothing && return  # already set by a racing task
        c_idx = 1
        r = (false, nothing)
        while !r[1] && table.chunks !== nothing && c_idx <= length(table.chunks)
            # Fetch the chunk directly — no nested Dagger.spawn needed here.
            chunk_data = fetch(table.chunks[c_idx])
            has_data = isnonempty(chunk_data)
            r = (has_data, has_data ? schema(rows(chunk_data)) : nothing)
            c_idx += 1
        end
        table.schema = r[2]
    end
    return table.schema
end

function determine_columnnames(table::DTable)
    s = determine_schema(table)
    return s === nothing ? nothing : s.names
end

function _getcolumn(table::DTable, col::Union{Symbol,Int})
    chunk_col = (_chunk, _col) -> getcolumn(_chunk, _col)
    v = [Dagger.spawn(chunk_col, chunk, col) for chunk in table.chunks]
    return ChainedVector(fetch.(v))
end

getcolumn(table::DTable, col::Symbol) = _getcolumn(table, col)
getcolumn(table::DTable, idx::Int) = _getcolumn(table, idx)

#######################################
# DTableRowIterator functions

schema(table::DTableRowIterator) = determine_schema(table.d)
length(table::DTableRowIterator) = length(table.d)

function _iterate(iter::DTableRowIterator, chunk_index)
    i = nothing
    row_iterator = nothing
    while i === nothing && chunk_index <= nchunks(iter.d)
        partition = retrieve(iter.d.chunks[chunk_index])
        row_iterator = rows(partition)
        i = iterate(row_iterator)
        chunk_index += 1
    end
    if i === nothing
        return nothing
    else
        row, row_state = i
        next_chunk_index = chunk_index
        return (row, (row_iterator, row_state, next_chunk_index))
    end
end

iterate(iter::DTableRowIterator) = _iterate(iter, 1)

function iterate(iter::DTableRowIterator, state)
    (row_iterator, row_state, next_chunk_index) = state
    i = iterate(row_iterator, row_state)
    if i === nothing
        _iterate(iter, next_chunk_index)
    else
        row, row_state = i
        return (row, (row_iterator, row_state, next_chunk_index))
    end
end

#######################################
# DTableColumnIterator functions

schema(table::DTableColumnIterator) = determine_schema(table.d)
columnnames(table::DTableColumnIterator) = determine_columnnames(table.d)
getcolumn(table::DTableColumnIterator, col::Symbol) = getcolumn(table.d, col)
getcolumn(table::DTableColumnIterator, idx::Int) = getcolumn(table.d, idx)
length(table::DTableColumnIterator) = length(columnnames(table))

function _iterate(table::DTableColumnIterator, column_index)
    columns = columnnames(table)
    if (columns === nothing || length(columns) < column_index)
        return nothing
    else
        return (getcolumn(table, column_index), column_index + 1)
    end
end

iterate(table::DTableColumnIterator) = _iterate(table, 1)
iterate(table::DTableColumnIterator, state) = _iterate(table, state)

#######################################
# DTablePartitionIterator functions

partitions(table::DTable) = DTablePartitionIterator(table)
length(table::DTablePartitionIterator) = nchunks(table.d)

function _iterate(table::DTablePartitionIterator, chunk_index)
    nchunks(table.d) < chunk_index && return nothing
    return (retrieve(table.d.chunks[chunk_index]), chunk_index + 1)
end

iterate(table::DTablePartitionIterator) = _iterate(table, 1)
iterate(table::DTablePartitionIterator, state) = _iterate(table, state)

#######################################
# GDTable
# For normal rows/columns access it should act the same as a DTable

istable(table::GDTable) = true
rowaccess(table::GDTable) = true
rows(table::GDTable) = DTableRowIterator(table.dtable)
columnaccess(table::GDTable) = true
columns(table::GDTable) = DTableColumnIterator(table.dtable)
schema(table::GDTable) = determine_schema(table.dtable)
getcolumn(table::GDTable, col::Symbol) = getcolumn(table.dtable, col)
getcolumn(table::GDTable, idx::Int) = getcolumn(table.dtable, idx)
columnnames(table::GDTable) = determine_columnnames(table.dtable)

#######################################
# GDTable partitions
# Here it makes sense to provide partitions as full key groups
# Same as normal iteration over GDTable, but returns partitions only without keys

struct GDTablePartitionIterator
    d::GDTable
end

partitions(table::GDTable) = GDTablePartitionIterator(table)

function _iterate(table::GDTablePartitionIterator, it)
    if it === nothing
        return nothing
    else
        ((_, partition), index_iter_state) = it
        return (partition, index_iter_state)
    end
end

iterate(table::GDTablePartitionIterator) = _iterate(table, iterate(table.d))
function iterate(table::GDTablePartitionIterator, index_iter_state)
    return _iterate(table, iterate(table.d, index_iter_state))
end
