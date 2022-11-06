module DTables

############################################################################################
# Using
############################################################################################

using Dagger: Dagger
using DataAPI: BroadcastedSelector
using DataFrames:
    AbstractDataFrame,
    AsTable,
    ByRow,
    ColumnIndex,
    MultiColumnIndex,
    normalize_selection,
    Index,
    make_pair_concrete
using InvertedIndices: BroadcastedInvertedIndex
using SentinelArrays: ChainedVector
using TableOperations: TableOperations
using Tables:
    columnindex,
    columnnames,
    columns,
    columntable,
    getcolumn,
    materializer,
    partitioner,
    rows,
    schema,
    Schema

############################################################################################
# Extend
############################################################################################

import Base:
    fetch,
    filter,
    getindex,
    getproperty,
    isready,
    iterate,
    keys,
    length,
    map,
    mapreduce,
    names,
    propertynames,
    reduce,
    show,
    wait
import DataAPI: leftjoin, ncol, nrow, innerjoin
import Tables:
    columnaccess, columnnames, columns, getcolumn, istable, partitions, rowaccess, rows, schema
import DataFrames: broadcast_pair, select

############################################################################################
# Export
############################################################################################

export DTable, DTableColumn, innerjoin, leftjoin, select, tabletype, tabletype!, trim, trim!

############################################################################################

include("table/dtable.jl")
include("table/gdtable.jl")
include("table/tables.jl")
include("operations/operations.jl")
include("operations/groupby.jl")
include("operations/join_interface.jl")
include("operations/join.jl")
include("table/dtable_column.jl")
include("operations/dataframes_interface_utils.jl")
include("operations/dataframes_interface.jl")

end
