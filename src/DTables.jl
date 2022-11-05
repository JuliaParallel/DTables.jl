module DTables

############################################################################################
# Using
############################################################################################

using Dagger: Dagger
using DataAPI: BroadcastedSelector
using DataFrames: AsTable, ByRow, ColumnIndex, MultiColumnIndex, normalize_selection, Index
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
    isready,
    iterate,
    iterate,
    keys,
    length,
    length,
    map,
    mapreduce,
    names,
    propertynames,
    reduce,
    show,
    wait,
    getproperty
import DataAPI: leftjoin, ncol, nrow, innerjoin
import Tables:
    columnaccess, columnnames, columns, getcolumn, istable, partitions, rowaccess, rows, schema

############################################################################################
# Export
############################################################################################

export DTable, DTableColumn, innerjoin, leftjoin, tabletype, tabletype!, trim, trim!

############################################################################################

include("table/dtable.jl")
include("table/gdtable.jl")
include("table/tables.jl")
include("table/operations.jl")
include("table/groupby.jl")
include("table/join_interface.jl")
include("table/join.jl")
include("table/dtable_column.jl")
include("table/dataframes_interface_utils.jl")
include("table/dataframes_interface.jl")

end
