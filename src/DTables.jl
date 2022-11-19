module DTables

############################################################################################
# Using
############################################################################################

using Dagger: Dagger
using DataAPI: BroadcastedSelector
using InvertedIndices: Not
using SentinelArrays: ChainedVector
using TableOperations: TableOperations
using Tables:
    ByRow,
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

############################################################################################
# Export
############################################################################################

export DTable, DTableColumn, innerjoin, leftjoin, tabletype, tabletype!, trim, trim!, ByRow, ncol, nrow, Not

############################################################################################

include("new_module_index/SeparateModuleIndex.jl")
using .SeparateModuleIndex: Index, MultiColumnIndex, ColumnIndex, AsTable
import .SeparateModuleIndex: index

include("new_module/SeparateModule.jl")
using .SeparateModule
import .SeparateModule: normalize_selection


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
