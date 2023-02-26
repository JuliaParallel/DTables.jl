module DTables

############################################################################################
# Using
############################################################################################

using Dagger: Dagger
using DataAPI: All, Between, Cols
using DataFrames:
    DataFrame,
    AsTable,
    ColumnIndex,
    MultiColumnIndex,
    normalize_selection,
    Index,
    make_pair_concrete
using InvertedIndices: Not
using SentinelArrays: ChainedVector
using Statistics: mean
using TableOperations: TableOperations
using Tables:
    columnindex,
    columnnames,
    ByRow,
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
import DataFrames: broadcast_pair, combine, select, index, transform

############################################################################################
# Export
############################################################################################

export All,
    AsTable,
    Between,
    ByRow,
    combine,
    Cols,
    DTable,
    DTableColumn,
    innerjoin,
    leftjoin,
    ncol,
    Not,
    nrow,
    select,
    tabletype,
    tabletype!,
    transform,
    trim,
    trim!
############################################################################################

include("table/dtable.jl")
include("table/gdtable.jl")
include("table/tables.jl")
include("table/dtable_column.jl")
include("operations/operations.jl")
include("operations/groupby.jl")
include("operations/join_interface.jl")
include("operations/join.jl")
include("operations/dataframes_interface_utils.jl")
include("operations/dataframes_interface.jl")

end
