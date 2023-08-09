const VTYPE = Vector{Union{Dagger.Chunk,Dagger.EagerThunk}}

"""
    DTable

Structure representing the distributed table based on Dagger.

The table is stored as a vector of `Chunk` structures which hold partitions of the table.
That vector can also store `Dagger.EagerThunk` structures when an operation that modifies
the underlying partitions was applied to it (currently only `filter`).
"""
mutable struct DTable
    chunks::VTYPE
    tabletype
    schema::Union{Nothing,Schema}
end

DTable(chunks::Vector, tabletype) = DTable(VTYPE(chunks), tabletype, nothing)
DTable(chunks::Vector, tabletype, schema) = DTable(VTYPE(chunks), tabletype, schema)

"""
    DTable(table; tabletype=nothing) -> DTable

Constructs a `DTable` using a `Tables.jl`-compatible input `table`.
Calls `partitions` on `table` and assumes the provided partitioning.
"""
function DTable(table; tabletype=nothing)
    chunks = Vector{Dagger.Chunk}()
    type = nothing
    sink = nothing
    for partition in partitions(table)
        if sink === nothing
            sink = materializer(tabletype !== nothing ? tabletype() : partition)
        end

        tpart = sink(partition)
        push!(chunks, Dagger.tochunk(tpart))

        type === nothing && (type = typeof(tpart).name.wrapper)
    end
    return DTable(chunks, type)
end

"""
    DTable(table, chunksize; tabletype=nothing, interpartition_merges=true) -> DTable

Constructs a `DTable` using a `Tables.jl` compatible `table` input.
It assumes no initial partitioning of the table and uses the `chunksize`
argument to partition the table (based on row count).

Providing `tabletype` kwarg overrides the internal table partition type.

Using the `interpartition_merges` kwarg you can decide whether you want to opt out of
merging rows between partitions. This option is enabled by default, which means it will
prioritize creating chunks of the specified size even if it means taking rows from two or
more partitions. When disabled there won't be any merges between partitions meaning several
chunks can be smaller than expected due to shortage of rows within a partition.
Please see tests for examples of behaviour.
"""
function DTable(table, chunksize::Integer; tabletype=nothing, interpartition_merges=true)
    chunks = Dagger.Chunk[]
    type = nothing
    sink = nothing

    leftovers = nothing
    leftovers_length = 0

    for partition in partitions(table)
        if sink === nothing
            sink = materializer(tabletype !== nothing ? tabletype() : partition)
        end

        if interpartition_merges && leftovers !== nothing
            inner_partitions = partitions(
                TableOperations.makepartitions(sink(partition), chunksize - leftovers_length)
            )

            merged_data = sink(
                TableOperations.joinpartitions(
                    partitioner(identity, [leftovers, sink(first(inner_partitions))])
                ),
            )

            if length(inner_partitions) == 1
                leftovers = merged_data
                leftovers_length = length(rows(leftovers))
                if leftovers_length == chunksize
                    # sometimes the next partition will be exactly the size of
                    # the chunksize - leftovers_length, so perfect match
                    push!(chunks, Dagger.tochunk(merged_data))
                    leftovers = nothing
                    leftovers_length = 0
                end
                continue
            else
                push!(chunks, Dagger.tochunk(merged_data))
                leftovers = nothing
                leftovers_length = 0
                partition = TableOperations.joinpartitions(
                    partitioner(identity, Iterators.drop(inner_partitions, 1))
                )
            end
        end

        inner_partitions = partitions(TableOperations.makepartitions(sink(partition), chunksize))

        for inner_partition in inner_partitions
            chunk_data = sink(inner_partition)
            chunk_data_rows = rows(chunk_data)

            if (
                interpartition_merges &&
                Base.haslength(chunk_data_rows) &&
                length(chunk_data_rows) < chunksize
            )
                # this is the last chunk with fewer than requested records
                # merge it with the first of the next partition
                leftovers = chunk_data
                leftovers_length = length(chunk_data_rows)
            else
                push!(chunks, Dagger.tochunk(chunk_data))
            end

            type === nothing && (type = typeof(chunk_data).name.wrapper)
        end
    end

    leftovers_length > 0 && push!(chunks, Dagger.tochunk(leftovers))

    return DTable(chunks, type)
end

"""
    DTable(loader_function, files::Vector{String}; tabletype=nothing)

Constructs a `DTable` using a list of filenames and a `loader_function`.
Partitioning is based on the contents of the files provided, which means that
one file is used to create one partition.

Providing `tabletype` kwarg overrides the internal table partition type.
"""
function DTable(loader_function::Function, files::Vector{String}; tabletype=nothing)
    chunks = Dagger.EagerThunk[
        Dagger.spawn(_file_load, file, loader_function, tabletype) for file in files
    ]
    return DTable(chunks, tabletype)
end

function _file_load(filename::AbstractString, loader_function::Function, tabletype::Any)
    part = loader_function(filename)
    sink = materializer(tabletype === nothing ? part : tabletype())
    tpart = sink(part)
    return tpart
end

"""
    fetch(d::DTable)

Collects all the chunks in the `DTable` into a single, non-distributed
instance of the underlying table type.

Fetching an empty DTable results in returning an empty `NamedTuple` regardless of the underlying `tabletype`.
"""
function fetch(d::DTable)
    sink = materializer(tabletype(d)())
    return sink(retrieve_partitions(d))
end

"""
    fetch(d::DTable, sink)

Collects all the chunks in the `DTable` into a single, non-distributed
instance of table type created using the provided `sink` function.
"""
fetch(d::DTable, sink) = sink(retrieve_partitions(d))

function retrieve_partitions(d::DTable)
    d2 = trim(d)
    return if nchunks(d2) > 0
        TableOperations.joinpartitions(partitioner(retrieve, d2.chunks))
    else
        NamedTuple()
    end
end

retrieve(x::Dagger.EagerThunk) = fetch(x)
retrieve(x::Dagger.Chunk) = collect(x)

"""
    tabletype!(d::DTable)

Provides the type of the underlying table partition and caches it in `d`.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype!(d::DTable) = d.tabletype = resolve_tabletype(d)

"""
    tabletype(d::DTable)

Provides the type of the underlying table partition.
Uses the cached tabletype if available.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype(d::DTable) = d.tabletype === nothing ? resolve_tabletype(d) : d.tabletype

function resolve_tabletype(d::DTable)
    _type = c -> isnonempty(c) ? typeof(c).name.wrapper : nothing
    t = nothing

    if nchunks(d) > 0
        for chunk in d.chunks
            t = fetch(Dagger.@spawn _type(chunk))
            t !== nothing && break
        end
    end
    return t !== nothing ? t : NamedTuple
end

function isnonempty(chunk)
    return length(rows(chunk)) > 0 && length(columnnames(chunk)) > 0
end

"""
    trim!(d::DTable) -> DTable

Removes empty chunks from `d`.
"""
function trim!(d::DTable)
    check_result = [Dagger.@spawn isnonempty(c) for c in d.chunks]
    d.chunks = getindex.(filter(x -> fetch(check_result[x[1]]), collect(enumerate(d.chunks))), 2)
    return d
end

"""
    trim(d::DTable) -> DTable

Returns `d` with empty chunks removed.
"""
trim(d::DTable) = trim!(DTable(d.chunks, d.tabletype))

show(io::IO, d::DTable) = show(io, MIME"text/plain"(), d)

function show(io::IO, ::MIME"text/plain", d::DTable)
    tabletype = d.tabletype === nothing ? "unknown (use `tabletype!(::DTable)`)" : d.tabletype
    println(io, "DTable with $(nchunks(d)) partitions")
    print(io, "Tabletype: $tabletype")
    return nothing
end

function chunk_lengths(table::DTable)
    f = x -> length(rows(x))
    return fetch.([Dagger.@spawn f(c) for c in table.chunks])
end

function length(table::DTable)
    return sum(chunk_lengths(table))
end

function first(table::DTable, rows::UInt)
    if nrow(table) == 0
        return table
    end

    chunk_length = chunk_lengths(table)[1]
    num_full_chunks = Int(floor(rows / chunk_length))       # number of required chunks
    sink = materializer(table.tabletype)
    if num_full_chunks * chunk_length == rows
        required_chunks = table.chunks[1:num_full_chunks]
    else
        # take only the needed rows from extra chunk
        needed_rows = rows - num_full_chunks * chunk_length
        extra_chunk = table.chunks[num_full_chunks + 1]
        extra_chunk_rows = rowtable(fetch(extra_chunk))
        new_chunk = Dagger.tochunk(sink(extra_chunk_rows[1:needed_rows]))
        required_chunks = vcat(table.chunks[1:num_full_chunks], [new_chunk])
    end
    return DTable(required_chunks, table.tabletype)
end

function columnnames_svector(d::DTable)
    colnames_tuple = determine_columnnames(d)
    return colnames_tuple !== nothing ? [sym for sym in colnames_tuple] : nothing
end

@inline nchunks(d::DTable) = length(d.chunks)

function merge_chunks(sink, chunks)
    return sink(TableOperations.joinpartitions(partitioner(retrieve, chunks)))
end

names(dt::DTable) = string.(columnnames_svector(dt))
names(dt::DTable, cols) = names(empty_dataframe(dt), cols)
propertynames(dt::DTable) = columnnames_svector(dt)

function wait(dt::DTable)
    for ch in dt.chunks
        !(ch isa Dagger.Chunk) && wait(ch)
    end
    return nothing
end

function isready(dt::DTable)
    return all([ch isa Dagger.Chunk ? true : (isready(ch); true) for ch in dt.chunks])
end

function getproperty(dt::DTable, s::Symbol)
    if s in fieldnames(DTable)
        return getfield(dt, s)
    else
        return DTableColumn(dt, s)
    end
end

ncol(d::DTable) = length(columns(d))
nrow(d::DTable) = length(d)
index(df::DTable) = Index(columnnames_svector(df))

function empty_dataframe(dt::DTable)
    s = determine_schema(dt)
    return DataFrame(
        Pair{Symbol}[s.names[i] => s.types[i][] for i in eachindex(s.names, s.types)];
        copycols=false,
    )
end
