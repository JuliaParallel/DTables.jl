using Dagger

# Reproduces the actual DTables bug pattern:
#
# In the original buggy code, all outer tasks (one per l-chunk) held a reference to the
# same d2::DTable object. Each called resolve_colnames(l_chunk, d2, on), which called
# determine_schema(d2). determine_schema:
#   1. Checks d2.schema -- initially nothing
#   2. Spawns a new inner task from inside the running outer task (nested spawn)
#   3. fetch()es that inner task (blocks the worker thread)
#   4. Writes the result back: d2.schema = computed_schema  <-- race
#
# This combines two issues: data race on shared mutable state + nested spawn+fetch.

mutable struct LazyTable
    chunks::Vector{Dagger.EagerThunk}
    schema::Union{Nothing,Vector{Symbol}}   # lazily cached, like DTable.schema
end

# Mirrors determine_schema: nested spawn+fetch, then mutates the shared field.
function schema_of(t::LazyTable)
    t.schema !== nothing && return t.schema
    # Nested spawn from inside a running task, then block on it:
    computed = fetch(Dagger.spawn(chunk -> [:col_a, :col_b], t.chunks[1]))
    return t.schema = computed      # concurrent write to shared mutable struct
end

function process_chunk(l_data, r_table::LazyTable)
    s = schema_of(r_table)          # calls nested spawn+fetch on shared object
    sleep(rand() * 0.001)
    return length(s)                # should always be 2
end

any_wrong = false

for trial in 1:30
    r_chunks = [Dagger.@spawn fill(1, 10) for _ in 1:6]
    r_table  = LazyTable(r_chunks, nothing)

    l_list       = [rand(Int, 3) for _ in 1:16]
    outer_tasks  = [Dagger.@spawn process_chunk(ld, r_table) for ld in l_list]
    results      = fetch.(outer_tasks)

    correct = all(==(2), results)
    mark    = correct ? "✓" : "✗ BUG got $results"
    println("Trial $trial  $mark")
    global any_wrong = any_wrong || !correct
end

println()
if any_wrong
    @error "BUG REPRODUCED: concurrent schema mutation / nested spawn+fetch"
    exit(1)
else
    println("All 30 trials correct")
    exit(0)
end
