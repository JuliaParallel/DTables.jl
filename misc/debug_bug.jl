using DTables
using DataFrames
using Tables
using Dagger
using Random

rng = MersenneTwister(2137)
a_len = 1000
b_len = 100

d1 = DataFrame(a=rand(rng, Int32, a_len) .% 100, b=collect(1:a_len))
d2 = DataFrame(a=rand(rng, Int32, b_len) .% 100, c=collect(1:b_len))

lj_ref = leftjoin(d1, d2, on=:a)

# Instrumented version of the original _join(type, l_chunk, r::DTable) logic
function buggy_leftjoin_dtable_pair(l_chunk, r::DTables.DTable; on=nothing, kwargs...)
    names, _, other_r, cmp_l, cmp_r = DTables.resolve_colnames(l_chunk, r, on)

    process_one_chunk =
        (type, l, r_chunk, cmp_l, cmp_r, other_r, lookup, r_sorted, l_sorted, r_unique) -> begin
            id_l_in = objectid(l); id_r_in = objectid(r_chunk)
            hash_l_a = hash(l.a); hash_r_a = hash(r_chunk.a)
            nrow_l = nrow(l); nrow_r = nrow(r_chunk)
            inner_l, inner_r = DTables.match_inner_indices(
                l, r_chunk, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique
            )
            len_l = length(inner_l); len_r = length(inner_r)
            # Independent brute-force verification
            bf_count = 0
            for i in 1:nrow_l, j in 1:nrow_r
                if l.a[i] == r_chunk.a[j]
                    bf_count += 1
                end
            end
            match_ok = (len_l == bf_count) ? "OK" : "WRONG(expected $bf_count)"
            println(stderr, "VFY l=$(id_l_in%10000) r=$(id_r_in%10000) hl=$(hash_l_a%10000) hr=$(hash_r_a%10000) mii=$len_l bf=$bf_count $match_ok")
            flush(stderr)
            inner_df = DTables.build_joined_table(type, names, l, r_chunk, inner_l, inner_r, Set{UInt}(), other_r)
            inner_chunk = Dagger.tochunk(inner_df)
            built_nrow = nrow(inner_df)
            mii_at_build = length(inner_l)
            mii_match = (built_nrow == mii_at_build) ? "OK" : "BLT_MISMATCH(mii=$mii_at_build)"
            println(stderr, "BLT r=$(id_r_in%10000) n=$built_nrow cid=$(objectid(inner_chunk)%100000) $mii_match")
            flush(stderr)
            outer_l = DTables.find_outer_indices(l, inner_l)
            return inner_chunk, outer_l, built_nrow
        end

    vs = [
        Dagger.@spawn process_one_chunk(
            :leftjoin, l_chunk, chunk, cmp_l, cmp_r, other_r, nothing, false, false, false
        ) for chunk in r.chunks
    ]

    v = fetch.(vs)
    for (i, vi) in enumerate(v)
        c = vi[1]
        expected_nrow = vi[3]
        retrieved_nrow = nrow(DTables.retrieve(c))
        cid = objectid(c) % 100000
        match = retrieved_nrow == expected_nrow ? "OK" : "MISMATCH(expected $expected_nrow)"
        println(stderr, "RET i=$i cid=$cid expected=$expected_nrow got=$retrieved_nrow $match")
        flush(stderr)
    end
    to_merge = [vi[1] for vi in v]

    outer_l = intersect([vi[2] for vi in v]...)
    inner_l = inner_r = Vector{UInt}()
    outer = Dagger.tochunk(
        DTables.build_joined_table(:leftjoin, names, l_chunk, fetch(first(r.chunks)), inner_l, inner_r, outer_l, other_r)
    )
    push!(to_merge, outer)

    inner_rows = [nrow(DTables.retrieve(c)) for c in to_merge[1:end-1]]
    outer_rows = nrow(DTables.retrieve(outer))
    tid = Threads.threadid()
    lid = objectid(l_chunk)
    println(stderr, "MERGE[$tid/$lid] inner=$(sum(inner_rows)) outer=$outer_rows total=$(sum(inner_rows)+outer_rows)")
    flush(stderr)

    result = DTables.merge_chunks(Tables.materializer(l_chunk), to_merge)
    return result
end

# Original buggy outer spawn: each outer task calls buggy_leftjoin_dtable_pair
function buggy_leftjoin(d1::DTable, d2::DTable; kwargs...)
    f = (l, r, ks) -> buggy_leftjoin_dtable_pair(l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    return DTable(v, d1.tabletype)
end

dt1 = DTable(d1, a_len ÷ 10)
dt2 = DTable(d2, b_len ÷ 10)

println("Reference leftjoin: $(nrow(lj_ref)) rows")
println("dt1 has $(length(dt1.chunks)) chunks, dt2 has $(length(dt2.chunks)) chunks")

# Test 0: direct call (NOT inside a Dagger task)
println("\n=== Test: direct call (no Dagger outer task) ===")
l_chunk1_direct = fetch(dt1.chunks[1])
direct_result = buggy_leftjoin_dtable_pair(l_chunk1_direct, dt2; on=:a)
println("Direct call result: $(nrow(direct_result)) rows")

# Test 1: single outer task (process only the first l-chunk)
println("\n=== Test: single outer task ===")
l_chunk1 = fetch(dt1.chunks[1])
single_task = Dagger.@spawn buggy_leftjoin_dtable_pair(l_chunk1, dt2; on=:a)
sr = fetch(single_task)
println("Single outer task: $(nrow(sr)) rows (MISMATCH lines above?)")

# Test 2: full buggy join
println("\n=== Test: full buggy join ===")
for trial in 1:1
    result = fetch(buggy_leftjoin(dt1, dt2; on=:a), DataFrame)
    nrows = nrow(result)
    status = nrows == nrow(lj_ref) ? "OK" : "WRONG (expected $(nrow(lj_ref)))"
    println("Trial $trial: $nrows rows — $status")
end
