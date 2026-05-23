using DTables
using DataFrames
using Random

rng = MersenneTwister(2137)

a_len = 1000
b_len = 100

d1 = DataFrame(a=rand(rng, Int32, a_len) .% 100, b=collect(1:a_len))
d2 = DataFrame(a=rand(rng, Int32, b_len) .% 100, c=collect(1:b_len))

lj_ref = leftjoin(d1, d2, on=:a)
lj10 = fetch(leftjoin(DTable(d1, a_len ÷ 10), DTable(d2, b_len ÷ 10), on=:a), DataFrame)

sort!(lj_ref, propertynames(lj_ref))
sort!(lj10,   propertynames(lj_ref))

@assert isequal(lj_ref, lj10) "leftjoin mismatch: ref $(nrow(lj_ref)) rows, got $(nrow(lj10)) rows"
println("leftjoin: $(nrow(lj10)) rows (ref $(nrow(lj_ref)))")

ij_ref = innerjoin(d1, d2, on=:a)
ij10 = fetch(innerjoin(DTable(d1, a_len ÷ 10), DTable(d2, b_len ÷ 10), on=:a), DataFrame)

sort!(ij_ref, propertynames(ij_ref))
sort!(ij10,   propertynames(ij_ref))

@assert isequal(ij_ref, ij10) "innerjoin mismatch: ref $(nrow(ij_ref)) rows, got $(nrow(ij10)) rows"
println("innerjoin: $(nrow(ij10)) rows (ref $(nrow(ij_ref)))")

println("All assertions passed")
