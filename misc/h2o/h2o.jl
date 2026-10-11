ENV["JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR"] = "true"
ENV["JULIA_MEMPOOL_EXPERIMENTAL_MEMORY_BOUND"] = string(8 * (2^30)) 

using CSV
using DTables
using DataFrames
using Tables
using OnlineStats

d = DTable(CSV.Chunks("misc/h2o/data/G1_1e8_1e2_0_0.csv",pool=true,ntasks=100))

gb = GroupBy(String7, Sum())
f = r -> (r.id1, r.v1)
@time a3 = fetch(DTables.mapreduce(f, fit!, d, init=gb))

# @time a1 = fetch(
#     reduce(
#         fit!, 
#         DTables.groupby(d,:id1),
#         cols=[:v1];
#         init=Sum()
#     )
# )

# @time a2 = fetch(
#     reduce(
#         fit!, 
#         DTables.groupby(d,:id1),
#         cols=[:v1];
#         init=Sum()
#     )
# )