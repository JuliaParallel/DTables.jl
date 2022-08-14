using DTables
using Test

using Distributed

@info(
    "Execution environment details",
    julia_version=VERSION,
    n_workers=Distributed.nworkers(),
    n_procs=Distributed.nprocs(),
    n_threads=Threads.nthreads(),
)

@testset "DTables.jl" begin
    include("table.jl")
    include("table_dataframes.jl")
    include("column.jl")
end
