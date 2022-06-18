using DTables
using Test

@testset "DTables.jl" begin
    include("table.jl")
    include("table_dataframes.jl")
end
