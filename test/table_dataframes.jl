import Dagger
using Test
using Tables
using DataFrames
using Statistics
import SentinelArrays

@testset "dtable-dataframes" begin
    @testset "select" begin
        s = 100_000
        nt = (a=collect(1:s) .% 3, b=rand(s))
        dt = DTable(nt, s ÷ 10)
        df = fetch(dt, DataFrame)

        t = (args...) -> begin
            dt_01 = DTables.select(dt, args...)
            df_01 = DataFrames.select(df, args...)

            result = try
                all(isapprox.(Tables.columns(df_01), Tables.columns(fetch(dt_01, DataFrame))))
            catch
                all(isequal.(Tables.columns(df_01), Tables.columns(fetch(dt_01, DataFrame))))
            end
            result
        end

        @test t(:a)
        @test t(1)
        @test t(:b)
        @test t(2)
        @test t(:a, :b)
        @test t(1, 2)
        @test t(:b, :a)
        @test t(2, 1)
        @test t(:b, :a, AsTable([:a, :b]) => ByRow(sum))
        @test t(:b, :a, AsTable(:) => ByRow(sum))
        @test t(AsTable([:a, :b]) => ByRow(sum))
        @test t(AsTable(:) => ByRow(sum))
        @test t([:a, :b] => ((x, y) -> x .+ y), :b, :a)
        @test t([:a, :b] => ((x, y) -> x .+ y), :b, :a, [:a, :b] => ((x, y) -> x .+ y) => :abfun2)
        @test t([:a, :a] => ((x, y) -> x .+ y))
        @test t(:a => sum)
        @test t(:a => sum, :a => mean)
        @test t(:a => sum, :b, :a)
        @test t(:b => sum, :a => sum, :b, :a)
        @test t(names(dt) .=> sum, names(dt) .=> mean .=> "test" .* names(dt))
        @test t(AsTable([:a, :b]) => ByRow(identity))
        @test t(AsTable([:a, :b]) => ByRow(identity) => AsTable)
        # @test # t(AsTable([:a, :b]) => identity) # this should technically fail on DTables
        @test t(AsTable([:a, :b]) => identity => AsTable)
        @test t([] => ByRow(() -> 1) => :x)
        @test fetch(DTables.select(dt, [] => ByRow(rand) => :x)).x isa SentinelArrays.ChainedVector{Float64, Vector{Float64}}
        @test fetch(DTables.select(dt, [] => (() -> rand(s)) => :x)).x isa SentinelArrays.ChainedVector{Float64, Vector{Float64}}
    end
end
