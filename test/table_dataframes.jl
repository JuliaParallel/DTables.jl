import Dagger
using Test
using Tables
using DataFrames
using Statistics
using SentinelArrays: ChainedVector

@testset "dtable-dataframes" begin
    @testset "select" begin
        s = 10_000
        nt = (a=collect(1:s) .% 3, b=rand(s))
        dt = DTable(nt, s ÷ 10)
        df = fetch(dt, DataFrame)

        t = (args...) -> begin
            dt_01 = select(dt, args...)
            df_01 = select(df, args...)

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
        @test fetch(select(dt, [] => ByRow(rand) => :x)).x isa ChainedVector{Float64, Vector{Float64}}
        @test fetch(select(dt, [] => (() -> rand(s)) => :x)).x isa ChainedVector{Float64, Vector{Float64}}
    end

    @testset "names" begin
        v = DTable((a=[1], x1=[2], x2=[3], x3=[4], x4=[5]))
        @test names(v, All()) == names(v, :) == names(v) == ["a", "x1", "x2", "x3", "x4"]
        @test names(v, Between(:x1, :x3)) == ["x1", "x2", "x3"]
        @test names(v, Not(:a)) == names(v, r"x") == ["x1", "x2", "x3", "x4"]
        @test names(v, :x1) == names(v, 2) == ["x1"]
        @test names(v, Cols()) == names(v, Cols()) == []
    end

    @testset "combine" begin
        s = 10_000
        nt = (a=collect(1:s) .% 3, b=rand(s))
        dt = DTable(nt, s ÷ 10)
        df = fetch(dt, DataFrame)

        t = (args...) -> begin
            dt_01 = combine(dt, args...)
            df_01 = combine(df, args...)

            result = try
                all(isapprox.(Tables.columns(df_01), Tables.columns(fetch(dt_01, DataFrame))))
            catch
                all(isequal.(Tables.columns(df_01), Tables.columns(fetch(dt_01, DataFrame))))
            end
            result
        end

        @test t(:a => mean)
        @test t(:a => mean, :b)
        @test t(:a => mean, :b => mean)
        @test t(:a => mean, :b, :b => mean)
        @test t([] => (()-> ones(5_000)), :a => mean)
        @test t([] => (()-> ones(15_000)), :a => mean)
    end
end
