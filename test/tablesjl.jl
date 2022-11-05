@testset "Tables.jl interface" begin
    @testset "tables.jl source" begin
        nt = (a=1:100, b=1:100)

        d1 = DTable(nt, 10) # standard row based constructor

        # partition constructor, check with DTable as input
        d2 = DTable(d1)
        d3 = DTable(Tables.partitioner(identity, [nt for _ in 1:10]))

        @test length(d1.chunks) == length(d2.chunks) == length(d3.chunks)

        @test Tables.getcolumn(d1, 1) == 1:100
        @test Tables.getcolumn(d1, 2) == 1:100
        @test Tables.getcolumn(d1, :a) == 1:100
        @test Tables.getcolumn(d1, :b) == 1:100
        @test DTables.determine_columnnames(d1) == (:a, :b)

        @test DTables.determine_schema(d1).names == (:a, :b)
        @test DTables.determine_schema(d1).types == (Int, Int)

        for c in Tables.columns(d1)
            @test c == 1:100
        end

        @test all([ r.a == r.b == v for (r,v) in zip(collect(Tables.rows(d1)),1:100)])

        # length tests for collect on iterators
        @test length(d1) == 100
        @test length(Tables.rows(d1)) == 100
        @test length(Tables.columns(d1)) == 2
        @test length(Tables.partitions(d1)) == 10

        # GDTable things

        g = DTables.groupby(d1, r -> r.a % 10, chunksize=3)
        t1 = Tables.columntable(Tables.rows(g))
        @test 1:100 == sort(t1.a) == sort(t1.b)
        t2 = collect(Tables.columns(g))
        @test 1:100 == sort(t2[1]) == sort(t2[2])

        for partition in Tables.partitions(g)
            @test partition isa DTable
            v = Tables.getcolumn(partition, :a)[1]
            @test all([el%10 == v%10 for el in Tables.getcolumn(partition, :a)])
        end
    end

    @testset "remaining utilities" begin
        nt = (; a=[1,2,3], b=[2,2,3])

        d = DTable(nt, 1)

        @test Tables.istable(d)
        @test Tables.rowaccess(d)
        @test Tables.columnaccess(d)

        @test Tables.getcolumn(Tables.columns(d), :a) == [1, 2, 3]

        gd = DTables.groupby(d, :b)

        @test Tables.istable(gd)
        @test Tables.rowaccess(gd)
        @test Tables.columnaccess(gd)
        @test Tables.schema(gd).names == (:a, :b)
        @test Tables.schema(gd).types == (Int64, Int64)
        @test Tables.getcolumn(gd, :a) == [1, 2, 3]
        @test Tables.getcolumn(gd, 1) == [1, 2, 3]
        @test Tables.columnnames(gd) == (:a, :b)
    end
end
