@testset "DTableColumn" begin
    col_a = collect(1:100_000).%10_000
    col_b = rand(100_000)
    nt = (a=col_a, b=col_b)
    d = DTable(nt, 10_000)

    @test collect(DTableColumn(d, 1)) == col_a
    @test collect(DTableColumn(d, "a")) == col_a
    @test collect(DTableColumn(d, :a)) == col_a
    @test collect(DTableColumn(d, 2)) == col_b
    @test collect(DTableColumn(d, "b")) == col_b
    @test collect(DTableColumn(d, :b)) == col_b
end
