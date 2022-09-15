@testset "DTableColumn" begin
    S = 10_000
    col_a = collect(1:S)
    col_b = rand(S)
    nt = (a=col_a, b=col_b)
    d = DTable(nt, S ÷ 10)

    @test collect(DTableColumn(d, 1)) == col_a
    @test collect(DTableColumn(d, "a")) == col_a
    @test collect(DTableColumn(d, :a)) == col_a
    @test collect(DTableColumn(d, 2)) == col_b
    @test collect(DTableColumn(d, "b")) == col_b
    @test collect(DTableColumn(d, :b)) == col_b

    d2 = filter(x -> x.a <= S / 2, d)
    @test collect(DTableColumn(d2, 1)) == col_a[1:Int(S / 2)]
    @test collect(DTableColumn(d2, 2)) == col_b[1:Int(S / 2)]

    d2 = filter(x -> x.a >= S / 2, d)
    @test collect(DTableColumn(d2, 1)) == col_a[Int(S / 2):end]
    @test collect(DTableColumn(d2, 2)) == col_b[Int(S / 2):end]

    d2 = filter(x -> x.a < 0, d)
    @test collect(DTableColumn(d2, 1)) == col_a[1:-1]
    @test collect(DTableColumn(d2, 2)) == col_b[1:-1]
end
