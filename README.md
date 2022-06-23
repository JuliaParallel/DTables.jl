# DTables

Distributed table structures and data manipulation operations built on top of [Dagger.jl](https://github.com/JuliaParallel/Dagger.jl)

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliaparallel.github.io/DTables.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliaparallel.github.io/DTables.jl/dev/)
[![Build Status](https://github.com/juliaparallel/DTables.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/juliaparallel/DTables.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/juliaparallel/DTables.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/juliaparallel/DTables.jl)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)

# Usage

Below you can find a quick example on how to get started with DTables.

There's a lot more you can do though, so please refer to the documentation!


```julia
# launch a Julia session with threads/workers

julia> using DTables

julia> dt = DTable((a=rand(100), b=rand(100)), 10)
DTable with 10 partitions
Tabletype: NamedTuple

julia> m = map(r -> (x=sum(r), id=Threads.threadid(),), dt)
DTable with 10 partitions
Tabletype: NamedTuple

julia> xsum = reduce((x, y) -> x + y, m, init=0, cols=[:x])
EagerThunk (running)

julia> threads_used = reduce((acc, el) -> union(acc, el), m, init=Set(), cols=[:id])
EagerThunk (running)

julia> fetch(xsum)
(x = 95.71209812014976,)

julia> fetch(threads_used)
(id = Set(Any[5, 4, 6, 13, 2, 10, 9, 12, 8, 3]),)
```
