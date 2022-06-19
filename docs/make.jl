using DTables
using Documenter

DocMeta.setdocmeta!(DTables, :DocTestSetup, :(using DTables); recursive=true)

makedocs(;
    modules=[DTables],
    authors="Krystian Guliński, Julian Samaroo, and contributors",
    repo="https://github.com/JuliaParallel/DTables.jl/blob/{commit}{path}#{line}",
    sitename="DTables.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://juliaparallel.github.io/DTables.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "User guide" => "dtable.md",
        "API" => "api.md",
    ],
)

deploydocs(;
    repo="github.com/JuliaParallel/DTables.jl",
    devbranch="main",
)
