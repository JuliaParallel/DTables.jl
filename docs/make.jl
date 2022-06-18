using DTables
using Documenter

DocMeta.setdocmeta!(DTables, :DocTestSetup, :(using DTables); recursive=true)

makedocs(;
    modules=[DTables],
    authors="Krystian Guliński, Julian Samaroo, and contributors",
    repo="https://github.com/juliaparallel/DTables.jl/blob/{commit}{path}#{line}",
    sitename="DTables.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://juliaparallel.github.io/DTables.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/juliaparallel/DTables.jl",
    devbranch="master",
)
