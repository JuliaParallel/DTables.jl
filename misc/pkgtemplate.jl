using PkgTemplates

t = Template(;
    user="juliaparallel",
    authors="Krystian Guliński, Julian Samaroo, and contributors",
    julia=v"1.6",
    plugins=[
        CompatHelper(),
        Codecov(),
        Documenter{GitHubActions}(),
        BlueStyleBadge(),
        RegisterAction(),
        Tests(;project=true),
        GitHubActions(;extra_versions=["1.6", "1.7", "nightly"], windows=true, osx=true),
    ]
)

generate(t, "DTables")
