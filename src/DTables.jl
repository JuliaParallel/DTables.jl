module DTables

import Dagger

include("table/dtable.jl")
include("table/gdtable.jl")
include("table/tables.jl")
include("table/operations.jl")
include("table/groupby.jl")
include("table/join_interface.jl")
include("table/join.jl")
include("table/dtable_column.jl")
include("table/dataframes_interface_utils.jl")
include("table/dataframes_interface.jl")

end
