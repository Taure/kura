-module(kura_cov2_colmap_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"colmap_items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string, column = <<"custom_col">>}
    ].
