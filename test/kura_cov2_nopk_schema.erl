-module(kura_cov2_nopk_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"no_pk_items">>.

fields() ->
    [
        #kura_field{name = name, type = string},
        #kura_field{name = value, type = integer}
    ].
