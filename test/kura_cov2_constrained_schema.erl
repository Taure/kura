-module(kura_cov2_constrained_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, constraints/0]).

table() -> <<"constrained_items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = age, type = integer}
    ].

constraints() ->
    [
        {unique, [name]},
        {check, <<"age > 0">>}
    ].
