-module(kura_test_cte_scope_note_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"cte_scope_notes".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = org_id, type = integer, nullable = false},
        #kura_field{name = name, type = string}
    ].
