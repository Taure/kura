-module(kura_test_indexed_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> <<"indexed_items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = code, type = string, nullable = false},
        #kura_field{name = category, type = string}
    ].

indexes() ->
    [{[code], #{unique => true}}].
