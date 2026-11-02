-module(kura_test_enc_nested_array_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_nested_array".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = grid, type = {array, {array, {encrypted, string}}}}
    ].
