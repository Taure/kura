-module(kura_test_enc_float_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_float".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = amounts, type = {array, {encrypted, float}}},
        #kura_field{name = amount, type = {encrypted, float}}
    ].
