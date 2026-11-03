-module(kura_test_enc_dup_column_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_dup_column".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = a, type = {encrypted, string}, column = ~"b"},
        #kura_field{name = b, type = {encrypted, string}}
    ].
