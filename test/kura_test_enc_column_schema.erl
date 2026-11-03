-module(kura_test_enc_column_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_column".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = ssn, type = {encrypted, string}, column = ~"ssn_ciphertext"},
        #kura_field{name = draft, type = {encrypted, string}, virtual = true}
    ].
