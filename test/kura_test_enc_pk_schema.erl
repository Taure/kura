-module(kura_test_enc_pk_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_pk".

fields() ->
    [
        #kura_field{name = ssn, type = {encrypted, string}, primary_key = true},
        #kura_field{name = name, type = string}
    ].
