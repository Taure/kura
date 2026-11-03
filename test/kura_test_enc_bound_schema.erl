-module(kura_test_enc_bound_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_bound".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = ssn, type = {encrypted, string}},
        #kura_field{name = nickname, type = {encrypted, string}},
        #kura_field{name = tags, type = {array, {encrypted, text}}},
        #kura_field{name = ssn_confirmation, type = string, virtual = true}
    ].
