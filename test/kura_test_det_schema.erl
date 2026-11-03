-module(kura_test_det_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"det_users".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{
            name = email,
            type = {encrypted, string, #{mode => deterministic, normalize => casefold}}
        },
        #kura_field{name = ssn, type = {encrypted, string, #{mode => deterministic}}},
        #kura_field{name = token, type = {encrypted, uuid, #{mode => deterministic}}},
        #kura_field{name = pin, type = {encrypted, integer, #{mode => deterministic}}},
        #kura_field{name = note, type = {encrypted, text}},
        #kura_field{name = secret, type = {encrypted, text, #{}}}
    ].
