-module(kura_test_enc_bound_other_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_bound_other".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = ssn, type = {encrypted, string}}
    ].
