-module(kura_test_plain_ssn_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"plain_ssn".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = ssn, type = string},
        #kura_field{name = secret, type = string, virtual = true}
    ].
