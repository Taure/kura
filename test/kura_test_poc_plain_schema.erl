-module(kura_test_poc_plain_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"poc_plain".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = user_id, type = integer},
        #kura_field{name = ssn, type = binary}
    ].
