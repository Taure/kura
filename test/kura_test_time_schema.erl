-module(kura_test_time_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"time_slots".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = at, type = time},
        #kura_field{name = big, type = bigint},
        #kura_field{name = small, type = smallint}
    ].
