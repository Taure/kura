-module(kura_test_partial_upsert_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"partial_upserts">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = scope, type = string},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = value, type = jsonb, nullable = false}
    ].

associations() -> [].
