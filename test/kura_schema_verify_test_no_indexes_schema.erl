-module(kura_schema_verify_test_no_indexes_schema).
-behaviour(kura_schema).

-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).

table() -> <<"kura_verify_test_no_idx">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = body, type = text}
    ].
