-module(kura_test_tag).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, primary_key/0]).

table() -> <<"tags">>.

primary_key() -> id.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].
