-module(kura_test_listed_timestamps_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, timestamps/0]).

table() -> ~"listed_stamp_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string},
        #kura_field{name = created_at, type = utc_datetime},
        #kura_field{name = modified_at, type = utc_datetime, column = ~"changed_at"}
    ].

timestamps() -> [{created_at, utc_datetime}, {modified_at, utc_datetime}].
