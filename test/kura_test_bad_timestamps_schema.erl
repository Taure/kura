-module(kura_test_bad_timestamps_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, timestamps/0]).

table() -> ~"bad_stamped_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

timestamps() -> [{created_at, utc_datetime}].
