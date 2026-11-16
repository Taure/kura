-module(kura_test_malformed_timestamps_schema).
-moduledoc "A `timestamps/0` that is not a list of fields, as 2.30 let it be.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, timestamps/0]).

table() -> ~"malformed_stamps".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

-spec timestamps() -> dynamic().
timestamps() -> [inserted_at, updated_at].
