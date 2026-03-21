-module(stream_bench_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"stream_bench".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = payload, type = text, nullable = false},
        #kura_field{name = value, type = integer},
        #kura_field{name = inserted_at, type = utc_datetime}
    ].
