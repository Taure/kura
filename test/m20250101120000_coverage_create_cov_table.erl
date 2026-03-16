-module(m20250101120000_coverage_create_cov_table).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [
        {create_table, <<"coverage_items">>, [
            #kura_column{name = id, type = id, primary_key = true, nullable = false},
            #kura_column{name = name, type = string, nullable = false},
            #kura_column{name = inserted_at, type = utc_datetime},
            #kura_column{name = updated_at, type = utc_datetime}
        ]}
    ].

down() ->
    [{drop_table, <<"coverage_items">>}].
