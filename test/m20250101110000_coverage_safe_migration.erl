-module(m20250101110000_coverage_safe_migration).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0, safe/0]).

up() ->
    [{alter_table, <<"coverage_items">>, [{drop_column, old_col}]}].

down() ->
    [
        {alter_table, <<"coverage_items">>, [
            {add_column, #kura_column{name = old_col, type = string}}
        ]}
    ].

safe() ->
    [{drop_column, old_col}].
