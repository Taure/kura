-module(m20250101300000_ma_core_c).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [
        {create_table, ~"ma_core_c", [
            #kura_column{name = id, type = id, primary_key = true, nullable = false}
        ]}
    ].

down() ->
    [{drop_table, ~"ma_core_c"}].
