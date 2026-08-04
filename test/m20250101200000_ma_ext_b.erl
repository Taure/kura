-module(m20250101200000_ma_ext_b).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [
        {create_table, ~"ma_ext_b", [
            #kura_column{name = id, type = id, primary_key = true, nullable = false}
        ]}
    ].

down() ->
    [{drop_table, ~"ma_ext_b"}].
