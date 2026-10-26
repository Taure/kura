-module(m20250303100000_nontx_refuse_items).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [
        {create_table, ~"nontx_refuse_items", [
            #kura_column{name = id, type = id, primary_key = true, nullable = false},
            #kura_column{name = name, type = string}
        ]}
    ].

down() ->
    [{drop_table, ~"nontx_refuse_items"}].
