-module(m20250302120000_nontx_fail_after).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [
        {create_table, ~"nontx_fail_after", [
            #kura_column{name = id, type = id, primary_key = true, nullable = false}
        ]}
    ].

down() ->
    [{drop_table, ~"nontx_fail_after"}].
