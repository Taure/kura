-module(m20250303110000_nontx_refuse_index).
-behaviour(kura_migration).

-export([up/0, down/0]).

up() ->
    [{create_index, ~"nontx_refuse_items_by_name", ~"nontx_refuse_items", [name], [concurrently]}].

down() ->
    [{drop_index, ~"nontx_refuse_items_by_name"}].
