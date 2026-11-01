-module(m20250302110000_nontx_fail_partial).
-behaviour(kura_migration).

-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [
        {create_index, ~"nontx_fail_items", [name], #{concurrently => true}},
        {execute, ~"SELECT * FROM nontx_no_such_table"}
    ].

down() ->
    [{drop_index, ~"nontx_fail_items_name_index"}].
