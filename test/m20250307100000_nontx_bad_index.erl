-module(m20250307100000_nontx_bad_index).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [
        {create_table, ~"nontx_bad_index_items", [
            #kura_column{name = id, type = id, primary_key = true, nullable = false},
            #kura_column{name = name, type = text}
        ]},
        {create_index, ~"nontx_bad_index_items", [name], #{using => bad_method()}}
    ].

down() ->
    [{drop_table, ~"nontx_bad_index_items"}].

-spec bad_method() -> dynamic().
bad_method() -> 'Hash'.
