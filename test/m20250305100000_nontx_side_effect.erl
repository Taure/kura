-module(m20250305100000_nontx_side_effect).
-behaviour(kura_migration).

-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [
        {execute, ~"CREATE TABLE IF NOT EXISTS nontx_side_effects (id serial PRIMARY KEY)"},
        {execute, ~"INSERT INTO nontx_side_effects DEFAULT VALUES"}
    ].

down() ->
    [{execute, ~"DROP TABLE IF EXISTS nontx_side_effects"}].
