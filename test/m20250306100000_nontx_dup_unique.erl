-module(m20250306100000_nontx_dup_unique).
-behaviour(kura_migration).

-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [
        {execute, ~"CREATE TABLE IF NOT EXISTS nontx_dups (email text)"},
        {execute, ~"INSERT INTO nontx_dups VALUES ('dup@example.se'), ('dup@example.se')"},
        {create_index, ~"nontx_dups", [email], #{unique => true, concurrently => true}}
    ].

down() ->
    [{execute, ~"DROP TABLE IF EXISTS nontx_dups"}].
