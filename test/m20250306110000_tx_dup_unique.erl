-module(m20250306110000_tx_dup_unique).
-behaviour(kura_migration).

-export([up/0, down/0]).

up() ->
    [
        {execute, ~"CREATE TABLE tx_dups (email text)"},
        {execute, ~"INSERT INTO tx_dups VALUES ('dup@example.se'), ('dup@example.se')"},
        {create_index, ~"tx_dups", [email], #{unique => true}}
    ].

down() ->
    [{execute, ~"DROP TABLE tx_dups"}].
