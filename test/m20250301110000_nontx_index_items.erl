-module(m20250301110000_nontx_index_items).
-behaviour(kura_migration).

-export([up/0, down/0, transaction/0]).

transaction() -> false.

up() ->
    [
        {execute, lock_held_by_this_session()},
        {create_index, ~"nontx_items", [name], #{concurrently => true}}
    ].

down() ->
    [
        {execute, lock_held_by_this_session()},
        {execute, ~"DROP INDEX CONCURRENTLY \"nontx_items_name_index\""}
    ].

%% Divides by zero unless the statement's own session holds the
%% migration advisory lock.
lock_held_by_this_session() ->
    ~"""
    SELECT 1 / count(*)::int FROM pg_locks
    WHERE locktype = 'advisory' AND objid = 571629482
      AND pid = pg_backend_pid() AND granted
    """.
