-module(kura_migrator_pool_SUITE).
-moduledoc """
Regression coverage for `kura_migrator` pool-readiness handling.

These tests pin two production hazards:

1. `pgo` workers connect asynchronously, so when an application starts
   and immediately calls `kura_migrator:migrate/1` the pool may exist
   but reject queries with `none_available` until handshake completes.
   Before the `wait_for_pool/1` gate, the migrator's catch-all
   swallowed that as a generic `migration_failed` error and the app
   booted with **zero** migrations applied — silently. Fast CT
   startup hits this every time.

2. The migrator must report a real error (not silently succeed) when
   the configured pool genuinely doesn't exist, otherwise downstream
   schema queries fail later with confusing `relation "..." does not
   exist` messages instead of pointing at the actual problem.

The tests exercise both hazards: a happy path (pool up, migrate
applies versions and they show up in `schema_migrations`), a missing
pool (returns `{error, pool_unavailable}`), and a race scenario
(pool started immediately before `migrate/1`, must succeed without
a manual sleep).

`pgo_sup` is `simple_one_for_one`, which means we can't reliably
remove a started pool by name — `delete_child/2` is disallowed and
`terminate_child/2` needs a pid we don't track. To stay
order-independent the suite never tears the live pool down; for
"missing pool" cases we point the repo at a pool name that has never
been started.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("kura.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    wait_for_pool_ok_when_pool_running/1,
    wait_for_pool_returns_error_for_missing_pool/1,
    wait_for_pool_uses_configured_timeout/1,
    migrate_returns_error_when_pool_missing/1,
    migrate_succeeds_immediately_after_pool_start/1,
    migrate_actually_records_versions_in_schema_migrations/1,
    rollback_returns_error_when_pool_missing/1,
    status_returns_empty_when_pool_missing/1
]).

-define(LIVE_POOL, kura_migrator_pool_suite_live).
-define(MISSING_POOL, kura_migrator_pool_suite_missing).
-define(REPO, kura_migrator_pool_test_repo).
-define(APP, kura_migrator_pool_test_app).

all() ->
    [
        wait_for_pool_ok_when_pool_running,
        wait_for_pool_returns_error_for_missing_pool,
        wait_for_pool_uses_configured_timeout,
        migrate_returns_error_when_pool_missing,
        migrate_succeeds_immediately_after_pool_start,
        migrate_actually_records_versions_in_schema_migrations,
        rollback_returns_error_when_pool_missing,
        status_returns_empty_when_pool_missing
    ].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    %% Register a fake application that owns the migration modules so
    %% `kura_migrator:discover_migrations/1` finds them.
    MigMods = [
        m20250101120000_coverage_create_cov_table,
        m20250101130000_coverage_add_cov_index
    ],
    AppSpec =
        {application, ?APP, [
            {description, "test app for kura_migrator_pool_SUITE"},
            {vsn, "0.0.1"},
            {modules, [?REPO | MigMods]},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    ok = application:load(AppSpec),
    %% Skip ensure_database — connect directly to the existing kura_test DB.
    application:set_env(kura, ensure_database, false),
    Config.

end_per_suite(_Config) ->
    application:unset_env(kura, ensure_database),
    application:unset_env(kura, migration_pool_ready_timeout),
    application:unset_env(kura, migration_pool_ready_interval),
    application:unload(?APP),
    ok.

init_per_testcase(_TC, Config) ->
    %% Default each test to the missing pool. Tests that need a live pool
    %% switch the repo config to ?LIVE_POOL explicitly.
    set_repo_pool(?MISSING_POOL),
    %% Also wipe the migration-state tables so migrate tests start clean.
    cleanup_db_tables(),
    Config.

end_per_testcase(_TC, _Config) ->
    application:unset_env(kura, migration_pool_ready_timeout),
    application:unset_env(kura, migration_pool_ready_interval),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

wait_for_pool_ok_when_pool_running(_Config) ->
    %% Baseline: with a fully-warm pool, wait_for_pool/1 returns ok
    %% promptly. Anchors the success path so a regression of the
    %% wait/probe loop is caught.
    set_repo_pool(?LIVE_POOL),
    ensure_live_pool_ready(),
    ?assertEqual(ok, kura_migrator:wait_for_pool(?REPO)),
    ?assertEqual(ok, kura_migrator:wait_for_pool(?REPO, 100)).

wait_for_pool_returns_error_for_missing_pool(_Config) ->
    %% No pool by ?MISSING_POOL name => probe always fails => after
    %% timeout we get `{error, pool_unavailable}`. The previous
    %% behaviour was to swallow the failure and let downstream queries
    %% blow up with confusing "relation does not exist" messages.
    ?assertEqual(
        {error, pool_unavailable},
        kura_migrator:wait_for_pool(?REPO, 100)
    ).

wait_for_pool_uses_configured_timeout(_Config) ->
    %% wait_for_pool/1 reads the timeout from the kura app env. With a
    %% tight cap and no pool, it must give up close to the configured
    %% timeout — never silently extend the wait.
    application:set_env(kura, migration_pool_ready_timeout, 75),
    application:set_env(kura, migration_pool_ready_interval, 25),
    Start = erlang:monotonic_time(millisecond),
    Result = kura_migrator:wait_for_pool(?REPO),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ?assertEqual({error, pool_unavailable}, Result),
    %% Allow some slack for scheduler jitter; the point is it doesn't
    %% block indefinitely or silently use the 5s default.
    ?assert(Elapsed < 1000).

migrate_returns_error_when_pool_missing(_Config) ->
    %% Without wait_for_pool, this used to crash inside
    %% `with_migration_lock` and return a generic `{error, none_available}`
    %% — but only after appearing to "almost" work. We want a clean,
    %% explicit `{error, pool_unavailable}` so the caller can react.
    application:set_env(kura, migration_pool_ready_timeout, 100),
    ?assertEqual({error, pool_unavailable}, kura_migrator:migrate(?REPO)).

migrate_succeeds_immediately_after_pool_start(_Config) ->
    %% This is the pgo race that motivated wait_for_pool/1. Use a fresh
    %% pool name (not the suite-wide ?LIVE_POOL) so this test starts
    %% from genuine cold-start: pool started, then migrate/1 called in
    %% the same scheduling slice. pgo workers haven't finished their
    %% handshake yet, so the very first `pgo:query` would fail with
    %% `none_available`. With wait_for_pool the migrator retries until
    %% at least one worker is ready.
    ColdPool = make_unique_pool_name(),
    set_repo_pool(ColdPool),
    application:set_env(kura, migration_pool_ready_timeout, 5000),
    {ok, _} = pgo_sup:start_child(ColdPool, pool_config()),
    %% Deliberately do NOT sleep — the regression we're pinning is that
    %% migrate/1 must tolerate this exact zero-warmup window.
    Result = kura_migrator:migrate(?REPO),
    ?assertMatch({ok, _}, Result),
    {ok, Versions} = Result,
    ?assertEqual(2, length(Versions)).

migrate_actually_records_versions_in_schema_migrations(_Config) ->
    %% Don't trust the return value alone: validate the side effect that
    %% callers actually depend on. The original asobi failure mode was
    %% migrate/1 returning {error, none_available} silently, leaving
    %% schema_migrations empty AND tables uncreated — but later code
    %% paths still tried to use those tables. This test pins both: the
    %% versions table is populated AND the migrations' tables exist.
    set_repo_pool(?LIVE_POOL),
    ensure_live_pool_ready(),
    {ok, Versions} = kura_migrator:migrate(?REPO),
    ?assertEqual(2, length(Versions)),
    %% schema_migrations contains exactly those versions
    #{rows := Rows} = pgo:query(
        ~"SELECT version FROM schema_migrations ORDER BY version",
        [],
        #{pool => ?LIVE_POOL}
    ),
    Recorded = [V || #{version := V} <- Rows],
    ?assertEqual(lists:sort(Versions), Recorded),
    %% The table the create-table migration declares actually exists
    #{rows := _} = pgo:query(
        ~"SELECT 1 FROM coverage_items LIMIT 0", [], #{pool => ?LIVE_POOL}
    ).

rollback_returns_error_when_pool_missing(_Config) ->
    application:set_env(kura, migration_pool_ready_timeout, 100),
    ?assertEqual({error, pool_unavailable}, kura_migrator:rollback(?REPO)),
    ?assertEqual({error, pool_unavailable}, kura_migrator:rollback(?REPO, 5)).

status_returns_empty_when_pool_missing(_Config) ->
    %% status/1 is read-only and used by tooling; on missing pool we
    %% return [] rather than crashing the caller's process.
    application:set_env(kura, migration_pool_ready_timeout, 100),
    ?assertEqual([], kura_migrator:status(?REPO)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

set_repo_pool(PoolName) ->
    application:set_env(?APP, ?REPO, repo_config(PoolName)).

repo_config(PoolName) ->
    #{
        pool => PoolName,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 2
    }.

ensure_live_pool_ready() ->
    %% Idempotent: start the pool once, then wait until it can answer
    %% queries. Reused across tests via the suite-wide ?LIVE_POOL atom
    %% (pgo_sup is simple_one_for_one so we can't easily kill pools
    %% between tests).
    case pgo_sup:start_child(?LIVE_POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = kura_migrator:wait_for_pool(?REPO, 5000).

pool_config() ->
    #{
        host => "localhost",
        port => 5555,
        database => "kura_test",
        user => "postgres",
        password => "root",
        pool_size => 2,
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    }.

make_unique_pool_name() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    list_to_atom("kura_migrator_pool_suite_cold_" ++ Suffix).

cleanup_db_tables() ->
    %% Each test must start with no `coverage_items` table and no
    %% `schema_migrations` rows — otherwise the migrate tests can't
    %% distinguish "applied this run" from "leftover state". Use the
    %% live pool (started lazily) to do the cleanup, since we need
    %% *some* pool to issue SQL. After this runs, ?LIVE_POOL exists
    %% and is warm; tests that want "no pool" point at ?MISSING_POOL.
    case pgo_sup:start_child(?LIVE_POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = poll_pool_ready(?LIVE_POOL, 5000),
    _ = pgo:query(
        ~"DROP TABLE IF EXISTS coverage_items CASCADE", [], #{pool => ?LIVE_POOL}
    ),
    _ = pgo:query(
        ~"DROP TABLE IF EXISTS schema_migrations CASCADE", [], #{pool => ?LIVE_POOL}
    ),
    ok.

poll_pool_ready(Pool, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    poll_pool_loop(Pool, Deadline).

poll_pool_loop(Pool, Deadline) ->
    case pgo:query(~"SELECT 1", [], #{pool => Pool}) of
        #{rows := _} ->
            ok;
        {error, _} ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    {error, pool_not_ready};
                false ->
                    timer:sleep(50),
                    poll_pool_loop(Pool, Deadline)
            end
    end.
