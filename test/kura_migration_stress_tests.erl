-module(kura_migration_stress_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(LOCK_KEY, 571629482).

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

migration_stress_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"advisory lock serializes concurrent acquirers",
                {timeout, 30, fun t_advisory_lock_serializes/0}},
            {"concurrent migrate - only one wins",
                {timeout, 30, fun t_concurrent_migrate_one_wins/0}},
            {"migration failure releases lock",
                {timeout, 30, fun t_migration_failure_releases_lock/0}},
            {"rollback correctness: up, insert, down, up again",
                {timeout, 30, fun t_rollback_correctness/0}}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_stress_repo:start(),
    kura_migrator:ensure_schema_migrations(kura_stress_repo),
    ok.

teardown(_) ->
    Pool = pool(),
    %% Clean up any test migration entries and tables
    kura_db:query_pool(
        Pool,
        <<"DELETE FROM schema_migrations WHERE version >= 99990000000000">>,
        []
    ),
    kura_db:query_pool(Pool, <<"DROP TABLE IF EXISTS mig_stress_test CASCADE">>, []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

pool() ->
    maps:get(pool, kura_repo:config(kura_stress_repo)).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_advisory_lock_serializes() ->
    Self = self(),
    N = 5,
    _Pids = [
        spawn_link(fun() ->
            kura_db:transaction_pool(pool(), fun() ->
                kura_db:query_pool(
                    pool(),
                    <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
                    [?LOCK_KEY]
                ),
                T1 = erlang:monotonic_time(microsecond),
                timer:sleep(100),
                T2 = erlang:monotonic_time(microsecond),
                Self ! {lock_held, I, T1, T2}
            end)
        end)
     || I <- lists:seq(1, N)
    ],
    Intervals = [
        receive
            {lock_held, I, T1, T2} -> {T1, T2}
        after 20000 -> error({timeout, I})
        end
     || I <- lists:seq(1, N)
    ],
    %% Sort by start time
    Sorted = lists:sort(Intervals),
    %% Verify no overlap: each start must be >= previous end
    check_no_overlap(Sorted).

check_no_overlap([_]) ->
    ok;
check_no_overlap([{_S1, E1}, {S2, E2} | Rest]) ->
    ?assert(
        S2 >= E1,
        io_lib:format("Overlap detected: prev ended at ~p, next started at ~p", [E1, S2])
    ),
    check_no_overlap([{S2, E2} | Rest]).

t_concurrent_migrate_one_wins() ->
    Pool = pool(),
    Version = 99990000000001,
    %% Clean up from any previous run
    kura_db:query_pool(
        Pool,
        <<"DELETE FROM schema_migrations WHERE version = $1">>,
        [Version]
    ),
    kura_db:query_pool(Pool, <<"DROP TABLE IF EXISTS mig_stress_test CASCADE">>, []),
    Self = self(),
    N = 5,
    %% Each process tries to "migrate" by acquiring the advisory lock and creating the table
    _Pids = [
        spawn_link(fun() ->
            Result = kura_db:transaction_pool(Pool, fun() ->
                kura_db:query_pool(
                    Pool,
                    <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
                    [?LOCK_KEY]
                ),
                %% Check if already applied
                case
                    kura_db:query_pool(
                        Pool,
                        <<"SELECT 1 FROM schema_migrations WHERE version = $1">>,
                        [Version]
                    )
                of
                    #{rows := [_ | _]} ->
                        already_applied;
                    #{rows := []} ->
                        kura_db:query_pool(
                            Pool,
                            <<
                                "CREATE TABLE IF NOT EXISTS mig_stress_test ("
                                "id BIGSERIAL PRIMARY KEY, val TEXT)"
                            >>,
                            []
                        ),
                        kura_db:query_pool(
                            Pool,
                            <<"INSERT INTO schema_migrations (version) VALUES ($1)">>,
                            [Version]
                        ),
                        applied
                end
            end),
            Self ! {migrate_result, I, Result}
        end)
     || I <- lists:seq(1, N)
    ],
    Results = [
        receive
            {migrate_result, I, R} -> R
        after 20000 -> error({timeout, I})
        end
     || I <- lists:seq(1, N)
    ],
    Applied = [R || R <- Results, R =:= applied],
    AlreadyApplied = [R || R <- Results, R =:= already_applied],
    ?assertEqual(1, length(Applied)),
    ?assertEqual(N - 1, length(AlreadyApplied)),
    %% Verify single entry in schema_migrations
    #{rows := Rows} = kura_db:query_pool(
        Pool,
        <<"SELECT version FROM schema_migrations WHERE version = $1">>,
        [Version]
    ),
    ?assertEqual(1, length(Rows)).

t_migration_failure_releases_lock() ->
    Pool = pool(),
    %% A transaction that fails should release the advisory lock
    try
        kura_db:transaction_pool(Pool, fun() ->
            kura_db:query_pool(
                Pool,
                <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
                [?LOCK_KEY]
            ),
            error(simulated_migration_failure)
        end)
    catch
        _:_ -> ok
    end,
    %% Another process should be able to acquire the lock immediately
    Start = erlang:monotonic_time(millisecond),
    kura_db:transaction_pool(Pool, fun() ->
        kura_db:query_pool(
            Pool,
            <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
            [?LOCK_KEY]
        ),
        ok
    end),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ?assert(Elapsed < 1000).

t_rollback_correctness() ->
    Pool = pool(),
    Version = 99990000000002,
    %% Clean slate
    kura_db:query_pool(Pool, <<"DELETE FROM schema_migrations WHERE version = $1">>, [Version]),
    kura_db:query_pool(Pool, <<"DROP TABLE IF EXISTS mig_stress_test CASCADE">>, []),
    %% Migrate up: create table + record version
    kura_db:transaction_pool(Pool, fun() ->
        kura_db:query_pool(
            Pool,
            <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
            [?LOCK_KEY]
        ),
        kura_db:query_pool(
            Pool,
            <<"CREATE TABLE mig_stress_test (id BIGSERIAL PRIMARY KEY, val TEXT)">>,
            []
        ),
        kura_db:query_pool(
            Pool,
            <<"INSERT INTO schema_migrations (version) VALUES ($1)">>,
            [Version]
        ),
        ok
    end),
    %% Insert data
    kura_db:query_pool(Pool, <<"INSERT INTO mig_stress_test (val) VALUES ('hello')">>, []),
    #{rows := [#{val := <<"hello">>}]} = kura_db:query_pool(
        Pool, <<"SELECT val FROM mig_stress_test">>, []
    ),
    %% Rollback: drop table + remove version
    kura_db:transaction_pool(Pool, fun() ->
        kura_db:query_pool(
            Pool,
            <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
            [?LOCK_KEY]
        ),
        kura_db:query_pool(Pool, <<"DROP TABLE mig_stress_test">>, []),
        kura_db:query_pool(
            Pool,
            <<"DELETE FROM schema_migrations WHERE version = $1">>,
            [Version]
        ),
        ok
    end),
    %% Verify table is gone
    case kura_db:query_pool(Pool, <<"SELECT 1 FROM mig_stress_test LIMIT 1">>, []) of
        {error, _} -> ok
    end,
    %% Migrate up again - should succeed cleanly
    kura_db:transaction_pool(Pool, fun() ->
        kura_db:query_pool(
            Pool,
            <<"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock">>,
            [?LOCK_KEY]
        ),
        kura_db:query_pool(
            Pool,
            <<"CREATE TABLE mig_stress_test (id BIGSERIAL PRIMARY KEY, val TEXT)">>,
            []
        ),
        kura_db:query_pool(
            Pool,
            <<"INSERT INTO schema_migrations (version) VALUES ($1)">>,
            [Version]
        ),
        ok
    end),
    %% Verify clean state (empty table)
    #{rows := EmptyRows} = kura_db:query_pool(
        Pool, <<"SELECT * FROM mig_stress_test">>, []
    ),
    ?assertEqual(0, length(EmptyRows)).
