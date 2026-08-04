-module(kura_migrator_multi_app_SUITE).
-moduledoc """
End-to-end coverage for multi-application migrations against Postgres.

Four production hazards are pinned here:

1. An extension shipping its own migrations as a separate OTP
   application never had them run. `migration_apps/0` fixes that, and
   the order comes from the OTP `applications` graph so a dependency's
   migrations land before its dependents'.

2. `schema_migrations` keys on version alone. Two applications shipping
   the same version silently produced a duplicate-key failure halfway
   through a batch; discovery now refuses up front and names both
   claimants.

3. `rollback/2` silently dropped applied versions no module claimed, so
   it could roll back fewer migrations than asked and still report
   success - while running the surviving `down/0`s across the gap.

4. The applied-version set was read *before* the advisory lock. Two
   nodes booting together both saw the same migration as pending; the
   loser then hit a duplicate key on `schema_migrations` and failed its
   whole batch. `applied_set_is_read_inside_the_advisory_lock/1`
   reproduces that race deterministically.

`m20250101200000_ma_ext_b` deliberately sorts *between* the core
application's two versions, so any test that passes on global version
ordering would have caught nothing.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    single_app_repo_behaviour_is_unchanged/1,
    extension_migrations_run_in_dependency_order/1,
    rollback_runs_in_reverse_apply_order/1,
    rollback_names_applied_versions_with_no_module/1,
    rollback_ignores_orphans_outside_the_window/1,
    duplicate_version_across_apps_blocks_migrate/1,
    applied_set_is_read_inside_the_advisory_lock/1
]).

-define(LIVE_POOL, kura_migrator_multi_app_suite_live).
-define(REPO, kura_multi_app_repo).
-define(PLAIN_REPO, kura_single_app_repo).

-define(CORE, kura_ma_core).
-define(EXT, kura_ma_ext).
-define(CLASH, kura_ma_clash).

-define(CORE_A, 20250101100000).
-define(EXT_B, 20250101200000).
-define(CORE_C, 20250101300000).

%% Mirrors ?MIGRATION_LOCK_KEY in kura_migrator. Duplicated deliberately:
%% the constant is part of the cross-node contract, so a change to it
%% must break this test.
-define(LOCK_KEY, 571629482).

-define(TABLES, [~"ma_core_a", ~"ma_core_c", ~"ma_ext_b", ~"ma_clash_c"]).

all() ->
    [
        single_app_repo_behaviour_is_unchanged,
        extension_migrations_run_in_dependency_order,
        rollback_runs_in_reverse_apply_order,
        rollback_names_applied_versions_with_no_module,
        rollback_ignores_orphans_outside_the_window,
        duplicate_version_across_apps_blocks_migrate,
        applied_set_is_read_inside_the_advisory_lock
    ].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:ensure_all_started(kura),
    load_app(?CORE, [?REPO, ?PLAIN_REPO, m20250101100000_ma_core_a, m20250101300000_ma_core_c], []),
    load_app(?EXT, [m20250101200000_ma_ext_b], [?CORE]),
    load_app(?CLASH, [m20250101300000_ma_clash_c], [?CORE]),
    application:set_env(?CORE, ?REPO, repo_config()),
    application:set_env(?CORE, ?PLAIN_REPO, repo_config()),
    application:set_env(kura, ensure_database, false),
    Config.

end_per_suite(_Config) ->
    application:unset_env(kura, ensure_database),
    application:unset_env(kura, kura_ma_declared_apps),
    application:unload(?CLASH),
    application:unload(?EXT),
    application:unload(?CORE),
    ok.

init_per_testcase(_TC, Config) ->
    case pgo_sup:start_child(?LIVE_POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = poll_pool_ready(?LIVE_POOL, 5000),
    declare([]),
    cleanup_db(),
    Config.

end_per_testcase(_TC, _Config) ->
    declare([]),
    cleanup_db(),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

single_app_repo_behaviour_is_unchanged(_Config) ->
    %% The shape every consumer ships today: one repo, one application,
    %% no migration_apps/0. Versions ascending on the way up, the
    %% highest version first on the way down, status in version order.
    %% Multi-application discovery must not perturb any of it.
    ?assertEqual({ok, [?CORE_A, ?CORE_C]}, kura_migrator:migrate(?PLAIN_REPO)),
    ?assertEqual(
        [
            {?CORE_A, m20250101100000_ma_core_a, up},
            {?CORE_C, m20250101300000_ma_core_c, up}
        ],
        kura_migrator:status(?PLAIN_REPO)
    ),
    ?assertEqual([?CORE_A, ?CORE_C], applied_versions()),
    ?assertEqual({ok, []}, kura_migrator:migrate(?PLAIN_REPO)),

    ?assertEqual({ok, [?CORE_C]}, kura_migrator:rollback(?PLAIN_REPO)),
    ?assertEqual([?CORE_A], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assertNot(table_exists(~"ma_core_c")),
    %% The extension's application is loaded but undeclared, so none of
    %% its migrations are visible to this repo.
    ?assertNot(table_exists(~"ma_ext_b")).

extension_migrations_run_in_dependency_order(_Config) ->
    %% ma_ext_b's version (20250101200000) sits between the core's two.
    %% Ordering by version would interleave it; the OTP applications
    %% graph says kura_ma_ext depends on kura_ma_core, so both of the
    %% core's migrations must run first.
    declare([?EXT]),
    ?assertEqual({ok, [?CORE_A, ?CORE_C, ?EXT_B]}, kura_migrator:migrate(?REPO)),
    ?assertEqual(
        [
            {?CORE_A, m20250101100000_ma_core_a, up},
            {?CORE_C, m20250101300000_ma_core_c, up},
            {?EXT_B, m20250101200000_ma_ext_b, up}
        ],
        kura_migrator:status(?REPO)
    ),
    ?assert(table_exists(~"ma_core_a")),
    ?assert(table_exists(~"ma_core_c")),
    ?assert(table_exists(~"ma_ext_b")).

rollback_runs_in_reverse_apply_order(_Config) ->
    %% The window is the two highest applied versions - all
    %% `schema_migrations` can offer, since it records versions only.
    %% Within the window, execution reverses the *apply* order, so the
    %% dependent application's down/0 runs before the dependency's.
    %% Ordering the window by version would give [?CORE_C, ?EXT_B].
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),
    ?assertEqual({ok, [?EXT_B, ?CORE_C]}, kura_migrator:rollback(?REPO, 2)),
    ?assertEqual([?CORE_A], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assertNot(table_exists(~"ma_core_c")),
    ?assertNot(table_exists(~"ma_ext_b")).

rollback_names_applied_versions_with_no_module(_Config) ->
    %% An applied version no module claims - the extension dropped out
    %% of migration_apps/0, or the migration file was deleted. This used
    %% to be skipped, so rollback(?REPO, 2) rolled back one migration
    %% and reported success. Now it refuses and names the version.
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),
    Orphan = 20259999000000,
    insert_version(Orphan),

    ?assertEqual(
        {error, {unknown_applied_versions, [Orphan]}},
        kura_migrator:rollback(?REPO, 2)
    ),
    %% Nothing rolled back: the transaction aborted before any DDL.
    ?assertEqual([?CORE_A, ?EXT_B, ?CORE_C, Orphan], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assert(table_exists(~"ma_core_c")),
    ?assert(table_exists(~"ma_ext_b")).

rollback_ignores_orphans_outside_the_window(_Config) ->
    %% An orphan older than everything in the window cannot affect the
    %% order of the migrations being rolled back, so it must not break
    %% an otherwise healthy rollback. A consumer that deleted an ancient
    %% migration module keeps working.
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),
    Orphan = 20200101000000,
    insert_version(Orphan),

    ?assertEqual({ok, [?CORE_C]}, kura_migrator:rollback(?REPO, 1)),
    ?assertEqual([Orphan, ?CORE_A, ?EXT_B], applied_versions()).

duplicate_version_across_apps_blocks_migrate(_Config) ->
    %% m20250101300000_ma_clash_c shares a version with
    %% m20250101300000_ma_core_c. `schema_migrations` keys on version
    %% alone, so one of the two would have been recorded as the other.
    %% Discovery refuses before any DDL runs and names both claimants.
    declare([?CLASH]),
    ?assertEqual(
        {error,
            {duplicate_migration_version, [
                {?CORE_C, [
                    {?CORE, m20250101300000_ma_core_c},
                    {?CLASH, m20250101300000_ma_clash_c}
                ]}
            ]}},
        kura_migrator:migrate(?REPO)
    ),
    ?assertEqual([], applied_versions()),
    ?assertNot(table_exists(~"ma_core_a")),
    ?assertNot(table_exists(~"ma_core_c")),
    ?assertNot(table_exists(~"ma_clash_c")).

applied_set_is_read_inside_the_advisory_lock(_Config) ->
    %% The TOCTOU. Another node holds the migration advisory lock and,
    %% inside that same transaction, records ?CORE_A as applied. Our
    %% migrate/1 blocks on the lock and must observe the row once the
    %% holder commits - applying only ?CORE_C.
    %%
    %% Reading the applied set before taking the lock made migrate/1 see
    %% an empty table, try to apply ?CORE_A too, and fail the whole
    %% batch on the schema_migrations primary key.
    kura_migrator:ensure_schema_migrations(?PLAIN_REPO),
    Holder = spawn_lock_holder(?CORE_A, 700),
    receive
        {Holder, locked} -> ok
    after 5000 -> ct:fail(lock_not_acquired)
    end,

    ?assertEqual({ok, [?CORE_C]}, kura_migrator:migrate(?PLAIN_REPO)),
    ?assertEqual([?CORE_A, ?CORE_C], applied_versions()),
    receive
        {Holder, committed} -> ok
    after 5000 -> ct:fail(holder_did_not_commit)
    end.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

spawn_lock_holder(Version, HoldMs) ->
    Parent = self(),
    spawn_link(fun() ->
        pgo:transaction(?LIVE_POOL, fun() -> hold(Parent, Version, HoldMs) end, #{}),
        Parent ! {self(), committed}
    end).

hold(Parent, Version, HoldMs) ->
    #{command := _} = tx_query(
        ~"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock", [?LOCK_KEY]
    ),
    #{command := _} = tx_query(
        ~"INSERT INTO schema_migrations (version) VALUES ($1)", [Version]
    ),
    Parent ! {self(), locked},
    timer:sleep(HoldMs).

tx_query(SQL, Params) ->
    pgo:query(SQL, Params, #{pool => ?LIVE_POOL}).

declare(Apps) ->
    application:set_env(kura, kura_ma_declared_apps, Apps).

load_app(Name, Modules, Deps) ->
    Spec =
        {application, Name, [
            {description, "kura multi-app migration suite"},
            {vsn, "0.0.1"},
            {modules, Modules},
            {registered, []},
            {applications, [kernel, stdlib | Deps]}
        ]},
    case application:load(Spec) of
        ok -> ok;
        {error, {already_loaded, Name}} -> ok
    end.

repo_config() ->
    #{
        pool => ?LIVE_POOL,
        pool_module => kura_pool_pgo,
        driver_module => kura_driver_pgo,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 4
    }.

pool_config() ->
    #{
        host => "localhost",
        port => 5555,
        database => "kura_test",
        user => "postgres",
        password => "root",
        pool_size => 4,
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    }.

applied_versions() ->
    #{rows := Rows} = query(~"SELECT version FROM schema_migrations ORDER BY version", []),
    [V || #{version := V} <- Rows].

insert_version(Version) ->
    #{command := _} = query(~"INSERT INTO schema_migrations (version) VALUES ($1)", [Version]),
    ok.

table_exists(Name) ->
    #{rows := [#{count := N}]} = query(
        ~"SELECT count(*)::int AS count FROM information_schema.tables WHERE table_name = $1",
        [Name]
    ),
    N > 0.

query(SQL, Params) ->
    pgo:query(SQL, Params, #{
        pool => ?LIVE_POOL, decode_opts => [return_rows_as_maps, column_name_as_atom]
    }).

cleanup_db() ->
    [
        pgo:query(<<"DROP TABLE IF EXISTS ", T/binary, " CASCADE">>, [], #{pool => ?LIVE_POOL})
     || T <- ?TABLES
    ],
    _ = pgo:query(~"DROP TABLE IF EXISTS schema_migrations CASCADE", [], #{pool => ?LIVE_POOL}),
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
