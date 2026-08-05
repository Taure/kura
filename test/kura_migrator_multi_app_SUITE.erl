-module(kura_migrator_multi_app_SUITE).
-moduledoc """
End-to-end coverage for multi-application migrations against Postgres.

Six production hazards are pinned here:

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

4. `rollback/1,2` and `fake/1` choose a set of versions with no regard
   for which application shipped them. Across applications that turns
   into half-migrating several unrelated ones, or stamping a fresh
   extension's tables as created when they do not exist. Both refuse a
   multi-application repo now; `rollback/3` and `fake/2` name one.

5. The applied-version set was read *before* the advisory lock. Two
   nodes booting together both saw the same migration as pending; the
   loser then hit a duplicate key on `schema_migrations` and failed its
   whole batch. `applied_set_is_read_inside_the_advisory_lock/1`
   reproduces that race deterministically.

6. `schema_migrations` was created outside any lock and the statement's
   result was thrown away, so two nodes could race to create it and a
   failure to create it surfaced much later as a missing table.

`m20250101200000_ma_ext_b` deliberately sorts *between* the core
application's two versions, so any test that passes on global version
ordering would have caught nothing. It is also *lower* than
`m20250101300000_ma_core_c`, so a per-application rollback window that
was really a global version window could never select it.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    single_app_repo_behaviour_is_unchanged/1,
    extension_migrations_run_in_dependency_order/1,
    rollback_refuses_a_multi_application_repo/1,
    rollback_of_one_application_ignores_the_others/1,
    rollback_of_an_application_outside_the_set_is_named/1,
    fake_refuses_a_multi_application_repo/1,
    fake_of_one_application_leaves_the_others_pending/1,
    rollback_names_applied_versions_with_no_module/1,
    rollback_ignores_orphans_outside_the_window/1,
    duplicate_version_across_apps_blocks_migrate/1,
    applied_set_is_read_inside_the_advisory_lock/1,
    schema_migrations_bootstrap_waits_for_the_advisory_lock/1,
    schema_migrations_bootstrap_failure_is_returned/1
]).

-define(LIVE_POOL, kura_migrator_multi_app_suite_live).
-define(DENIED_POOL, kura_migrator_multi_app_suite_denied).
-define(REPO, kura_multi_app_repo).
-define(PLAIN_REPO, kura_single_app_repo).
-define(DENIED_REPO, kura_denied_app_repo).

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
        rollback_refuses_a_multi_application_repo,
        rollback_of_one_application_ignores_the_others,
        rollback_of_an_application_outside_the_set_is_named,
        fake_refuses_a_multi_application_repo,
        fake_of_one_application_leaves_the_others_pending,
        rollback_names_applied_versions_with_no_module,
        rollback_ignores_orphans_outside_the_window,
        duplicate_version_across_apps_blocks_migrate,
        applied_set_is_read_inside_the_advisory_lock,
        schema_migrations_bootstrap_waits_for_the_advisory_lock,
        schema_migrations_bootstrap_failure_is_returned
    ].

init_per_suite(Config) ->
    application:ensure_all_started(minato),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:ensure_all_started(kura),
    load_app(
        ?CORE,
        [
            ?REPO,
            ?PLAIN_REPO,
            ?DENIED_REPO,
            m20250101100000_ma_core_a,
            m20250101300000_ma_core_c
        ],
        []
    ),
    load_app(?EXT, [m20250101200000_ma_ext_b], [?CORE]),
    load_app(?CLASH, [m20250101300000_ma_clash_c], [?CORE]),
    application:set_env(?CORE, ?REPO, repo_config()),
    application:set_env(?CORE, ?PLAIN_REPO, repo_config()),
    application:set_env(?CORE, ?DENIED_REPO, denied_repo_config()),
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
    case kura_pool_minato:start_pool(?LIVE_POOL, pool_config()) of
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
    ?assertNot(table_exists(~"ma_ext_b")),

    %% One application is not a multi-application set, so neither
    %% refusal fires: rollback/2 still takes a version window and fake/1
    %% still stamps the lot.
    {ok, [?CORE_C]} = kura_migrator:migrate(?PLAIN_REPO),
    ?assertEqual({ok, [?CORE_C, ?CORE_A]}, kura_migrator:rollback(?PLAIN_REPO, 2)),
    ?assertEqual([], applied_versions()),
    ?assertEqual({ok, [?CORE_A, ?CORE_C]}, kura_migrator:fake(?PLAIN_REPO)),
    ?assertEqual([?CORE_A, ?CORE_C], applied_versions()),
    ?assertNot(table_exists(~"ma_core_a")).

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

rollback_refuses_a_multi_application_repo(_Config) ->
    %% "The last two" is a version window, and version is all
    %% `schema_migrations` records. Here it spans both applications:
    %% it would run ma_ext_b's down/0 and ma_core_c's, leaving the core
    %% application half-migrated and the extension entirely gone, in an
    %% order neither application asked for. The dependency graph cannot
    %% rescue it either - a dependency's migration can be the *newer*
    %% one, so the window can take a dependency out from under a
    %% dependent that stays applied.
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),

    ?assertEqual(
        {error, {ambiguous_rollback, [?CORE, ?EXT]}},
        kura_migrator:rollback(?REPO, 2)
    ),
    ?assertEqual(
        {error, {ambiguous_rollback, [?CORE, ?EXT]}},
        kura_migrator:rollback(?REPO)
    ),
    %% Refused, not attempted: every row and every table survives.
    ?assertEqual([?CORE_A, ?EXT_B, ?CORE_C], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assert(table_exists(~"ma_core_c")),
    ?assert(table_exists(~"ma_ext_b")).

rollback_of_one_application_ignores_the_others(_Config) ->
    %% rollback/3's window is the named application's applied versions,
    %% not the repo's. Both directions are checked because each fails a
    %% different way under a global version window:
    %%
    %%   - rolling the core back 2 would take ma_ext_b (20250101200000
    %%     sits between the core's two), not ma_core_a;
    %%   - rolling the extension back 1 would take ma_core_c, the
    %%     globally highest, and never touch the extension at all.
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),

    ?assertEqual({ok, [?CORE_C, ?CORE_A]}, kura_migrator:rollback(?REPO, ?CORE, 2)),
    ?assertEqual([?EXT_B], applied_versions()),
    ?assertNot(table_exists(~"ma_core_a")),
    ?assertNot(table_exists(~"ma_core_c")),
    ?assert(table_exists(~"ma_ext_b")),

    {ok, _} = kura_migrator:migrate(?REPO),
    ?assertEqual({ok, [?EXT_B]}, kura_migrator:rollback(?REPO, ?EXT, 1)),
    ?assertEqual([?CORE_A, ?CORE_C], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assert(table_exists(~"ma_core_c")),
    ?assertNot(table_exists(~"ma_ext_b")).

rollback_of_an_application_outside_the_set_is_named(_Config) ->
    %% A typo in the application name, or an extension the repo never
    %% declared. Rolling back "nothing, successfully" would read as the
    %% extension already being down.
    declare([?EXT]),
    {ok, _} = kura_migrator:migrate(?REPO),

    ?assertEqual(
        {error, {unknown_migration_app, ?CLASH, [?CORE, ?EXT]}},
        kura_migrator:rollback(?REPO, ?CLASH, 1)
    ),
    ?assertEqual([?CORE_A, ?EXT_B, ?CORE_C], applied_versions()).

fake_refuses_a_multi_application_repo(_Config) ->
    %% The brownfield baseline with an extension freshly added. fake/1
    %% stamps every pending migration, so it would record ma_ext_b as
    %% applied while ma_ext_b's table has never been created - and
    %% migrate/1 would then have nothing left to do, for good.
    declare([?EXT]),

    ?assertEqual(
        {error, {ambiguous_fake, [?CORE, ?EXT]}},
        kura_migrator:fake(?REPO)
    ),
    ?assertEqual([], applied_versions()).

fake_of_one_application_leaves_the_others_pending(_Config) ->
    %% What the operator in the case above actually wants: baseline the
    %% host, whose tables exist, and let migrate/1 create the freshly
    %% installed extension's for real.
    declare([?EXT]),

    ?assertEqual({ok, [?CORE_A, ?CORE_C]}, kura_migrator:fake(?REPO, ?CORE)),
    %% Stamped without DDL: rows recorded, no core tables.
    ?assertEqual([?CORE_A, ?CORE_C], applied_versions()),
    ?assertNot(table_exists(~"ma_core_a")),
    ?assertNot(table_exists(~"ma_core_c")),
    %% The extension stayed pending, so its table really does get built.
    ?assertEqual(
        [
            {?CORE_A, m20250101100000_ma_core_a, up},
            {?CORE_C, m20250101300000_ma_core_c, up},
            {?EXT_B, m20250101200000_ma_ext_b, pending}
        ],
        kura_migrator:status(?REPO)
    ),
    ?assertEqual({ok, [?EXT_B]}, kura_migrator:migrate(?REPO)),
    ?assert(table_exists(~"ma_ext_b")),

    ?assertEqual(
        {error, {unknown_migration_app, ?CLASH, [?CORE, ?EXT]}},
        kura_migrator:fake(?REPO, ?CLASH)
    ).

rollback_names_applied_versions_with_no_module(_Config) ->
    %% An applied version no module claims - a migration file deleted
    %% while its schema_migrations row remains. This used to be skipped,
    %% so rollback(Repo, 2) rolled back one migration and reported
    %% success. Now it refuses and names the version.
    %%
    %% Single-application repo: rollback/2 is the only form that has a
    %% version window wide enough to hit an unattributable row.
    {ok, _} = kura_migrator:migrate(?PLAIN_REPO),
    Orphan = 20259999000000,
    insert_version(Orphan),

    ?assertEqual(
        {error, {unknown_applied_versions, [Orphan]}},
        kura_migrator:rollback(?PLAIN_REPO, 2)
    ),
    %% Nothing rolled back: the transaction aborted before any DDL.
    ?assertEqual([?CORE_A, ?CORE_C, Orphan], applied_versions()),
    ?assert(table_exists(~"ma_core_a")),
    ?assert(table_exists(~"ma_core_c")).

rollback_ignores_orphans_outside_the_window(_Config) ->
    %% An orphan older than everything in the window cannot affect the
    %% order of the migrations being rolled back, so it must not break
    %% an otherwise healthy rollback. A consumer that deleted an ancient
    %% migration module keeps working.
    {ok, _} = kura_migrator:migrate(?PLAIN_REPO),
    Orphan = 20200101000000,
    insert_version(Orphan),

    ?assertEqual({ok, [?CORE_C]}, kura_migrator:rollback(?PLAIN_REPO, 1)),
    ?assertEqual([Orphan, ?CORE_A], applied_versions()).

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

schema_migrations_bootstrap_waits_for_the_advisory_lock(_Config) ->
    %% `CREATE TABLE IF NOT EXISTS` is not atomic against a concurrent
    %% creator: two sessions can both pass the existence check and the
    %% loser fails on pg_type's unique index. The bootstrap therefore
    %% has to hold the migration lock, which means it cannot return
    %% while another transaction holds it.
    %%
    %% Outside the lock this returns immediately and the assertion on
    %% elapsed time fails.
    HoldMs = 700,
    Holder = spawn_lock_only_holder(HoldMs),
    receive
        {Holder, locked} -> ok
    after 5000 -> ct:fail(lock_not_acquired)
    end,

    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual(ok, kura_migrator:ensure_schema_migrations(?PLAIN_REPO)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    receive
        {Holder, committed} -> ok
    after 5000 -> ct:fail(holder_did_not_commit)
    end,
    ?assert(Elapsed >= HoldMs div 2).

schema_migrations_bootstrap_failure_is_returned(_Config) ->
    %% The statement's result used to be discarded, so a database that
    %% would not let kura create schema_migrations produced `ok` here
    %% and `relation "schema_migrations" does not exist` from every
    %% query after it. The role has USAGE but not CREATE on `public`,
    %% which is the default for a non-owner role since Postgres 15.
    ensure_denied_role(),
    ensure_denied_pool(),
    _ = query(~"DROP TABLE IF EXISTS schema_migrations CASCADE", []),

    ?assertMatch(
        {error, {schema_migrations_failed, _}},
        kura_migrator:ensure_schema_migrations(?DENIED_REPO)
    ),
    ?assertNot(table_exists(~"schema_migrations")),

    %% And it propagates: migrate/1 reports the bootstrap failure rather
    %% than running on and failing on a missing table.
    ?assertMatch(
        {error, {schema_migrations_failed, _}},
        kura_migrator:migrate(?DENIED_REPO)
    ).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

ensure_denied_role() ->
    %% CREATE ROLE has no IF NOT EXISTS, and the role outlives the
    %% suite, so a duplicate is the normal case on a re-run.
    _ = query(~"CREATE ROLE kura_ma_nocreate LOGIN PASSWORD 'nocreate'", []),
    ok.

ensure_denied_pool() ->
    case kura_pool_minato:start_pool(?DENIED_POOL, denied_pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = poll_pool_ready(?DENIED_POOL, 5000).

spawn_lock_holder(Version, HoldMs) ->
    Parent = self(),
    spawn_link(fun() ->
        kura_driver_minato:transaction(
            kura_pool_minato, ?LIVE_POOL, fun() -> hold(Parent, Version, HoldMs) end, #{}
        ),
        Parent ! {self(), committed}
    end).

%% The bootstrap runs before schema_migrations exists, so this holder
%% takes the lock and nothing else.
spawn_lock_only_holder(HoldMs) ->
    Parent = self(),
    spawn_link(fun() ->
        kura_driver_minato:transaction(
            kura_pool_minato, ?LIVE_POOL, fun() -> hold(Parent, undefined, HoldMs) end, #{}
        ),
        Parent ! {self(), committed}
    end).

hold(Parent, Version, HoldMs) ->
    #{command := _} = tx_query(
        ~"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock", [?LOCK_KEY]
    ),
    case Version of
        undefined ->
            ok;
        _ ->
            #{command := _} = tx_query(
                ~"INSERT INTO schema_migrations (version) VALUES ($1)", [Version]
            )
    end,
    Parent ! {self(), locked},
    timer:sleep(HoldMs).

tx_query(SQL, Params) ->
    kura_driver_minato:query(kura_pool_minato, ?LIVE_POOL, SQL, Params, #{}).

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
        pool_module => kura_pool_minato,
        driver_module => kura_driver_minato,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 4
    }.

denied_repo_config() ->
    (repo_config())#{pool => ?DENIED_POOL, username => <<"kura_ma_nocreate">>}.

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

denied_pool_config() ->
    (pool_config())#{user => "kura_ma_nocreate", password => "nocreate", pool_size => 2}.

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
    kura_driver_minato:query(kura_pool_minato, ?LIVE_POOL, SQL, Params, #{}).

cleanup_db() ->
    [
        kura_driver_minato:query(
            kura_pool_minato, ?LIVE_POOL, <<"DROP TABLE IF EXISTS ", T/binary, " CASCADE">>, [], #{}
        )
     || T <- ?TABLES
    ],
    _ = kura_driver_minato:query(
        kura_pool_minato, ?LIVE_POOL, ~"DROP TABLE IF EXISTS schema_migrations CASCADE", [], #{}
    ),
    ok.

poll_pool_ready(Pool, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    poll_pool_loop(Pool, Deadline).

poll_pool_loop(Pool, Deadline) ->
    case kura_driver_minato:query(kura_pool_minato, Pool, ~"SELECT 1", [], #{}) of
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
