-module(kura_migrator_nontx_tests).
-moduledoc """
Non-transactional migrations against a real Postgres (Taure/kura#139).

Each scenario is a fake application whose migrations `kura_nontx_repo`
picks up through `migration_apps/0`. `CREATE INDEX CONCURRENTLY` refuses
to run inside a transaction block, so the happy path proves the
migration ran outside one; the index migration also divides by zero
unless its own session holds the migration advisory lock.
""".

-include_lib("eunit/include/eunit.hrl").

-define(REPO, kura_nontx_repo).
-define(POOL, kura_migrator_nontx_tests_pool).
-define(BASE, kura_nontx_base).
-define(OK_APP, kura_nontx_ok).
-define(FAIL_APP, kura_nontx_fail).
-define(REFUSE_APP, kura_nontx_refuse).
-define(RAISE_APP, kura_nontx_raise).
-define(SIDE_APP, kura_nontx_side).
-define(NONTX_DUP_APP, kura_nontx_dup).
-define(TX_DUP_APP, kura_tx_dup).
-define(LOCK_TIMEOUT_POOL, kura_migrator_nontx_tests_lock_timeout_pool).

-define(LOCK_KEY, 571629482).

-define(CREATE, 20250301100000).
-define(INDEX, 20250301110000).
-define(NOTE, 20250301120000).
-define(FAIL_ITEMS, 20250302100000).
-define(FAIL_PARTIAL, 20250302110000).
-define(FAIL_AFTER, 20250302120000).
-define(REFUSE_INDEX, 20250303110000).
-define(RAISE, 20250304100000).
-define(SIDE, 20250305100000).
-define(NONTX_DUP, 20250306100000).
-define(TX_DUP, 20250306110000).

-define(TABLES, [
    ~"nontx_items",
    ~"nontx_fail_items",
    ~"nontx_fail_after",
    ~"nontx_refuse_items",
    ~"nontx_side_effects",
    ~"nontx_dups",
    ~"tx_dups"
]).

nontx_test_() ->
    {setup, fun setup/0, fun cleanup/1,
        {foreach, fun reset/0, fun(_) -> reset() end, [
            {"CREATE INDEX CONCURRENTLY runs through a non-transactional migration",
                fun concurrent_index_runs_outside_a_transaction/0},
            {"rollback runs a non-transactional down/0 outside a transaction",
                fun rollback_runs_down_outside_a_transaction/0},
            {"a failure part-way keeps earlier statements and leaves the version pending",
                fun failure_part_way_leaves_version_unrecorded/0},
            {"a split run that fails reports what it committed",
                fun failed_split_run_reports_committed_versions/0},
            {"concurrently in a transactional migration is refused before anything runs",
                fun concurrently_in_transactional_migration_is_refused/0},
            {"a non-transactional up/0 that raises stops the run while planning",
                fun raising_up_stops_the_run_while_planning/0},
            {"a non-transactional step skips what another node did meanwhile",
                fun non_transactional_step_rereads_under_the_lock/0},
            {"a pool without advisory_locks runs the step without the lock",
                fun runs_without_a_lock_on_a_lockless_pool/0},
            {"two migrators run each migration once", {timeout, 30, fun concurrent_migrators/0}},
            {"a non-transactional step waits for the lock another session holds",
                {timeout, 30, fun waits_for_a_held_lock/0}},
            {"migration_lock_timeout ends a wait for a held lock",
                {timeout, 30, fun lock_wait_times_out/0}},
            {"migration_lock_timeout ends a wait for the session lock",
                {timeout, 30, fun session_lock_wait_times_out/0}},
            {"an unreadable schema_migrations runs nothing again",
                {timeout, 30, fun unreadable_schema_migrations_runs_nothing/0}},
            {"a busy lock is tried once inside a transaction or the sandbox",
                {timeout, 10, fun busy_lock_is_tried_once_when_enclosed/0}},
            {"a failed unique build keeps duplicated values out of the error",
                fun duplicate_values_stay_out_of_the_error/0},
            {"a non-transactional plan is refused inside the sandbox",
                {timeout, 10, fun refused_in_sandbox/0}},
            {"a non-transactional plan is refused inside an outer transaction",
                {timeout, 10, fun refused_in_outer_transaction/0}},
            {"a raising unlock is logged, not raised", fun raising_unlock_is_kept_quiet/0},
            {"only an unlock answering true releases", fun unlock_answer_decides_release/0},
            {"query_on runs on the given connection", fun query_on_uses_the_connection/0},
            {"query_on/5 hands its options to the driver", fun query_on_passes_options/0}
        ]}}.

setup() ->
    {ok, _} = application:ensure_all_started(minato),
    application:set_env(kura, dialect, kura_dialect_pg),
    {ok, _} = application:ensure_all_started(kura),
    application:set_env(kura, ensure_database, false),
    application:set_env(kura, migration_lock_retry_interval, 20),
    load_app(?BASE, [?REPO]),
    load_app(?OK_APP, [
        m20250301100000_nontx_create_items,
        m20250301110000_nontx_index_items,
        m20250301120000_nontx_add_note
    ]),
    load_app(?FAIL_APP, [
        m20250302100000_nontx_fail_items,
        m20250302110000_nontx_fail_partial,
        m20250302120000_nontx_fail_after
    ]),
    load_app(?REFUSE_APP, [
        m20250303100000_nontx_refuse_items,
        m20250303110000_nontx_refuse_index
    ]),
    load_app(?RAISE_APP, [m20250304100000_nontx_raise]),
    load_app(?SIDE_APP, [m20250305100000_nontx_side_effect]),
    load_app(?NONTX_DUP_APP, [m20250306100000_nontx_dup_unique]),
    load_app(?TX_DUP_APP, [m20250306110000_tx_dup_unique]),
    application:set_env(?BASE, ?REPO, repo_config()),
    case kura_pool_minato:start_pool(?POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = kura_migrator:wait_for_pool(?REPO, 5000),
    ok.

cleanup(_) ->
    reset(),
    application:unset_env(kura, ensure_database),
    application:unset_env(kura, migration_lock_retry_interval),
    application:unset_env(kura, kura_nontx_apps),
    [
        application:unload(A)
     || A <- [
            ?OK_APP,
            ?FAIL_APP,
            ?REFUSE_APP,
            ?RAISE_APP,
            ?SIDE_APP,
            ?NONTX_DUP_APP,
            ?TX_DUP_APP,
            ?BASE
        ]
    ],
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

concurrent_index_runs_outside_a_transaction() ->
    declare([?OK_APP]),
    ?assertEqual({ok, [?CREATE, ?INDEX, ?NOTE]}, kura_migrator:migrate(?REPO)),
    ?assertEqual([?CREATE, ?INDEX, ?NOTE], applied_versions()),
    ?assertEqual([true], index_valid(~"nontx_items_name_index")),
    ?assert(column_exists(~"nontx_items", ~"note")),
    ?assertEqual({ok, []}, kura_migrator:migrate(?REPO)),
    ?assertEqual(false, lock_held()).

rollback_runs_down_outside_a_transaction() ->
    declare([?OK_APP]),
    {ok, _} = kura_migrator:migrate(?REPO),
    ?assertEqual({ok, [?NOTE, ?INDEX]}, kura_migrator:rollback(?REPO, ?OK_APP, 2)),
    ?assertEqual([?CREATE], applied_versions()),
    ?assertEqual([], index_valid(~"nontx_items_name_index")),
    ?assertNot(column_exists(~"nontx_items", ~"note")),
    ?assertEqual({ok, [?CREATE]}, kura_migrator:rollback(?REPO, ?OK_APP, 1)),
    ?assertEqual([], applied_versions()),
    ?assertEqual(false, lock_held()).

failure_part_way_leaves_version_unrecorded() ->
    declare([?FAIL_APP]),
    ?assertMatch(
        {error, {migration_failed, ?FAIL_PARTIAL, _}}, kura_migrator:migrate(?REPO)
    ),
    ?assertEqual([?FAIL_ITEMS], applied_versions()),
    ?assertEqual([true], index_valid(~"nontx_fail_items_name_index")),
    ?assertNot(table_exists(~"nontx_fail_after")),
    ?assertEqual(
        [
            {?FAIL_ITEMS, m20250302100000_nontx_fail_items, up},
            {?FAIL_PARTIAL, m20250302110000_nontx_fail_partial, pending},
            {?FAIL_AFTER, m20250302120000_nontx_fail_after, pending}
        ],
        kura_migrator:status(?REPO)
    ),
    ?assertEqual(false, lock_held()).

failed_split_run_reports_committed_versions() ->
    declare([?FAIL_APP]),
    Stop = [kura, migrator, migrate, stop],
    Ref = telemetry_test:attach_event_handlers(self(), [Stop]),
    try
        {error, {migration_failed, ?FAIL_PARTIAL, _}} = kura_migrator:migrate(?REPO),
        receive
            {Stop, Ref, _Measurements, Metadata} ->
                ?assertMatch(
                    #{result := error, applied_count := 1, applied_versions := [?FAIL_ITEMS]},
                    Metadata
                )
        after 1000 -> error(no_stop_event)
        end
    after
        telemetry:detach(Ref)
    end.

concurrently_in_transactional_migration_is_refused() ->
    declare([?REFUSE_APP]),
    ?assertEqual(
        {error, {concurrently_in_transaction, ?REFUSE_INDEX, m20250303110000_nontx_refuse_index}},
        kura_migrator:migrate(?REPO)
    ),
    ?assertEqual([], applied_versions()),
    ?assertNot(table_exists(~"nontx_refuse_items")).

raising_up_stops_the_run_while_planning() ->
    declare([?RAISE_APP]),
    ?assertEqual({error, nontx_test_boom}, kura_migrator:migrate(?REPO)),
    ?assertEqual(
        [{?RAISE, m20250304100000_nontx_raise, pending}], kura_migrator:status(?REPO)
    ),
    ?assertEqual(false, lock_held()).

non_transactional_step_rereads_under_the_lock() ->
    Side = {?SIDE, m20250305100000_nontx_side_effect},
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    _ = query(~"INSERT INTO schema_migrations (version) VALUES ($1)", [?SIDE]),
    ?assertEqual({ok, []}, kura_migrator:run_non_transactional(Side, up, ?REPO)),
    ?assertNot(table_exists(~"nontx_side_effects")),
    _ = query(~"DELETE FROM schema_migrations WHERE version = $1", [?SIDE]),
    _ = query(~"CREATE TABLE nontx_side_effects (id serial PRIMARY KEY)", []),
    ?assertEqual({ok, []}, kura_migrator:run_non_transactional(Side, down, ?REPO)),
    ?assert(table_exists(~"nontx_side_effects")),
    ?assertEqual(false, lock_held()).

runs_without_a_lock_on_a_lockless_pool() ->
    Side = {?SIDE, m20250305100000_nontx_side_effect},
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    application:set_env(?BASE, ?REPO, (repo_config())#{pool_module => kura_nontx_nolock_pool}),
    try
        ?assertEqual({ok, [?SIDE]}, kura_migrator:run_non_transactional(Side, up, ?REPO))
    after
        application:set_env(?BASE, ?REPO, repo_config())
    end,
    ?assertEqual(1, side_effects()),
    ?assertEqual([?SIDE], applied_versions()).

concurrent_migrators() ->
    declare([?OK_APP]),
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    Parent = self(),
    Pids = [
        spawn_link(fun() -> Parent ! {self(), kura_migrator:migrate(?REPO)} end)
     || _ <- [1, 2]
    ],
    Results = [
        receive
            {Pid, Result} -> Result
        after 20000 -> error(migrator_timeout)
        end
     || Pid <- Pids
    ],
    Applied = lists:append([Versions || {ok, Versions} <- Results]),
    ?assertEqual(2, length([ok || {ok, _} <- Results])),
    ?assertEqual([?CREATE, ?INDEX, ?NOTE], lists:sort(Applied)),
    ?assertEqual([?CREATE, ?INDEX, ?NOTE], applied_versions()).

waits_for_a_held_lock() ->
    declare([?OK_APP]),
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    try
        #{rows := [#{locked := true}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_try_advisory_lock($1) AS locked", [?LOCK_KEY], decode()
        ),
        Parent = self(),
        Pid = spawn_link(fun() -> Parent ! {self(), kura_migrator:migrate(?REPO)} end),
        receive
            {Pid, Early} -> error({migrated_while_locked, Early})
        after 400 -> ok
        end,
        ?assertEqual([], applied_versions()),
        _ = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_advisory_unlock($1)", [?LOCK_KEY], decode()
        ),
        receive
            {Pid, Result} -> ?assertEqual({ok, [?CREATE, ?INDEX, ?NOTE]}, Result)
        after 10000 -> error(migrator_timeout)
        end
    after
        kura_pool_minato:checkin(?POOL, Token)
    end.

lock_wait_times_out() ->
    declare([?OK_APP]),
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    application:set_env(kura, migration_lock_timeout, 200),
    try
        #{rows := [#{locked := true}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_try_advisory_lock($1) AS locked", [?LOCK_KEY], decode()
        ),
        ?assertEqual({error, migration_lock_timeout}, kura_migrator:migrate(?REPO)),
        ?assertEqual(
            {error, migration_lock_timeout}, kura_migrator:ensure_schema_migrations(?REPO)
        ),
        ?assertEqual([], applied_versions())
    after
        application:unset_env(kura, migration_lock_timeout),
        _ = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_advisory_unlock($1)", [?LOCK_KEY], decode()
        ),
        kura_pool_minato:checkin(?POOL, Token)
    end.

session_lock_wait_times_out() ->
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    application:set_env(kura, migration_lock_timeout, 200),
    try
        #{rows := [#{locked := true}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_try_advisory_lock($1) AS locked", [?LOCK_KEY], decode()
        ),
        ?assertEqual(
            {error, migration_lock_timeout},
            kura_migrator:run_non_transactional(
                {?INDEX, m20250301110000_nontx_index_items}, up, ?REPO
            )
        ),
        ?assertEqual([], applied_versions())
    after
        application:unset_env(kura, migration_lock_timeout),
        _ = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_advisory_unlock($1)", [?LOCK_KEY], decode()
        ),
        kura_pool_minato:checkin(?POOL, Token)
    end,
    ?assertEqual(false, lock_held()).

unreadable_schema_migrations_runs_nothing() ->
    declare([?SIDE_APP]),
    ?assertEqual({ok, [?SIDE]}, kura_migrator:migrate(?REPO)),
    ?assertEqual(1, side_effects()),
    {ok, _} = minato:start_pool(?LOCK_TIMEOUT_POOL, #{
        size => 2,
        connection => #{
            host => "localhost",
            port => 5555,
            database => ~"kura_test",
            user => ~"postgres",
            password => ~"root",
            parameters => #{~"lock_timeout" => ~"200"}
        }
    }),
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    application:set_env(?BASE, ?REPO, (repo_config())#{pool => ?LOCK_TIMEOUT_POOL}),
    try
        _ = kura_driver_minato:query_on(Conn, ~"BEGIN", [], decode()),
        #{command := lock} = kura_driver_minato:query_on(
            Conn, ~"LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE", [], decode()
        ),
        ?assertMatch(
            {error, {schema_migrations_unreadable, _}},
            kura_migrator:run_non_transactional(
                {?SIDE, m20250305100000_nontx_side_effect}, up, ?REPO
            )
        ),
        ?assertMatch(
            {error, {schema_migrations_unreadable, _}},
            kura_migrator:run_non_transactional(
                {?SIDE, m20250305100000_nontx_side_effect}, down, ?REPO
            )
        )
    after
        application:set_env(?BASE, ?REPO, repo_config()),
        _ = kura_driver_minato:query_on(Conn, ~"ROLLBACK", [], decode()),
        kura_pool_minato:checkin(?POOL, Token),
        ok = minato:stop_pool(?LOCK_TIMEOUT_POOL)
    end,
    ?assertEqual(1, side_effects()),
    ?assertEqual([?SIDE], applied_versions()).

side_effects() ->
    #{rows := [#{n := N}]} = query(~"SELECT count(*)::int AS n FROM nontx_side_effects", []),
    N.

busy_lock_is_tried_once_when_enclosed() ->
    declare([?OK_APP]),
    ok = kura_migrator:ensure_schema_migrations(?REPO),
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    try
        #{rows := [#{locked := true}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_try_advisory_lock($1) AS locked", [?LOCK_KEY], decode()
        ),
        ok = kura_sandbox:checkout(?REPO),
        try
            ?assertEqual(
                {error, migration_lock_busy}, kura_migrator:ensure_schema_migrations(?REPO)
            ),
            ?assertEqual({error, migration_lock_busy}, kura_migrator:migrate(?REPO))
        after
            kura_sandbox:checkin(?REPO)
        end,
        ?assertEqual(
            {error, migration_lock_busy},
            kura_db:transaction(?REPO, fun() -> kura_migrator:migrate(?REPO) end)
        ),
        ?assertEqual([], applied_versions())
    after
        _ = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_advisory_unlock($1)", [?LOCK_KEY], decode()
        ),
        kura_pool_minato:checkin(?POOL, Token)
    end.

duplicate_values_stay_out_of_the_error() ->
    declare([?NONTX_DUP_APP]),
    {error, {migration_failed, ?NONTX_DUP, {pgsql_error, NonTx}}} = kura_migrator:migrate(?REPO),
    ?assertMatch(#{code := ~"23505", message := _}, NonTx),
    ?assertNot(is_map_key(detail, NonTx)),
    ?assertEqual(nomatch, binary:match(term_to_binary(NonTx), ~"dup@example.se")),
    reset(),
    declare([?TX_DUP_APP]),
    {error, {migration_failed, ?TX_DUP, {pgsql_error, Tx}}} = kura_migrator:migrate(?REPO),
    ?assertMatch(#{code := ~"23505", message := _}, Tx),
    ?assertNot(is_map_key(detail, Tx)),
    ?assertEqual(nomatch, binary:match(term_to_binary(Tx), ~"dup@example.se")).

refused_in_sandbox() ->
    declare([?OK_APP]),
    ok = kura_sandbox:checkout(?REPO),
    Result =
        try
            kura_migrator:migrate(?REPO)
        after
            kura_sandbox:checkin(?REPO)
        end,
    ?assertEqual(
        {error, {non_transactional_in_transaction, ?INDEX, m20250301110000_nontx_index_items}},
        Result
    ),
    ?assertEqual([], applied_versions()),
    ?assertNot(table_exists(~"nontx_items")),
    ?assertEqual(false, lock_held()).

refused_in_outer_transaction() ->
    declare([?OK_APP]),
    Result = kura_db:transaction(?REPO, fun() -> kura_migrator:migrate(?REPO) end),
    ?assertEqual(
        {error, {non_transactional_in_transaction, ?INDEX, m20250301110000_nontx_index_items}},
        Result
    ),
    ?assertEqual([], applied_versions()),
    ?assertNot(table_exists(~"nontx_items")),
    ?assertEqual(false, lock_held()).

raising_unlock_is_kept_quiet() ->
    ?assertError(
        function_clause, kura_db:query_on(?REPO, not_a_connection, ~"SELECT 1", [])
    ),
    ?assertEqual(
        not_released, kura_migrator:release_session_lock({conn, ?REPO, not_a_connection})
    ).

unlock_answer_decides_release() ->
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    Target = {conn, ?REPO, Conn},
    try
        ?assertEqual(not_released, kura_migrator:release_session_lock(Target)),
        #{rows := [#{locked := true}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_try_advisory_lock($1) AS locked", [?LOCK_KEY], decode()
        ),
        ?assertEqual(released, kura_migrator:release_session_lock(Target)),
        ?assertEqual(false, lock_held())
    after
        kura_pool_minato:checkin(?POOL, Token)
    end.

query_on_uses_the_connection() ->
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    try
        #{rows := [#{pid := Pid}]} = kura_db:query_on(
            ?REPO, Conn, ~"SELECT pg_backend_pid() AS pid", []
        ),
        #{rows := [#{pid := Same}]} = kura_driver_minato:query_on(
            Conn, ~"SELECT pg_backend_pid() AS pid", [], decode()
        ),
        ?assertEqual(Pid, Same),
        ?assertMatch({error, _}, kura_db:query_on(?REPO, Conn, ~"SELECT * FROM nontx_none", []))
    after
        kura_pool_minato:checkin(?POOL, Token)
    end.

query_on_passes_options() ->
    {ok, Conn, Token} = kura_pool_minato:checkout(?POOL, #{}),
    try
        ?assertMatch(
            #{rows := [#{one := 1}]},
            kura_db:query_on(?REPO, Conn, ~"SELECT 1 AS one", [], #{timeout => 5000})
        ),
        ?assertMatch(
            {error, _},
            kura_db:query_on(?REPO, Conn, ~"SELECT pg_sleep(2)", [], #{timeout => 50})
        )
    after
        kura_pool_minato:checkin(?POOL, Token)
    end.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

declare(Apps) ->
    application:set_env(kura, kura_nontx_apps, Apps).

load_app(Name, Modules) ->
    Spec =
        {application, Name, [
            {description, "kura non-transactional migration tests"},
            {vsn, "0.0.1"},
            {modules, Modules},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    case application:load(Spec) of
        ok -> ok;
        {error, {already_loaded, Name}} -> ok
    end.

reset() ->
    declare([]),
    [query(iolist_to_binary([~"DROP TABLE IF EXISTS ", T, ~" CASCADE"]), []) || T <- ?TABLES],
    _ = query(~"DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    ok.

applied_versions() ->
    case query(~"SELECT version FROM schema_migrations ORDER BY version", []) of
        #{rows := Rows} -> [V || #{version := V} <- Rows];
        {error, _} -> []
    end.

index_valid(Name) ->
    #{rows := Rows} = query(
        ~"""
        SELECT i.indisvalid AS valid FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1
        """,
        [Name]
    ),
    [V || #{valid := V} <- Rows].

table_exists(Name) ->
    #{rows := [#{exists := E}]} = query(
        ~"SELECT to_regclass($1) IS NOT NULL AS exists", [Name]
    ),
    E.

column_exists(Table, Column) ->
    #{rows := Rows} = query(
        ~"SELECT 1 AS one FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
        [Table, Column]
    ),
    Rows =/= [].

lock_held() ->
    #{rows := [#{held := Held}]} = query(
        ~"""
        SELECT count(*) > 0 AS held FROM pg_locks
        WHERE locktype = 'advisory' AND objid::bigint = $1
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = 0 AND objsubid = 1
        """,
        [?LOCK_KEY]
    ),
    Held.

query(SQL, Params) ->
    kura_driver_minato:query(kura_pool_minato, ?POOL, SQL, Params, decode()).

decode() ->
    #{decode_opts => [return_rows_as_maps, column_name_as_atom]}.

repo_config() ->
    #{
        pool => ?POOL,
        pool_module => kura_pool_minato,
        driver_module => kura_driver_minato,
        database => ~"kura_test",
        hostname => ~"localhost",
        port => 5555,
        username => ~"postgres",
        password => ~"root",
        pool_size => 6
    }.

pool_config() ->
    #{
        host => "localhost",
        port => 5555,
        database => "kura_test",
        user => "postgres",
        password => "root",
        pool_size => 6,
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    }.
