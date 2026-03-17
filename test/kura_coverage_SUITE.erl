-module(kura_coverage_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("kura.hrl").

-compile(export_all).

-eqwalizer({nowarn_function, insert_all_with_returning/1}).

%%----------------------------------------------------------------------
%% CT callbacks
%%----------------------------------------------------------------------

all() ->
    [
        {group, repo_config},
        {group, migrator},
        {group, repo_worker_errors},
        {group, stream},
        {group, preloader},
        {group, legacy_logging}
    ].

groups() ->
    [
        {repo_config, [], [
            config_without_init,
            config_with_init,
            config_missing_env
        ]},
        {migrator, [sequence], [
            migrate_up,
            migrate_status_after_up,
            migrate_idempotent,
            rollback_one,
            rollback_multiple_steps,
            rollback_all,
            migrate_after_rollback,
            migrate_error_bad_sql,
            migrate_with_safe_callback
        ]},
        {repo_worker_errors, [], [
            update_all_counts,
            delete_all_counts,
            delete_not_found,
            insert_with_opts_on_conflict,
            insert_all_with_returning,
            insert_all_without_returning,
            reload_no_primary_key,
            multi_with_fun_insert,
            multi_with_fun_update,
            multi_with_fun_delete,
            insert_invalid_changeset_with_opts,
            update_invalid_changeset,
            handle_pg_error_not_null,
            handle_pg_error_fk_violation,
            build_log_event_error_result,
            build_log_event_num_rows,
            build_log_event_bare_result,
            build_telemetry_metadata_error,
            build_telemetry_metadata_num_rows,
            build_telemetry_metadata_bare,
            extract_source_update,
            extract_source_into,
            extract_source_nomatch,
            default_logger_fn
        ]},
        {stream, [sequence], [
            stream_3_arity,
            stream_custom_batch_size
        ]},
        {preloader, [], [
            preload_nested_nil,
            preload_nested_single_map
        ]},
        {legacy_logging, [], [
            legacy_log_true,
            legacy_log_fun,
            legacy_log_mf
        ]}
    ].

%%----------------------------------------------------------------------
%% Suite init/end
%%----------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    kura_sandbox:start(),
    create_tables(),
    Config.

end_per_suite(_Config) ->
    drop_tables(),
    ok.

%%----------------------------------------------------------------------
%% Group init/end
%%----------------------------------------------------------------------

init_per_group(migrator, Config) ->
    %% Register a fake application with migration modules
    MigMods = [m20250101120000_coverage_create_cov_table, m20250101130000_coverage_add_cov_index],
    AppSpec =
        {application, kura_coverage_mig_app, [
            {description, "test app for migration coverage"},
            {vsn, "0.0.1"},
            {modules, [kura_coverage_mig_repo | MigMods]},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    ok = application:load(AppSpec),
    application:set_env(kura_coverage_mig_app, kura_coverage_mig_repo, #{
        pool => kura_test_repo,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 5
    }),
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    Config;
init_per_group(stream, Config) ->
    %% Streams use pgo:transaction internally, incompatible with sandbox.
    %% Insert test data outside sandbox, clean up in end_per_group.
    Config;
init_per_group(_, Config) ->
    Config.

end_per_group(migrator, _Config) ->
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    application:unload(kura_coverage_mig_app),
    ok;
end_per_group(stream, _Config) ->
    kura_test_repo:query("DELETE FROM users WHERE name LIKE 'Stream3%' OR name LIKE 'SBS_%'", []),
    ok;
end_per_group(_, _Config) ->
    ok.

%%----------------------------------------------------------------------
%% Testcase init/end (sandbox isolation)
%%----------------------------------------------------------------------

no_sandbox_tests() ->
    [
        config_without_init,
        config_with_init,
        config_missing_env,
        build_log_event_error_result,
        build_log_event_num_rows,
        build_log_event_bare_result,
        build_telemetry_metadata_error,
        build_telemetry_metadata_num_rows,
        build_telemetry_metadata_bare,
        extract_source_update,
        extract_source_into,
        extract_source_nomatch,
        reload_no_primary_key,
        insert_invalid_changeset_with_opts,
        update_invalid_changeset,
        default_logger_fn,
        migrate_up,
        migrate_status_after_up,
        migrate_idempotent,
        rollback_one,
        rollback_multiple_steps,
        rollback_all,
        migrate_after_rollback,
        migrate_error_bad_sql,
        migrate_with_safe_callback,
        stream_3_arity,
        stream_custom_batch_size
    ].

init_per_testcase(TC, Config) ->
    case lists:member(TC, no_sandbox_tests()) of
        true ->
            Config;
        false ->
            kura_sandbox:checkout(kura_test_repo),
            Config
    end.

end_per_testcase(TC, _Config) ->
    case lists:member(TC, no_sandbox_tests()) of
        true ->
            ok;
        false ->
            kura_sandbox:checkin(kura_test_repo),
            ok
    end.

%%----------------------------------------------------------------------
%% Table management
%%----------------------------------------------------------------------

create_tables() ->
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS profiles ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  bio VARCHAR(255),"
        "  user_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  body TEXT,"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS comments ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  body TEXT NOT NULL,"
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tags ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts_tags ("
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  tag_id BIGINT NOT NULL REFERENCES tags(id),"
        "  PRIMARY KEY (post_id, tag_id)"
        ")",
        []
    ),
    ok.

drop_tables() ->
    kura_test_repo:query("DROP TABLE IF EXISTS posts_tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS comments CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS posts CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS profiles CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => Name, email => Email}, [name, email]),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

insert_profile(Bio, UserId) ->
    CS = kura_changeset:cast(
        kura_test_profile_schema,
        #{},
        #{bio => Bio, user_id => UserId},
        [bio, user_id]
    ),
    kura_test_repo:insert(CS).

%%----------------------------------------------------------------------
%% repo_config group
%%----------------------------------------------------------------------

config_without_init(_Config) ->
    application:set_env(kura, kura_coverage_noinit_repo, #{database => <<"test_db">>}),
    Result = kura_repo:config(kura_coverage_noinit_repo),
    ?assertEqual(#{database => <<"test_db">>}, Result).

config_with_init(_Config) ->
    application:set_env(kura, kura_coverage_init_repo, #{database => <<"test_db">>}),
    Result = kura_repo:config(kura_coverage_init_repo),
    ?assertEqual(#{database => <<"test_db">>, extra => true}, Result).

config_missing_env(_Config) ->
    application:unset_env(kura, kura_coverage_noinit_repo),
    Result = kura_repo:config(kura_coverage_noinit_repo),
    ?assertEqual(#{}, Result).

%%----------------------------------------------------------------------
%% migrator group
%%----------------------------------------------------------------------

migrate_up(_Config) ->
    {ok, Versions} = kura_migrator:migrate(kura_coverage_mig_repo),
    ?assertEqual(2, length(Versions)),
    ?assert(lists:member(20250101120000, Versions)),
    ?assert(lists:member(20250101130000, Versions)),
    {ok, _} = kura_test_repo:query("SELECT 1 FROM coverage_items LIMIT 0", []).

migrate_status_after_up(_Config) ->
    Status = kura_migrator:status(kura_coverage_mig_repo),
    ?assertEqual(2, length(Status)),
    lists:foreach(fun({_V, _M, S}) -> ?assertEqual(up, S) end, Status).

migrate_idempotent(_Config) ->
    {ok, Versions} = kura_migrator:migrate(kura_coverage_mig_repo),
    ?assertEqual([], Versions).

rollback_one(_Config) ->
    {ok, Rolled} = kura_migrator:rollback(kura_coverage_mig_repo),
    ?assertEqual([20250101130000], Rolled),
    Status = kura_migrator:status(kura_coverage_mig_repo),
    [{_, _, up}, {_, _, pending}] = Status.

rollback_multiple_steps(_Config) ->
    {ok, _} = kura_migrator:migrate(kura_coverage_mig_repo),
    {ok, Rolled} = kura_migrator:rollback(kura_coverage_mig_repo, 2),
    ?assertEqual(2, length(Rolled)),
    Status = kura_migrator:status(kura_coverage_mig_repo),
    lists:foreach(fun({_V, _M, S}) -> ?assertEqual(pending, S) end, Status).

rollback_all(_Config) ->
    {ok, _} = kura_migrator:migrate(kura_coverage_mig_repo),
    {ok, Rolled} = kura_migrator:rollback(kura_coverage_mig_repo, 100),
    ?assertEqual(2, length(Rolled)).

migrate_after_rollback(_Config) ->
    {ok, Versions} = kura_migrator:migrate(kura_coverage_mig_repo),
    ?assertEqual(2, length(Versions)).

migrate_error_bad_sql(_Config) ->
    %% Add a bad migration module to trigger migration_failed error path
    application:unload(kura_coverage_mig_app),
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    MigMods = [m20250101140000_coverage_bad_migration],
    AppSpec =
        {application, kura_coverage_mig_app, [
            {description, "test app for migration coverage"},
            {vsn, "0.0.1"},
            {modules, [kura_coverage_mig_repo | MigMods]},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    ok = application:load(AppSpec),
    application:set_env(kura_coverage_mig_app, kura_coverage_mig_repo, #{
        pool => kura_test_repo
    }),
    {error, _Reason} = kura_migrator:migrate(kura_coverage_mig_repo),
    application:unload(kura_coverage_mig_app).

migrate_with_safe_callback(_Config) ->
    %% Test migration with safe/0 callback (covers get_safe_entries, log_warnings with table context)
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []),
    %% Create the table that the safe migration will alter
    kura_test_repo:query(
        "CREATE TABLE coverage_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  old_col VARCHAR(255),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    MigMods = [m20250101110000_coverage_safe_migration],
    AppSpec =
        {application, kura_coverage_mig_app, [
            {description, "test app for migration coverage"},
            {vsn, "0.0.1"},
            {modules, [kura_coverage_mig_repo | MigMods]},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    ok = application:load(AppSpec),
    application:set_env(kura_coverage_mig_app, kura_coverage_mig_repo, #{
        pool => kura_test_repo
    }),
    {ok, [20250101110000]} = kura_migrator:migrate(kura_coverage_mig_repo),
    application:unload(kura_coverage_mig_app),
    kura_test_repo:query("DROP TABLE IF EXISTS coverage_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS schema_migrations CASCADE", []).

%%----------------------------------------------------------------------
%% repo_worker_errors group
%%----------------------------------------------------------------------

update_all_counts(_Config) ->
    {ok, User} = insert_user(<<"UAE_User">>, <<"uae@test.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"nonexistent">>}),
    {ok, 0} = kura_test_repo:update_all(Q, #{name => <<"updated">>}),
    Q2 = kura_query:where(kura_query:from(kura_test_schema), {id, maps:get(id, User)}),
    {ok, 1} = kura_test_repo:update_all(Q2, #{name => <<"updated">>}).

delete_all_counts(_Config) ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"nonexistent_del">>}),
    {ok, 0} = kura_test_repo:delete_all(Q).

delete_not_found(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{id => 999999}, #{}, []),
    {error, not_found} = kura_test_repo:delete(CS).

insert_with_opts_on_conflict(_Config) ->
    %% Insert with on_conflict that targets a specific field
    {ok, _} = insert_user(<<"ConflictUser">>, <<"conflict@test.com">>),
    %% Add unique constraint on email first
    kura_test_repo:query(
        "CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique ON users (email)", []
    ),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{name => <<"ConflictUser2">>, email => <<"conflict@test.com">>},
        [name, email]
    ),
    Result = kura_test_repo:insert(CS, #{on_conflict => {email, nothing}}),
    ?assertMatch({ok, _}, Result),
    kura_test_repo:query("DROP INDEX IF EXISTS users_email_unique", []).

insert_with_opts_returning_rows(_Config) ->
    %% Insert with on_conflict returning empty rows (noop branch, line 175-176)
    ok.

insert_all_with_returning(_Config) ->
    Entries = [
        #{name => <<"IAR_1">>, email => <<"iar1@test.com">>},
        #{name => <<"IAR_2">>, email => <<"iar2@test.com">>}
    ],
    {ok, Count, Rows} = kura_test_repo:insert_all(kura_test_schema, Entries, #{returning => true}),
    ?assertEqual(2, Count),
    ?assertEqual(2, length(Rows)),
    ?assert(lists:all(fun(R) -> is_map_key(id, R) end, Rows)).

insert_all_without_returning(_Config) ->
    Entries = [
        #{name => <<"IANR_1">>, email => <<"ianr1@test.com">>},
        #{name => <<"IANR_2">>, email => <<"ianr2@test.com">>}
    ],
    {ok, 2} = kura_test_repo:insert_all(kura_test_schema, Entries, #{}).

reload_no_primary_key(_Config) ->
    Result = kura_repo_worker:reload(kura_test_repo, kura_test_schema, #{name => <<"test">>}),
    ?assertEqual({error, no_primary_key}, Result).

multi_with_fun_insert(_Config) ->
    M = kura_multi:new(),
    M1 = kura_multi:insert(M, create_user, fun(_Acc) ->
        kura_changeset:cast(
            kura_test_schema, #{}, #{name => <<"MFI">>, email => <<"mfi@test.com">>}, [name, email]
        )
    end),
    {ok, Results} = kura_test_repo:multi(M1),
    ?assertMatch(#{create_user := #{name := <<"MFI">>}}, Results).

multi_with_fun_update(_Config) ->
    {ok, User} = insert_user(<<"MFU_User">>, <<"mfu@test.com">>),
    M = kura_multi:new(),
    M1 = kura_multi:update(M, update_user, fun(_Acc) ->
        kura_changeset:cast(kura_test_schema, User, #{name => <<"MFU_Updated">>}, [name])
    end),
    {ok, Results} = kura_test_repo:multi(M1),
    ?assertEqual(<<"MFU_Updated">>, maps:get(name, maps:get(update_user, Results))).

multi_with_fun_delete(_Config) ->
    {ok, User} = insert_user(<<"MFD_User">>, <<"mfd@test.com">>),
    M = kura_multi:new(),
    M1 = kura_multi:delete(M, delete_user, fun(_Acc) ->
        kura_changeset:cast(kura_test_schema, User, #{}, [])
    end),
    {ok, Results} = kura_test_repo:multi(M1),
    ?assertMatch(#{delete_user := _}, Results).

insert_invalid_changeset_with_opts(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name, email]),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    {error, ErrCS} = kura_repo_worker:insert(kura_test_repo, CS1, #{}),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assertEqual(insert, ErrCS#kura_changeset.action).

update_invalid_changeset(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{id => 1}, #{name => <<"x">>}, [name]),
    CS1 = kura_changeset:validate_length(CS, name, [{min, 5}]),
    {error, ErrCS} = kura_repo_worker:update(kura_test_repo, CS1),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assertEqual(update, ErrCS#kura_changeset.action).

handle_pg_error_not_null(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"nn@test.com">>}, [email]),
    {error, ErrCS} = kura_test_repo:insert(CS),
    ?assertNot(ErrCS#kura_changeset.valid).

handle_pg_error_fk_violation(_Config) ->
    %% FK violation triggers foreign_key constraint handling (code 23503)
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{title => <<"Bad">>, body => <<"body">>, author_id => 999999},
        [title, body, author_id]
    ),
    {error, ErrCS} = kura_test_repo:insert(CS),
    ?assertNot(ErrCS#kura_changeset.valid).

build_log_event_error_result(_Config) ->
    Event = kura_repo_worker:build_log_event(
        kura_test_repo, <<"SELECT 1">>, [], {error, reason}, 100
    ),
    ?assertEqual(error, maps:get(result, Event)),
    ?assertEqual(0, maps:get(num_rows, Event)).

build_log_event_num_rows(_Config) ->
    Event = kura_repo_worker:build_log_event(
        kura_test_repo, <<"SELECT 1">>, [], #{num_rows => 5}, 100
    ),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(5, maps:get(num_rows, Event)).

build_log_event_bare_result(_Config) ->
    %% Result that is neither error nor has num_rows/rows (covers line 402 _ -> {ok, 0})
    Event = kura_repo_worker:build_log_event(
        kura_test_repo, <<"SELECT 1">>, [], ok, 50
    ),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(0, maps:get(num_rows, Event)).

build_telemetry_metadata_error(_Config) ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        kura_test_repo, <<"SELECT 1">>, [], {error, reason}
    ),
    ?assertEqual(error, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)).

build_telemetry_metadata_num_rows(_Config) ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        kura_test_repo, <<"SELECT 1">>, [], #{num_rows => 3}
    ),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(3, maps:get(num_rows, Meta)).

build_telemetry_metadata_bare(_Config) ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        kura_test_repo, <<"SELECT 1">>, [], ok
    ),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)).

extract_source_update(_Config) ->
    ?assertEqual(
        <<"users">>, kura_repo_worker:extract_source(<<"UPDATE \"users\" SET name = $1">>)
    ).

extract_source_into(_Config) ->
    ?assertEqual(
        <<"users">>, kura_repo_worker:extract_source(<<"INSERT INTO \"users\" (name) VALUES ($1)">>)
    ).

extract_source_nomatch(_Config) ->
    ?assertEqual(undefined, kura_repo_worker:extract_source(<<"SELECT 1">>)).

default_logger_fn(_Config) ->
    Logger = kura_repo_worker:default_logger(),
    ?assert(is_function(Logger, 1)),
    Logger(#{query => <<"SELECT 1">>, duration_us => 42}).

%%----------------------------------------------------------------------
%% stream group
%%----------------------------------------------------------------------

stream_3_arity(_Config) ->
    %% Insert data outside sandbox (streams use their own pgo:transaction)
    {ok, _} = insert_user(<<"Stream3_1">>, <<"stream3_1@test.com">>),
    {ok, _} = insert_user(<<"Stream3_2">>, <<"stream3_2@test.com">>),

    Q = kura_query:where(kura_query:from(kura_test_schema), {name, like, <<"Stream3%">>}),
    Self = self(),
    ok = kura_stream:stream(kura_test_repo, Q, fun(Batch) ->
        Self ! {batch, Batch},
        ok
    end),
    receive
        {batch, Rows} -> ?assert(length(Rows) >= 2)
    after 5000 -> ct:fail(timeout)
    end.

stream_custom_batch_size(_Config) ->
    Entries = [
        #{
            name => iolist_to_binary([<<"SBS_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"sbs_">>, integer_to_binary(I), <<"@test.com">>])
        }
     || I <- lists:seq(1, 5)
    ],
    {ok, 5} = kura_test_repo:insert_all(kura_test_schema, Entries),

    Q = kura_query:where(kura_query:from(kura_test_schema), {name, like, <<"SBS_%">>}),
    Self = self(),
    ok = kura_stream:stream(
        kura_test_repo,
        Q,
        fun(Batch) ->
            Self ! {batch, length(Batch)},
            ok
        end,
        #{batch_size => 2}
    ),

    BatchSizes = collect_batches([]),
    TotalRows = lists:sum(BatchSizes),
    ?assertEqual(5, TotalRows),
    ?assert(length(BatchSizes) >= 3).

collect_batches(Acc) ->
    receive
        {batch, N} -> collect_batches([N | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.

%%----------------------------------------------------------------------
%% preloader group
%%----------------------------------------------------------------------

preload_nested_nil(_Config) ->
    {ok, User} = insert_user(<<"PNil_User">>, <<"pnil@test.com">>),
    Loaded = kura_test_repo:preload(kura_test_schema, User, [{profile, [user]}]),
    ?assertEqual(nil, maps:get(profile, Loaded)).

preload_nested_single_map(_Config) ->
    {ok, User} = insert_user(<<"PMap_User">>, <<"pmap@test.com">>),
    UserId = maps:get(id, User),
    {ok, _} = insert_profile(<<"My Bio">>, UserId),
    Loaded = kura_test_repo:preload(kura_test_schema, User, [{profile, [user]}]),
    Profile = maps:get(profile, Loaded),
    ?assert(is_map(Profile)),
    ?assertEqual(<<"My Bio">>, maps:get(bio, Profile)),
    NestedUser = maps:get(user, Profile),
    ?assertEqual(UserId, maps:get(id, NestedUser)).

%%----------------------------------------------------------------------
%% legacy_logging group
%%----------------------------------------------------------------------

legacy_log_true(_Config) ->
    application:set_env(kura, log, true),
    {ok, _} = insert_user(<<"LogTrue">>, <<"logtrue@test.com">>),
    application:unset_env(kura, log).

legacy_log_fun(_Config) ->
    Self = self(),
    LogFun = fun(Event) ->
        Self ! {log_event, Event},
        ok
    end,
    application:set_env(kura, log, LogFun),
    {ok, _} = insert_user(<<"LogFun">>, <<"logfun@test.com">>),
    receive
        {log_event, Event} ->
            ?assert(is_map(Event)),
            ?assert(is_map_key(query, Event))
    after 2000 ->
        ct:fail(no_log_event_received)
    end,
    application:unset_env(kura, log).

legacy_log_mf(_Config) ->
    application:set_env(kura, log, {erlang, is_map}),
    {ok, _} = insert_user(<<"LogMF">>, <<"logmf@test.com">>),
    application:unset_env(kura, log).
