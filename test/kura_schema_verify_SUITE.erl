-module(kura_schema_verify_SUITE).
-moduledoc """
Coverage for `kura_schema_verify` — runtime drift detection between
declared schemas and the live database.

The asobi case that motivated this was: a `kura_schema:indexes/0`
callback declared a unique index that no migration ever created.
Code that referenced the constraint by name in `on_conflict` failed
on every upsert. The app booted successfully and the failure only
showed up under load.

These tests pin the contract `verify/1,2` must hold so callers can
trust it as a healthcheck/readiness gate:

- happy path: aligned schema returns ok
- missing table: declared but never created
- missing column: schema added a field that no migration migrated in
- missing index (the asobi case): `indexes/0` declares an index that
  no migration created
- column-aliasing: schemas using `column = <<"...">>` should compare
  against the aliased column, not the field name
- absence of `indexes/0`: a schema without the optional callback
  must not produce false positives
- pool unreachable: returns `{error, pool_unavailable}` rather than
  crashing the caller (e.g. during boot before pool is up)
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("kura.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    verify_returns_ok_when_schema_matches_db/1,
    verify_reports_missing_table/1,
    verify_reports_missing_column/1,
    verify_reports_missing_index/1,
    verify_handles_column_aliasing/1,
    verify_skips_schema_without_indexes_callback/1,
    verify_returns_pool_error_when_pool_missing/1,
    verify_with_explicit_module_list/1,
    verify_with_empty_module_list_short_circuits/1,
    discover_schemas_finds_implementing_modules/1
]).

-define(LIVE_POOL, kura_schema_verify_suite_live).
-define(REPO, kura_schema_verify_test_repo).
-define(APP, kura_schema_verify_test_app).
-define(VERIFY_TABLE, <<"kura_verify_test_t">>).

all() ->
    [
        verify_returns_ok_when_schema_matches_db,
        verify_reports_missing_table,
        verify_reports_missing_column,
        verify_reports_missing_index,
        verify_handles_column_aliasing,
        verify_skips_schema_without_indexes_callback,
        verify_returns_pool_error_when_pool_missing,
        verify_with_explicit_module_list,
        verify_with_empty_module_list_short_circuits,
        discover_schemas_finds_implementing_modules
    ].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    AppSpec =
        {application, ?APP, [
            {description, "test app for kura_schema_verify_SUITE"},
            {vsn, "0.0.1"},
            {modules, [
                ?REPO,
                kura_schema_verify_test_complete_schema,
                kura_schema_verify_test_aliased_schema,
                kura_schema_verify_test_no_indexes_schema
            ]},
            {registered, []},
            {applications, [kernel, stdlib]}
        ]},
    ok = application:load(AppSpec),
    application:set_env(?APP, ?REPO, #{
        pool => ?LIVE_POOL,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 2
    }),
    application:set_env(kura, ensure_database, false),
    Config.

end_per_suite(_Config) ->
    application:unset_env(kura, ensure_database),
    application:unload(?APP),
    ok.

init_per_testcase(_TC, Config) ->
    %% Most tests need a live pool. Start it idempotently.
    case pgo_sup:start_child(?LIVE_POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = poll_pool_ready(?LIVE_POOL, 5000),
    drop_test_table(),
    Config.

end_per_testcase(_TC, _Config) ->
    drop_test_table(),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

verify_returns_ok_when_schema_matches_db(_Config) ->
    %% Baseline: a fully-migrated table that matches the schema
    %% declaration must verify cleanly. Anchor the success path.
    create_aligned_table(),
    Schemas = [kura_schema_verify_test_complete_schema],
    ?assertEqual(ok, kura_schema_verify:verify(?REPO, Schemas)).

verify_reports_missing_table(_Config) ->
    %% No CREATE TABLE has run. The schema declares the table; verify
    %% must list it as missing.
    Schemas = [kura_schema_verify_test_complete_schema],
    {drift, Issues} = kura_schema_verify:verify(?REPO, Schemas),
    ?assert(lists:member({missing_table, ?VERIFY_TABLE}, Issues)),
    %% Don't follow-up with column/index checks on a non-existent
    %% table; that would produce noise.
    ?assertNot(
        lists:any(
            fun
                ({missing_column, _, _}) -> true;
                (_) -> false
            end,
            Issues
        )
    ).

verify_reports_missing_column(_Config) ->
    %% Table exists but is missing one declared field. Expect exactly
    %% one missing_column issue, no false positives on the others.
    create_table_missing_email(),
    Schemas = [kura_schema_verify_test_complete_schema],
    {drift, Issues} = kura_schema_verify:verify(?REPO, Schemas),
    ColIssues = [I || {missing_column, _, _} = I <- Issues],
    ?assertMatch([{missing_column, _, <<"email">>}], ColIssues).

verify_reports_missing_index(_Config) ->
    %% This is the asobi case: schema declares an index in indexes/0
    %% but no migration created it. verify/1,2 must surface that.
    create_aligned_table_without_index(),
    Schemas = [kura_schema_verify_test_complete_schema],
    {drift, Issues} = kura_schema_verify:verify(?REPO, Schemas),
    ?assertMatch(
        [{missing_index, _, [email], #{unique := true}}],
        [I || {missing_index, _, _, _} = I <- Issues]
    ).

verify_handles_column_aliasing(_Config) ->
    %% Schemas can map a logical field name to a different physical
    %% column via `column = <<"...">>`. verify/1,2 must compare
    %% against the aliased column, not the field name — otherwise it
    %% would report false positives for any aliased schema.
    create_aliased_table(),
    Schemas = [kura_schema_verify_test_aliased_schema],
    ?assertEqual(ok, kura_schema_verify:verify(?REPO, Schemas)).

verify_skips_schema_without_indexes_callback(_Config) ->
    %% indexes/0 is optional. Schemas without it must not produce
    %% missing_index issues — that would block adoption of the
    %% verifier on legacy schemas.
    create_table_for_no_indexes_schema(),
    Schemas = [kura_schema_verify_test_no_indexes_schema],
    ?assertEqual(ok, kura_schema_verify:verify(?REPO, Schemas)).

verify_returns_pool_error_when_pool_missing(_Config) ->
    %% Switch the repo config to a non-existent pool. verify/1,2 must
    %% return {error, pool_unavailable} rather than crash, so callers
    %% using this in a boot-time healthcheck don't take the whole app
    %% down on a transient pool unavailability.
    application:set_env(?APP, ?REPO, #{
        pool => kura_no_such_pool_for_verify_test,
        pool_size => 1
    }),
    Schemas = [kura_schema_verify_test_complete_schema],
    Result = kura_schema_verify:verify(?REPO, Schemas),
    ?assertEqual({error, pool_unavailable}, Result),
    %% Restore for subsequent tests.
    set_default_repo_config().

verify_with_explicit_module_list(_Config) ->
    %% verify/2 with a hand-picked subset must behave the same as
    %% auto-discovery for those modules. Useful for callers that
    %% want to scope the check.
    create_aligned_table(),
    [Mod] = [kura_schema_verify_test_complete_schema],
    ?assertEqual(ok, kura_schema_verify:verify(?REPO, [Mod])).

verify_with_empty_module_list_short_circuits(_Config) ->
    %% Empty list = nothing to verify = ok. Don't even probe the pool.
    %% A pre-boot caller passing [] must not get a spurious error.
    application:set_env(?APP, ?REPO, #{
        pool => kura_no_such_pool_for_empty_short_circuit,
        pool_size => 1
    }),
    ?assertEqual(ok, kura_schema_verify:verify(?REPO, [])),
    set_default_repo_config().

discover_schemas_finds_implementing_modules(_Config) ->
    Schemas = kura_schema_verify:discover_schemas(?REPO),
    ?assert(lists:member(kura_schema_verify_test_complete_schema, Schemas)),
    ?assert(lists:member(kura_schema_verify_test_aliased_schema, Schemas)),
    ?assert(lists:member(kura_schema_verify_test_no_indexes_schema, Schemas)),
    %% The repo module itself isn't a schema and must not be picked up.
    ?assertNot(lists:member(?REPO, Schemas)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

set_default_repo_config() ->
    application:set_env(?APP, ?REPO, #{
        pool => ?LIVE_POOL,
        database => <<"kura_test">>,
        hostname => <<"localhost">>,
        port => 5555,
        username => <<"postgres">>,
        password => <<"root">>,
        pool_size => 2
    }).

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

drop_test_table() ->
    _ = pgo:query(
        ~"DROP TABLE IF EXISTS kura_verify_test_t CASCADE", [], #{pool => ?LIVE_POOL}
    ),
    _ = pgo:query(
        ~"DROP TABLE IF EXISTS kura_verify_test_aliased CASCADE",
        [],
        #{pool => ?LIVE_POOL}
    ),
    _ = pgo:query(
        ~"DROP TABLE IF EXISTS kura_verify_test_no_idx CASCADE", [], #{pool => ?LIVE_POOL}
    ),
    ok.

create_aligned_table() ->
    %% Matches `kura_schema_verify_test_complete_schema`.
    _ = pgo:query(
        ~"CREATE TABLE kura_verify_test_t (id BIGSERIAL PRIMARY KEY, email VARCHAR(255), name VARCHAR(255))",
        [],
        #{pool => ?LIVE_POOL}
    ),
    _ = pgo:query(
        ~"CREATE UNIQUE INDEX kura_verify_test_t_email_index ON kura_verify_test_t (email)",
        [],
        #{pool => ?LIVE_POOL}
    ),
    ok.

create_aligned_table_without_index() ->
    %% Has all columns but is missing the declared unique email index
    %% — pins the asobi failure mode.
    _ = pgo:query(
        ~"CREATE TABLE kura_verify_test_t (id BIGSERIAL PRIMARY KEY, email VARCHAR(255), name VARCHAR(255))",
        [],
        #{pool => ?LIVE_POOL}
    ),
    ok.

create_table_missing_email() ->
    _ = pgo:query(
        ~"CREATE TABLE kura_verify_test_t (id BIGSERIAL PRIMARY KEY, name VARCHAR(255))",
        [],
        #{pool => ?LIVE_POOL}
    ),
    ok.

create_aliased_table() ->
    %% Matches `kura_schema_verify_test_aliased_schema` — the schema
    %% maps its `email` field to physical column `email_address`.
    _ = pgo:query(
        ~"CREATE TABLE kura_verify_test_aliased (id BIGSERIAL PRIMARY KEY, email_address VARCHAR(255))",
        [],
        #{pool => ?LIVE_POOL}
    ),
    ok.

create_table_for_no_indexes_schema() ->
    _ = pgo:query(
        ~"CREATE TABLE kura_verify_test_no_idx (id BIGSERIAL PRIMARY KEY, body TEXT)",
        [],
        #{pool => ?LIVE_POOL}
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
