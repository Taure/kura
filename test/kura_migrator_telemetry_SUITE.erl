-module(kura_migrator_telemetry_SUITE).
-moduledoc """
Coverage for `kura_migrator` telemetry events.

The asobi failure mode that motivated this was operationally
invisible: migrations silently failed and the app booted with zero
applied. There was no metric, no event, no log line saying "I just
applied 0 migrations against a brand new database, that's
suspicious." Ops dashboards had nothing to alert on.

These tests pin the telemetry contract so callers building
dashboards or alerting can rely on:

- `[kura, migrator, migrate, start]` — emitted before any work
  (measurements: `system_time`; metadata: `repo`, `pending_count`,
  `direction`)
- `[kura, migrator, migrate, stop]` — always emitted (measurements:
  `duration` in native time; metadata: `repo`, `direction`,
  `result`, `applied_count`, `error_reason`)
- `[kura, migrator, migration, apply]` — per applied migration
  (measurements: `duration`; metadata: `version`, `module`,
  `direction`, `op_count`)

The `applied_count` and `pending_count` metadata are the keys that
would have surfaced the asobi case: alert on
`applied_count == 0 AND pending_count > 0` and the bug becomes a
boot-time pageable event.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    migrate_emits_start_and_stop_with_pending_count/1,
    migrate_emits_per_migration_apply_event/1,
    migrate_idempotent_run_emits_zero_pending/1,
    migrate_pool_unavailable_does_not_emit_events/1,
    rollback_emits_start_and_stop_with_direction_down/1,
    migrate_handler_exception_does_not_break_migrator/1
]).

-define(LIVE_POOL, kura_migrator_telemetry_suite_live).
-define(REPO, kura_migrator_telemetry_test_repo).
-define(APP, kura_migrator_telemetry_test_app).

-define(EVENTS, [
    [kura, migrator, migrate, start],
    [kura, migrator, migrate, stop],
    [kura, migrator, migration, apply]
]).

all() ->
    [
        migrate_emits_start_and_stop_with_pending_count,
        migrate_emits_per_migration_apply_event,
        migrate_idempotent_run_emits_zero_pending,
        migrate_pool_unavailable_does_not_emit_events,
        rollback_emits_start_and_stop_with_direction_down,
        migrate_handler_exception_does_not_break_migrator
    ].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    MigMods = [
        m20250101120000_coverage_create_cov_table,
        m20250101130000_coverage_add_cov_index
    ],
    AppSpec =
        {application, ?APP, [
            {description, "test app for kura_migrator_telemetry_SUITE"},
            {vsn, "0.0.1"},
            {modules, [?REPO | MigMods]},
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
    case pgo_sup:start_child(?LIVE_POOL, pool_config()) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok = poll_pool_ready(?LIVE_POOL, 5000),
    cleanup_db(),
    Config.

end_per_testcase(_TC, _Config) ->
    cleanup_db(),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

migrate_emits_start_and_stop_with_pending_count(_Config) ->
    %% Fresh database has 2 pending migrations. The start event must
    %% report pending_count=2 and the stop event must report
    %% applied_count=2 with result=ok. This is the dashboard primary
    %% key: alert if pending_count > 0 AND applied_count != pending_count.
    Ref = telemetry_test:attach_event_handlers(self(), ?EVENTS),
    {ok, [_, _]} = kura_migrator:migrate(?REPO),
    Events = drain_events(?EVENTS),
    telemetry:detach(Ref),

    Start = find_event([kura, migrator, migrate, start], Events),
    Stop = find_event([kura, migrator, migrate, stop], Events),
    ?assertNotEqual(undefined, Start),
    ?assertNotEqual(undefined, Stop),

    {_, _, StartMd} = Start,
    ?assertEqual(?REPO, maps:get(repo, StartMd)),
    ?assertEqual(2, maps:get(pending_count, StartMd)),
    ?assertEqual(up, maps:get(direction, StartMd)),

    {_, StopMs, StopMd} = Stop,
    ?assertEqual(?REPO, maps:get(repo, StopMd)),
    ?assertEqual(ok, maps:get(result, StopMd)),
    ?assertEqual(2, maps:get(applied_count, StopMd)),
    ?assertEqual(up, maps:get(direction, StopMd)),
    ?assert(is_integer(maps:get(duration, StopMs))).

migrate_emits_per_migration_apply_event(_Config) ->
    %% Each applied migration emits its own apply event so dashboards
    %% can break down duration per migration. Useful when one
    %% migration starts dominating boot time.
    Ref = telemetry_test:attach_event_handlers(self(), ?EVENTS),
    {ok, _} = kura_migrator:migrate(?REPO),
    Events = drain_events(?EVENTS),
    telemetry:detach(Ref),

    ApplyEvents = [E || {[kura, migrator, migration, apply], _, _} = E <- Events],
    ?assertEqual(2, length(ApplyEvents)),
    lists:foreach(
        fun({_, Ms, Md}) ->
            ?assert(is_integer(maps:get(duration, Ms))),
            ?assert(is_integer(maps:get(version, Md))),
            ?assert(is_atom(maps:get(module, Md))),
            ?assertEqual(up, maps:get(direction, Md)),
            ?assert(is_integer(maps:get(op_count, Md)))
        end,
        ApplyEvents
    ).

migrate_idempotent_run_emits_zero_pending(_Config) ->
    %% A no-op migrate run still emits start and stop, but with
    %% pending_count=0 and applied_count=0. This is what an idle CI
    %% looks like; alerting must not pager on it.
    {ok, _} = kura_migrator:migrate(?REPO),

    Ref = telemetry_test:attach_event_handlers(self(), ?EVENTS),
    {ok, []} = kura_migrator:migrate(?REPO),
    Events = drain_events(?EVENTS),
    telemetry:detach(Ref),

    {_, _, StartMd} = find_event([kura, migrator, migrate, start], Events),
    {_, _, StopMd} = find_event([kura, migrator, migrate, stop], Events),
    ?assertEqual(0, maps:get(pending_count, StartMd)),
    ?assertEqual(0, maps:get(applied_count, StopMd)),
    ?assertEqual(ok, maps:get(result, StopMd)),
    %% No apply events on a no-op run.
    ?assertEqual(
        [],
        [E || {[kura, migrator, migration, apply], _, _} = E <- Events]
    ).

migrate_pool_unavailable_does_not_emit_events(_Config) ->
    %% If the pool isn't ready and `migrate/1` returns
    %% {error, pool_unavailable} via the wait_for_pool gate, no
    %% migrate-lifecycle events are emitted because nothing happened.
    %% Alerting on the absence of a stop event after a start is a
    %% useful "stuck migrator" signal — preserve that signal by not
    %% emitting either event for a pool failure.
    application:set_env(?APP, ?REPO, #{
        pool => kura_no_such_pool_for_telemetry_test,
        pool_size => 1
    }),
    application:set_env(kura, migration_pool_ready_timeout, 50),

    Ref = telemetry_test:attach_event_handlers(self(), ?EVENTS),
    {error, pool_unavailable} = kura_migrator:migrate(?REPO),
    Events = drain_events(?EVENTS),
    telemetry:detach(Ref),
    application:unset_env(kura, migration_pool_ready_timeout),
    set_default_repo_config(),

    ?assertEqual([], Events).

rollback_emits_start_and_stop_with_direction_down(_Config) ->
    %% rollback/1,2 must emit the same shape of events as migrate, with
    %% direction=down so dashboards can distinguish forward vs
    %% backward runs.
    {ok, _} = kura_migrator:migrate(?REPO),

    Ref = telemetry_test:attach_event_handlers(self(), ?EVENTS),
    {ok, _} = kura_migrator:rollback(?REPO),
    Events = drain_events(?EVENTS),
    telemetry:detach(Ref),

    {_, _, StartMd} = find_event([kura, migrator, migrate, start], Events),
    {_, _, StopMd} = find_event([kura, migrator, migrate, stop], Events),
    ?assertEqual(down, maps:get(direction, StartMd)),
    ?assertEqual(down, maps:get(direction, StopMd)),
    ?assertEqual(ok, maps:get(result, StopMd)),
    ?assertEqual(1, maps:get(applied_count, StopMd)).

migrate_handler_exception_does_not_break_migrator(_Config) ->
    %% Telemetry handlers run inline on the emitter's process. A
    %% buggy handler (e.g. one that crashes on a missing key) must
    %% never cascade into a migrator failure — a dropped event is
    %% always preferable to a dropped migration.
    HandlerId = {?MODULE, broken_handler, erlang:unique_integer()},
    ok = telemetry:attach(
        HandlerId,
        [kura, migrator, migrate, start],
        fun broken_handler/4,
        []
    ),
    try
        Result = kura_migrator:migrate(?REPO),
        ?assertMatch({ok, _}, Result)
    after
        telemetry:detach(HandlerId)
    end.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

broken_handler(_EventName, _Measurements, _Metadata, _Config) ->
    error(deliberately_broken).

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

drain_events(Filter) ->
    drain_events(Filter, []).

drain_events(Filter, Acc) ->
    receive
        {EventName, _Ref, Measurements, Metadata} ->
            case lists:member(EventName, Filter) of
                true -> drain_events(Filter, [{EventName, Measurements, Metadata} | Acc]);
                false -> drain_events(Filter, Acc)
            end
    after 100 ->
        lists:reverse(Acc)
    end.

find_event(Name, Events) ->
    case [E || {N, _, _} = E <- Events, N =:= Name] of
        [E | _] -> E;
        [] -> undefined
    end.

cleanup_db() ->
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
