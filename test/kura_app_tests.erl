-module(kura_app_tests).
-include_lib("eunit/include/eunit.hrl").

-export([start_pool/2, log/2]).

stop_returns_ok_test() ->
    ?assertEqual(ok, kura_app:stop(undefined)).

stop_with_any_state_test() ->
    ?assertEqual(ok, kura_app:stop(some_state)).

pool_config_defaults_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    Config = kura_app:pool_config(),
    ?assertEqual("localhost", maps:get(host, Config)),
    ?assertEqual(5432, maps:get(port, Config)),
    ?assertEqual("postgres", maps:get(database, Config)),
    ?assertEqual("postgres", maps:get(user, Config)),
    ?assertEqual("", maps:get(password, Config)),
    ?assertEqual(10, maps:get(pool_size, Config)),
    ?assertNot(maps:is_key(socket_options, Config)),
    clear_kura_env().

pool_config_custom_values_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:set_env(kura, host, "db.example.com"),
    application:set_env(kura, port, 5433),
    application:set_env(kura, database, "mydb"),
    application:set_env(kura, user, "myuser"),
    application:set_env(kura, password, "secret"),
    application:set_env(kura, pool_size, 20),
    Config = kura_app:pool_config(),
    ?assertEqual("db.example.com", maps:get(host, Config)),
    ?assertEqual(5433, maps:get(port, Config)),
    ?assertEqual("mydb", maps:get(database, Config)),
    ?assertEqual("myuser", maps:get(user, Config)),
    ?assertEqual("secret", maps:get(password, Config)),
    ?assertEqual(20, maps:get(pool_size, Config)),
    clear_kura_env().

pool_config_socket_options_inet6_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:set_env(kura, socket_options, [inet6]),
    Config = kura_app:pool_config(),
    ?assertEqual([inet6], maps:get(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_multiple_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:set_env(kura, socket_options, [inet6, {recbuf, 8192}]),
    Config = kura_app:pool_config(),
    ?assertEqual([inet6, {recbuf, 8192}], maps:get(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_empty_list_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    application:set_env(kura, socket_options, []),
    Config = kura_app:pool_config(),
    ?assertNot(maps:is_key(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_not_set_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_pg),
    Config = kura_app:pool_config(),
    ?assertNot(maps:is_key(socket_options, Config)),
    clear_kura_env().

%%----------------------------------------------------------------------
%% generic_pool_config (non-PG dialects): pass-through of all kura env
%%----------------------------------------------------------------------

pool_config_generic_passes_through_user_keys_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_sqlite),
    application:set_env(kura, database, <<":memory:">>),
    application:set_env(kura, pool_size, 1),
    Config = kura_app:pool_config(),
    ?assertEqual(<<":memory:">>, maps:get(database, Config)),
    ?assertEqual(1, maps:get(pool_size, Config)),
    ?assertNot(maps:is_key(host, Config)),
    ?assertNot(maps:is_key(port, Config)),
    clear_kura_env().

pool_config_generic_excludes_bookkeeping_keys_test() ->
    clear_kura_env(),
    application:set_env(kura, dialect, kura_dialect_sqlite),
    application:set_env(kura, repo, my_repo),
    application:set_env(kura, backend, my_backend),
    application:set_env(kura, pool_module, kura_pool_sqlite),
    Config = kura_app:pool_config(),
    ?assertNot(maps:is_key(repo, Config)),
    ?assertNot(maps:is_key(backend, Config)),
    ?assertNot(maps:is_key(dialect, Config)),
    ?assertNot(maps:is_key(pool_module, Config)),
    clear_kura_env().

%%----------------------------------------------------------------------
%% Multi-repo: {repos, #{Name => Cfg}}
%%----------------------------------------------------------------------

pool_config_multi_repo_returns_per_repo_map_test() ->
    clear_kura_env(),
    application:set_env(kura, repos, #{
        primary => #{
            dialect => kura_dialect_pg,
            host => "primary.example.com",
            port => 5432
        },
        analytics => #{
            dialect => kura_dialect_sqlite,
            database => <<":memory:">>
        }
    }),
    PrimaryCfg = kura_app:pool_config(primary),
    AnalyticsCfg = kura_app:pool_config(analytics),
    ?assertEqual("primary.example.com", maps:get(host, PrimaryCfg)),
    ?assertEqual(kura_dialect_pg, maps:get(dialect, PrimaryCfg)),
    ?assertEqual(<<":memory:">>, maps:get(database, AnalyticsCfg)),
    ?assertEqual(kura_dialect_sqlite, maps:get(dialect, AnalyticsCfg)),
    application:unset_env(kura, repos),
    clear_kura_env().

pool_config_multi_repo_unknown_repo_falls_back_test() ->
    clear_kura_env(),
    application:set_env(kura, repos, #{primary => #{dialect => kura_dialect_pg}}),
    application:set_env(kura, dialect, kura_dialect_pg),
    Cfg = kura_app:pool_config(unknown_repo),
    ?assertEqual("localhost", maps:get(host, Cfg)),
    application:unset_env(kura, repos),
    clear_kura_env().

%%----------------------------------------------------------------------
%% Compiler picks correct dialect per repo
%%----------------------------------------------------------------------

compiler_picks_per_repo_dialect_test() ->
    clear_kura_env(),
    application:set_env(kura, repos, #{
        repo_pg => #{dialect => kura_dialect_pg},
        repo_sqlite => #{dialect => kura_dialect_sqlite}
    }),
    ?assertEqual(kura_dialect_pg, kura_query_compiler:dialect(repo_pg)),
    ?assertEqual(kura_dialect_sqlite, kura_query_compiler:dialect(repo_sqlite)),
    application:unset_env(kura, repos),
    clear_kura_env().

clear_kura_env() ->
    unset_keys([
        host,
        port,
        database,
        user,
        password,
        pool_size,
        socket_options,
        dialect,
        repo,
        repos,
        backend,
        pool_module,
        driver_module
    ]).

-spec unset_keys([atom()]) -> ok.
unset_keys([]) ->
    ok;
unset_keys([K | Rest]) ->
    application:unset_env(kura, K),
    unset_keys(Rest).

%%----------------------------------------------------------------------
%% start_repo_pool/1 reports a pool that did not start
%%----------------------------------------------------------------------

pool_start_failure_is_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, econnrefused}, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_app_failing_repo))
        end)
    end),
    ?assertMatch(
        [
            #{
                level := error,
                msg :=
                    {report, #{
                        msg := ~"kura: pool did not start",
                        repo := kura_app_failing_repo,
                        pool_module := ?MODULE,
                        reason := econnrefused
                    }}
            }
        ],
        Events
    ).

pool_start_failure_hides_child_password_test() ->
    Child =
        {child, undefined, r, {minato_pool, start_link, [r, #{connection => #{password => ~"pw"}}]},
            permanent, false, 5000, worker, [minato_pool]},
    Logged = logged_report({error, {boom, Child, [{password, "pw2"}]}}, #{}),
    ?assertEqual(nomatch, string:find(Logged, "pw")),
    ?assertNotEqual(nomatch, string:find(Logged, "boom")).

pool_start_failure_hides_configured_password_test() ->
    Stack = [{minato_pool, init, [~"s3cret", "s3cret", [{password, "s3cret"}]], []}],
    Logged = logged_report({error, {function_clause, Stack}}, #{password => "s3cret"}),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")),
    ?assertNotEqual(nomatch, string:find(Logged, "function_clause")).

pool_start_failure_hides_an_improper_list_holding_the_password_test() ->
    Logged = logged_report({error, {bad_opts, improper_list(~"s3cret")}}, #{password => ~"s3cret"}),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")),
    ?assertNotEqual(nomatch, string:find(Logged, "bad_opts")).

-spec improper_list(dynamic()) -> term().
improper_list(Tail) ->
    [host | Tail].

pool_start_failure_keeps_an_empty_password_untouched_test() ->
    Logged = logged_report({error, {bad_value, [], ~""}}, #{password => ""}),
    ?assertEqual(nomatch, string:find(Logged, "redacted")).

pool_already_started_is_not_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, {already_started, self()}}, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_app_failing_repo))
        end)
    end),
    ?assertEqual([], Events).

start_pool(kura_app_failing_repo, _Config) ->
    get({?MODULE, pool_answer}).

log(#{meta := #{pid := Pid}} = Event, #{config := #{collector := Collector, pid := Pid}}) ->
    Collector ! {logged, Event};
log(_Event, _Config) ->
    ok.

with_pool_answer(Answer, Run) ->
    with_pool_answer(Answer, #{}, Run).

with_pool_answer(Answer, Config, Run) ->
    application:set_env(kura, repos, #{
        kura_app_failing_repo => Config#{pool_module => ?MODULE}
    }),
    put({?MODULE, pool_answer}, Answer),
    try
        Run()
    after
        erase({?MODULE, pool_answer}),
        application:unset_env(kura, repos)
    end.

logged_report(Answer, Config) ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(Answer, Config, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_app_failing_repo))
        end)
    end),
    [#{msg := {report, Report}}] = Events,
    lists:flatten(io_lib:format("~p", [Report])).

with_captured_logs(Run) ->
    Config = #{config => #{collector => self(), pid => self()}, level => all},
    ok = logger:add_handler(kura_app_tests_capture, ?MODULE, Config),
    try
        Run()
    after
        ok = logger:remove_handler(kura_app_tests_capture)
    end,
    collected([]).

collected(Acc) ->
    receive
        {logged, Event} -> collected([Event | Acc])
    after 0 -> lists:reverse(Acc)
    end.
