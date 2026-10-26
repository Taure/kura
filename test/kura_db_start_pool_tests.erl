-module(kura_db_start_pool_tests).
-include_lib("eunit/include/eunit.hrl").

-export([start_pool/2, log/2]).

%%----------------------------------------------------------------------
%% start_repo_pool/1 reports a pool that did not start
%%----------------------------------------------------------------------

pool_start_failure_is_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, econnrefused}, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo))
        end)
    end),
    ?assertMatch(
        [
            #{
                level := error,
                msg :=
                    {report, #{
                        msg := ~"kura: pool did not start",
                        repo := kura_db_failing_repo,
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

start_repo_pool_passes_the_repo_config_to_the_pool_module_test() ->
    Received = with_pool_answer({ok, self()}, #{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
        get({?MODULE, pool_config})
    end),
    ?assertEqual(#{database => ~"db"}, Received).

pool_already_started_is_not_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, {already_started, self()}}, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo))
        end)
    end),
    ?assertEqual([], Events).

%%----------------------------------------------------------------------
%% kura_repo_worker:start/1 reports a pool that did not start
%%----------------------------------------------------------------------

repo_worker_start_failure_is_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, econnrefused}, #{database => ~"db"}, fun() ->
            ?assertEqual({error, econnrefused}, kura_repo_worker:start(kura_db_failing_repo))
        end)
    end),
    ?assertMatch(
        [
            #{
                msg :=
                    {report, #{
                        msg := ~"kura: pool did not start",
                        repo := kura_db_failing_repo,
                        reason := econnrefused
                    }}
            }
        ],
        Events
    ).

repo_worker_start_failure_hides_the_password_test() ->
    Child =
        {child, undefined, r, {minato_pool, start_link, [r, #{password => "s3cret"}]}, permanent,
            false, 5000, worker, [minato_pool]},
    Events = with_captured_logs(fun() ->
        with_pool_answer(
            {error, {boom, Child}},
            #{database => ~"db", password => ~"s3cret"},
            fun() ->
                {error, Reason} = kura_repo_worker:start(kura_db_failing_repo),
                ?assertEqual(nomatch, string:find(io_lib:format("~0p", [Reason]), "s3cret"))
            end
        )
    end),
    [#{msg := {report, Report}}] = Events,
    Logged = lists:flatten(io_lib:format("~p", [Report])),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")),
    ?assertNotEqual(nomatch, string:find(Logged, "boom")).

repo_worker_start_failure_hides_a_non_ascii_password_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(
            {error, {boom, {state, ~"pä"}}},
            #{database => ~"db", password => ~"pä"},
            fun() ->
                ?assertEqual(
                    {error, {boom, {state, redacted}}}, kura_repo_worker:start(kura_db_failing_repo)
                )
            end
        )
    end),
    ?assertMatch([#{msg := {report, #{reason := {boom, {state, redacted}}}}}], Events).

repo_worker_already_started_is_not_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, {already_started, self()}}, #{database => ~"db"}, fun() ->
            ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo))
        end)
    end),
    ?assertEqual([], Events).

%%----------------------------------------------------------------------
%% A start that raises, and secrets other than the password
%%----------------------------------------------------------------------

a_raised_start_is_logged_and_fails_without_the_password_test() ->
    Config = #{password => ~"s3cret"},
    {Events, Raised} =
        with_pool_answer({raise, {bad_opts, ~"s3cret"}}, Config, fun() ->
            Run = fun() ->
                try kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config) of
                    Result -> {returned, Result}
                catch
                    Class:Reason:Stack -> {Class, Reason, Stack}
                end
            end,
            Self = self(),
            Logs = with_captured_logs(fun() -> Self ! {raised, Run()} end),
            receive
                {raised, R} -> {Logs, R}
            end
        end),
    ?assertMatch(
        {error, {pool_start_failed, kura_db_failing_repo, {error, {bad_opts, redacted}}}, _},
        Raised
    ),
    {error, _, Stack} = Raised,
    ?assertEqual(nomatch, string:find(io_lib:format("~0p", [Stack]), "s3cret")),
    [#{msg := {report, Report}}] = Events,
    Logged = lists:flatten(io_lib:format("~p", [Report])),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")),
    ?assertNotEqual(nomatch, string:find(Logged, "stacktrace")).

a_tls_private_key_is_redacted_test() ->
    Config = #{ssl_options => [{key, {'PrivateKeyInfo', ~"PRIVKEY"}}]},
    Logged = logged_report({error, {bad_pool_size, Config}}, Config),
    ?assertEqual(nomatch, string:find(Logged, "PRIVKEY")),
    ?assertNotEqual(nomatch, string:find(Logged, "bad_pool_size")).

a_tls_private_key_in_positional_args_is_redacted_test() ->
    Key = {'PrivateKeyInfo', ~"PRIVKEY"},
    Config = #{ssl_options => [{key, Key}]},
    Stack = [{ssl, validate_option, [key, Key, #{}], []}],
    Logged = logged_report(
        {error, {shutdown, {failed_to_start_child, c, {function_clause, Stack}}}}, Config
    ),
    ?assertEqual(nomatch, string:find(Logged, "PRIVKEY")),
    ?assertEqual(
        {bad_der, redacted}, logged_reason({error, {bad_der, ~"PRIVKEY"}}, Config)
    ).

a_tls_private_key_under_a_map_key_is_redacted_test() ->
    Config = #{ssl => #{key => ~"PRIVKEY"}},
    ?assertEqual(
        {boom, #{key => redacted}}, logged_reason({error, {boom, #{key => ~"PRIVKEY"}}}, Config)
    ).

a_non_ascii_binary_password_as_code_points_is_redacted_test() ->
    ?assertEqual(
        {boom, redacted}, logged_reason({error, {boom, [112, 228]}}, #{password => ~"pä"})
    ).

a_non_ascii_binary_password_as_bytes_is_redacted_test() ->
    ?assertEqual(
        {boom, redacted}, logged_reason({error, {boom, [112, 195, 164]}}, #{password => ~"pä"})
    ).

a_non_ascii_charlist_password_as_utf8_is_redacted_test() ->
    ?assertEqual(
        {boom, {state, redacted}},
        logged_reason({error, {boom, {state, ~"pä"}}}, #{password => [112, 228]})
    ).

an_empty_binary_password_is_not_treated_as_a_secret_test() ->
    ?assertEqual({bad_value, ~""}, logged_reason({error, {bad_value, ~""}}, #{password => ~""})).

a_binary_password_held_as_a_charlist_is_redacted_test() ->
    Logged = logged_report({error, {boom, {state, "s3cret"}}}, #{password => ~"s3cret"}),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")).

repo_worker_start_accepts_a_charlist_password_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(
            {error, {boom, {state, ~"s3cret"}}},
            #{database => ~"db", password => "s3cret"},
            fun() ->
                ?assertEqual(
                    {error, {boom, {state, redacted}}}, kura_repo_worker:start(kura_db_failing_repo)
                )
            end
        )
    end),
    ?assertMatch([#{msg := {report, #{reason := {boom, {state, redacted}}}}}], Events).

repo_worker_start_passes_the_repo_config_to_the_pool_module_test() ->
    Config = #{
        hostname => ~"db.internal",
        username => ~"app",
        password => ~"s3cret",
        database => ~"app_db",
        pool_size => 3
    },
    Received = with_pool_answer({ok, self()}, Config, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        get({?MODULE, pool_config})
    end),
    ?assertEqual(Config, Received).

repo_worker_start_adds_no_postgres_defaults_test() ->
    Received = with_pool_answer({ok, self()}, #{}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        get({?MODULE, pool_config})
    end),
    ?assertEqual(#{}, Received).

repo_worker_start_keeps_the_repo_settings_from_the_pool_module_test() ->
    Repo = #{
        backend => kura_backend_postgres,
        dialect => kura_dialect_pg,
        driver_module => kura_driver_minato,
        pool => other_pool,
        read_only => true,
        replicas => [other_repo],
        uuid_version => 7,
        encryption => #{keys => [{1, ~"S0VZ"}]},
        keyring => kura_keyring_env
    },
    Received = with_pool_answer({ok, self()}, Repo#{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        get({?MODULE, pool_config})
    end),
    ?assertEqual(#{database => ~"db"}, Received).

an_improper_list_password_is_not_raised_with_the_password_test() ->
    Config = #{password => improper_list(x)},
    Raised =
        try logged_reason({error, {boom, improper_list(x)}}, Config) of
            Reason -> {returned, Reason}
        catch
            Class:Error:Stack -> {Class, Error, Stack}
        end,
    ?assertEqual({returned, {boom, redacted}}, Raised).

repo_worker_start_names_the_pool_from_the_config_test() ->
    with_pool_answer({ok, self()}, #{pool => other_pool}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        ?assertEqual(other_pool, get({?MODULE, pool_name}))
    end).

%%----------------------------------------------------------------------
%% Boot and kura_repo_worker:start/1 resolve the config the same way
%%----------------------------------------------------------------------

boot_names_the_pool_from_the_config_test() ->
    with_pool_answer({ok, self()}, #{pool => other_pool}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
        ?assertEqual(other_pool, get({?MODULE, pool_name})),
        ?assertEqual(other_pool, kura_db:get_pool(kura_db_failing_repo))
    end).

boot_names_the_pool_after_the_repo_without_a_pool_key_test() ->
    with_pool_answer({ok, self()}, #{}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
        ?assertEqual(kura_db_failing_repo, get({?MODULE, pool_name}))
    end).

boot_applies_the_repo_init_test() ->
    with_pool_answer({ok, self()}, kura_db_init_repo, #{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_init_repo)),
        ?assertEqual(kura_db_init_pool, get({?MODULE, pool_name})),
        ?assertEqual(#{database => ~"from_init"}, get({?MODULE, pool_config}))
    end).

boot_applies_the_init_of_a_repo_not_yet_loaded_test() ->
    _ = code:purge(kura_db_init_repo),
    _ = code:delete(kura_db_init_repo),
    _ = code:purge(kura_db_init_repo),
    with_pool_answer({ok, self()}, kura_db_init_repo, #{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_init_repo)),
        ?assertEqual(#{database => ~"from_init"}, get({?MODULE, pool_config}))
    end).

repo_worker_start_applies_the_repo_init_test() ->
    with_pool_answer({ok, self()}, kura_db_init_repo, #{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_init_repo)),
        ?assertEqual(kura_db_init_pool, get({?MODULE, pool_name})),
        ?assertEqual(#{database => ~"from_init"}, get({?MODULE, pool_config}))
    end).

boot_and_start_pass_the_same_legacy_flat_config_test() ->
    Flat = [
        {repo, kura_db_failing_repo},
        {dialect, kura_dialect_pg},
        {pool_module, ?MODULE},
        {database, ~"flat_db"}
    ],
    with_flat_env(Flat, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
        AtBoot = get({?MODULE, pool_config}),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        ?assertEqual(AtBoot, get({?MODULE, pool_config})),
        ?assertEqual(#{database => ~"flat_db"}, AtBoot)
    end).

legacy_flat_config_keeps_other_repos_from_the_pool_test() ->
    Flat = [
        {repo, kura_db_failing_repo},
        {repos, #{other_repo => #{password => ~"s3cret"}}},
        {pool_module, ?MODULE},
        {database, ~"flat_db"}
    ],
    with_flat_env(Flat, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        ?assertEqual(#{database => ~"flat_db"}, get({?MODULE, pool_config}))
    end).

with_flat_env(Flat, Run) ->
    Saved = application:get_all_env(kura),
    [application:unset_env(kura, Key) || {Key, _} <- Saved],
    [application:set_env(kura, Key, Value) || {Key, Value} <- Flat],
    put({?MODULE, pool_answer}, {ok, self()}),
    try
        Run()
    after
        [application:unset_env(kura, Key) || {Key, _} <- Flat],
        [application:set_env(kura, Key, Value) || {Key, Value} <- Saved],
        erase({?MODULE, pool_answer}),
        erase({?MODULE, pool_name}),
        erase({?MODULE, pool_config})
    end.

deprecated_pool_config_returns_the_resolved_config_test() ->
    with_pool_answer({ok, self()}, kura_db_init_repo, #{database => ~"db"}, fun() ->
        ?assertEqual(
            #{database => ~"from_init", pool => kura_db_init_pool, pool_module => ?MODULE},
            kura_app:pool_config(kura_db_init_repo)
        )
    end).

deprecated_pool_config_raises_a_raising_init_redacted_test() ->
    Config = #{password => ~"s3cret"},
    {Raised, _Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, Config, fun() ->
            try kura_app:pool_config(kura_db_raising_init_repo) of
                Returned -> {returned, Returned}
            catch
                error:Reason -> Reason
            end
        end)
    end),
    ?assertMatch({init_failed, {error, {bad_config, redacted, _}}}, Raised).

deprecated_pool_config_reads_the_legacy_flat_repo_test() ->
    Flat = [{repo, kura_db_failing_repo}, {pool_module, ?MODULE}, {database, ~"flat_db"}],
    with_flat_env(Flat, fun() ->
        ?assertEqual(#{database => ~"flat_db", pool_module => ?MODULE}, kura_app:pool_config())
    end).

boot_logs_a_raising_init_and_carries_on_test() ->
    Config = #{database => ~"db", password => ~"s3cret"},
    Events = with_captured_logs(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, Config, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_raising_init_repo)),
            ?assertEqual(undefined, get({?MODULE, pool_name}))
        end)
    end),
    [#{level := error, msg := {report, Report}}] = Events,
    ?assertMatch(
        #{
            msg := ~"kura: repo init failed",
            repo := kura_db_raising_init_repo,
            reason := {error, {bad_config, redacted, #{password := redacted}}}
        },
        Report
    ),
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Report]), "s3cret")).

repo_worker_start_returns_a_raising_init_redacted_test() ->
    Config = #{database => ~"db", password => ~"s3cret"},
    Result = with_captured_logs_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, Config, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertMatch({error, {init_failed, {error, {bad_config, redacted, _}}}}, Result),
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Result]), "s3cret")).

a_raising_init_logs_no_arguments_from_the_stack_test() ->
    Config = #{raise => function_clause, password => ~"s3cret"},
    {Result, Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, Config, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertMatch({error, {init_failed, {error, function_clause}}}, Result),
    [#{msg := {report, Report}}] = Events,
    Logged = lists:flatten(io_lib:format("~p", [Report])),
    ?assertEqual(nomatch, string:find(Logged, "s3cret")),
    ?assertNotEqual(nomatch, string:find(Logged, "unmatched")).

with_captured_logs_result(Run) ->
    {Result, _Events} = with_captured_logs_and_result(Run),
    Result.

with_captured_logs_and_result(Run) ->
    Self = self(),
    Events = with_captured_logs(fun() -> Self ! {result, Run()} end),
    receive
        {result, Result} -> {Result, Events}
    end.

boot_logs_and_carries_on_where_start_returns_the_error_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer({error, {missing_option, user}}, #{database => ~"db"}, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
            ?assertEqual(
                {error, {missing_option, user}}, kura_repo_worker:start(kura_db_failing_repo)
            )
        end)
    end),
    ?assertMatch(
        [
            #{msg := {report, #{reason := {missing_option, user}}}},
            #{msg := {report, #{reason := {missing_option, user}}}}
        ],
        Events
    ).

start_pool(Pool, Config) ->
    put({?MODULE, pool_name}, Pool),
    put({?MODULE, pool_config}, Config),
    case get({?MODULE, pool_answer}) of
        {raise, Reason} -> error(Reason, [Pool, Config]);
        Answer -> Answer
    end.

log(#{meta := #{pid := Pid}} = Event, #{config := #{collector := Collector, pid := Pid}}) ->
    Collector ! {logged, Event};
log(_Event, _Config) ->
    ok.

with_pool_answer(Answer, Run) ->
    with_pool_answer(Answer, #{}, Run).

with_pool_answer(Answer, Config, Run) ->
    with_pool_answer(Answer, kura_db_failing_repo, Config, Run).

with_pool_answer(Answer, Repo, Config, Run) ->
    application:set_env(kura, repos, #{Repo => Config#{pool_module => ?MODULE}}),
    put({?MODULE, pool_answer}, Answer),
    try
        Run()
    after
        erase({?MODULE, pool_answer}),
        erase({?MODULE, pool_name}),
        erase({?MODULE, pool_config}),
        application:unset_env(kura, repos)
    end.

logged_report(Answer, Config) ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(Answer, Config, fun() ->
            ?assertMatch(
                {error, _}, kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config)
            )
        end)
    end),
    [#{msg := {report, Report}}] = Events,
    lists:flatten(io_lib:format("~p", [Report])).

logged_reason(Answer, Config) ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(Answer, Config, fun() ->
            ?assertMatch(
                {error, _}, kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config)
            )
        end)
    end),
    [#{msg := {report, #{reason := Reason}}}] = Events,
    Reason.

with_captured_logs(Run) ->
    Config = #{config => #{collector => self(), pid => self()}, level => all},
    ok = logger:add_handler(kura_db_start_pool_tests_capture, ?MODULE, Config),
    Console = logger:get_handler_config(default),
    _ = logger:set_handler_config(default, level, none),
    try
        Run()
    after
        ok = logger:remove_handler(kura_db_start_pool_tests_capture),
        case Console of
            {ok, #{level := Level}} -> ok = logger:set_handler_config(default, level, Level);
            _NoConsole -> ok
        end
    end,
    collected([]).

collected(Acc) ->
    receive
        {logged, Event} -> collected([Event | Acc])
    after 0 -> lists:reverse(Acc)
    end.
