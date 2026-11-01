-module(kura_db_start_pool_tests).
-include_lib("eunit/include/eunit.hrl").

-export([start_pool/2, stop_pool/1, query/5, log/2]).

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

pool_already_started_by_its_owner_is_not_logged_test() ->
    Events = with_pool_answer({ok, self()}, fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        with_captured_logs(fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo))
        end)
    end),
    ?assertEqual([], Events).

adopting_a_running_pool_kura_did_not_start_is_logged_test() ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(
            {error, {already_started, self()}},
            #{password => ~"s3cret", database => ~"db"},
            fun() -> ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)) end
        )
    end),
    ?assertMatch(
        [
            #{
                level := warning,
                msg :=
                    {report, #{
                        msg := ~"kura: adopted a running pool kura did not start",
                        repo := kura_db_failing_repo,
                        pool := kura_db_failing_repo,
                        pool_module := ?MODULE
                    }}
            }
        ],
        Events
    ),
    [#{msg := {report, Report}}] = Events,
    ?assertEqual([action, msg, pool, pool_module, repo], lists:sort(maps:keys(Report))),
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Report]), "s3cret")).

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

repo_worker_already_started_by_its_owner_is_not_logged_test() ->
    Events = with_pool_answer({ok, self()}, #{database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        with_captured_logs(fun() ->
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
                try
                    kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config#{
                        pool_module => ?MODULE
                    })
                of
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

an_encryption_keyring_in_a_pool_error_is_redacted_test() ->
    Config = #{database => ~"db"},
    Echoed = #{encryption => #{keys => [{1, ~"S0VZS0VZ"}]}, keyring => kura_keyring_env},
    ?assertEqual(
        {boom, #{encryption => redacted, keyring => redacted}},
        logged_reason({error, {boom, Echoed}}, Config)
    ).

an_encryption_option_in_a_pool_error_list_is_redacted_test() ->
    ?assertEqual(
        {boom, [{encryption, redacted}, {database, ~"db"}]},
        logged_reason(
            {error, {boom, [{encryption, #{keys => [{1, ~"S0VZ"}]}}, {database, ~"db"}]}},
            #{database => ~"db"}
        )
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

start_pool_runs_the_repo_init_once_test() ->
    ok = kura_db_counting_init_repo:reset(),
    try
        with_pool_answer({ok, self()}, kura_db_counting_init_repo, #{database => ~"db"}, fun() ->
            ?assertEqual(ok, kura_db:start_pool(kura_db_counting_init_repo)),
            ?assertEqual(#{database => ~"db"}, get({?MODULE, pool_config}))
        end),
        ?assertEqual(1, kura_db_counting_init_repo:calls())
    after
        kura_db_counting_init_repo:reset()
    end.

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
    Events = with_captured_logs(fun() ->
        with_flat_env(Flat, fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_failing_repo)),
            AtBoot = get({?MODULE, pool_config}),
            ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
            ?assertEqual(AtBoot, get({?MODULE, pool_config})),
            ?assertEqual(#{database => ~"flat_db", user => "postgres"}, AtBoot)
        end)
    end),
    ?assertMatch([#{level := warning}, #{level := warning}], Events).

legacy_flat_pg_config_gets_the_user_and_database_filled_test() ->
    {Received, Events} = flat_pg_start([]),
    ?assertEqual(#{user => "postgres", database => "postgres"}, Received),
    ?assertMatch(
        [
            #{
                level := warning,
                msg :=
                    {report, #{
                        msg :=
                            ~"kura: deprecated Postgres defaults filled in for the flat repo config",
                        repo := kura_db_failing_repo,
                        filled := [user, database]
                    }}
            }
        ],
        Events
    ),
    [#{msg := {report, #{action := Action}}}] = Events,
    ?assertNotEqual(nomatch, string:find(Action, "kura 3.0")).

legacy_flat_pg_config_with_a_user_gets_the_database_filled_test() ->
    {Received, Events} = flat_pg_start([{user, ~"app"}]),
    ?assertEqual(#{user => ~"app", database => "postgres"}, Received),
    ?assertMatch([#{level := warning, msg := {report, #{filled := [database]}}}], Events).

legacy_flat_pg_config_with_a_username_keeps_it_test() ->
    {Received, Events} = flat_pg_start([{username, ~"app"}, {database, ~"app_db"}]),
    ?assertEqual(#{username => ~"app", database => ~"app_db"}, Received),
    ?assertEqual([], Events).

legacy_flat_pg_config_with_a_user_and_database_is_not_filled_test() ->
    {Received, Events} = flat_pg_start([{user, ~"app"}, {database, ~"app_db"}]),
    ?assertEqual(#{user => ~"app", database => ~"app_db"}, Received),
    ?assertEqual([], Events).

legacy_flat_config_on_another_dialect_is_not_filled_test() ->
    Flat = [
        {repo, kura_db_failing_repo},
        {dialect, kura_test_sqlite_dialect},
        {pool_module, ?MODULE}
    ],
    Events = with_captured_logs(fun() ->
        with_flat_env(Flat, fun() ->
            ?assertEqual(ok, kura_db:start_pool(kura_db_failing_repo)),
            ?assertEqual(#{}, get({?MODULE, pool_config}))
        end)
    end),
    ?assertEqual([], Events).

repos_map_pg_config_is_not_filled_test() ->
    Flat = [
        {repo, kura_db_failing_repo},
        {repos, #{kura_db_failing_repo => #{dialect => kura_dialect_pg, pool_module => ?MODULE}}}
    ],
    Events = with_captured_logs(fun() ->
        with_flat_env(Flat, fun() ->
            ?assertEqual(ok, kura_db:start_pool(kura_db_failing_repo)),
            ?assertEqual(#{}, get({?MODULE, pool_config}))
        end)
    end),
    ?assertEqual([], Events).

flat_pg_start(Settings) ->
    Flat = [{repo, kura_db_failing_repo}, {dialect, kura_dialect_pg}, {pool_module, ?MODULE}],
    with_captured_logs_and_result(fun() ->
        with_flat_env(Flat ++ Settings, fun() ->
            ok = kura_db:start_pool(kura_db_failing_repo),
            get({?MODULE, pool_config})
        end)
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
    ?assertEqual({init_failed, {error, {bad_config, redacted}}}, Raised).

deprecated_pool_config_logs_nothing_on_a_raising_init_test() ->
    {_Raised, Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer(
            {ok, self()}, kura_db_raising_init_repo, #{password => ~"s3cret"}, fun() ->
                try
                    kura_app:pool_config(kura_db_raising_init_repo)
                catch
                    error:Reason -> Reason
                end
            end
        )
    end),
    ?assertEqual([], Events).

deprecated_pool_config_without_a_repo_is_empty_test() ->
    with_flat_env([{pool_module, ?MODULE}], fun() ->
        ?assertEqual(#{}, kura_app:pool_config())
    end).

deprecated_pool_config_of_an_unknown_repo_is_empty_test() ->
    with_flat_env([{repo, kura_db_failing_repo}, {database, ~"flat_db"}], fun() ->
        ?assertEqual(#{}, kura_app:pool_config(kura_db_no_such_repo))
    end).

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
            msg := ~"kura: pool did not start",
            repo := kura_db_raising_init_repo,
            reason := {init_failed, {error, {bad_config, redacted}}}
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
    ?assertEqual({error, {init_failed, {error, {bad_config, redacted}}}}, Result),
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Result]), "s3cret")).

a_raising_init_hides_the_encryption_keyring_test() ->
    Config = #{raise => invalid_config, encryption => #{keys => [{1, ~"S0VZS0VZ"}]}},
    {Result, Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, Config, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertEqual({error, {init_failed, {error, {invalid_config, redacted}}}}, Result),
    [#{msg := {report, Report}}] = Events,
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Report]), "S0VZS0VZ")).

a_raising_init_hides_a_secret_it_fetched_itself_test() ->
    {Result, Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer(
            {ok, self()}, kura_db_raising_init_repo, #{raise => fetched_secret}, fun() ->
                kura_repo_worker:start(kura_db_raising_init_repo)
            end
        )
    end),
    ?assertEqual({error, {init_failed, {error, {badmatch, redacted}}}}, Result),
    [#{msg := {report, Report}}] = Events,
    ?assertEqual(nomatch, string:find(io_lib:format("~p", [Report]), "supersecretvaultpw")).

a_raising_init_with_an_untagged_reason_is_redacted_whole_test() ->
    {Result, _Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, #{raise => thrown}, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertEqual({error, {init_failed, {throw, redacted}}}, Result).

a_raising_init_with_an_untagged_tuple_is_redacted_whole_test() ->
    {Result, _Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, #{raise => untagged}, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertEqual({error, {init_failed, {error, redacted}}}, Result).

a_raising_init_with_an_empty_tuple_is_redacted_whole_test() ->
    {Result, _Events} = with_captured_logs_and_result(fun() ->
        with_pool_answer({ok, self()}, kura_db_raising_init_repo, #{raise => empty}, fun() ->
            kura_repo_worker:start(kura_db_raising_init_repo)
        end)
    end),
    ?assertEqual({error, {init_failed, {exit, redacted}}}, Result).

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

%%----------------------------------------------------------------------
%% Queries read the settings the pool was started with (#200)
%%----------------------------------------------------------------------

init_does_not_run_at_query_time_after_a_pool_start_test() ->
    with_counting_repo(fun(Repo) ->
        ?assertEqual(ok, kura_db:start_pool(Repo)),
        ?assertEqual(counted_pool, kura_db:get_pool(Repo)),
        ?assertEqual(?MODULE, kura_db:get_pool_module(Repo)),
        ?assertEqual(?MODULE, kura_db:get_driver_module(Repo)),
        ?assertEqual(kura_dialect_pg, kura_query_compiler:dialect(Repo)),
        ?assertEqual(false, kura_repo:read_only(Repo)),
        ?assertEqual(Repo, kura_repo:replica(Repo)),
        ?assertEqual(v7, kura_repo_worker:uuid_version(Repo)),
        ?assertEqual(#{rows => [], num_rows => 0}, kura_db:query(Repo, ~"SELECT 1", [])),
        ?assertEqual(1, kura_db_counting_init_repo:calls())
    end).

a_raising_init_after_the_pool_start_does_not_reach_queries_test() ->
    with_counting_repo(fun(Repo) ->
        ?assertEqual(ok, kura_db:start_pool(Repo)),
        ?assertError({init_called_again, 2}, kura_repo:config(Repo)),
        ?assertEqual(#{rows => [], num_rows => 0}, kura_db:query(Repo, ~"SELECT 1", [])),
        ?assertEqual(false, kura_db:in_transaction(Repo)),
        ?assertEqual(false, kura_repo:read_only(Repo))
    end).

the_settings_hold_no_connection_settings_test() ->
    with_pool_answer({ok, self()}, #{password => ~"s3cret", database => ~"db"}, fun() ->
        ?assertEqual(ok, kura_db:start_pool(kura_db_failing_repo)),
        ?assertEqual(
            #{pool => kura_db_failing_repo, pool_module => ?MODULE},
            kura_repo:settings(kura_db_failing_repo)
        )
    end).

starting_the_pool_again_rereads_the_settings_test() ->
    with_pool_answer({ok, self()}, #{read_only => false}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        application:set_env(kura, repos, #{
            kura_db_failing_repo => #{read_only => true, pool_module => ?MODULE}
        }),
        ?assertEqual(false, kura_repo:read_only(kura_db_failing_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        ?assertEqual(true, kura_repo:read_only(kura_db_failing_repo))
    end).

a_failed_start_stores_no_settings_test() ->
    with_pool_answer({error, econnrefused}, #{read_only => false}, fun() ->
        _ = with_captured_logs(fun() -> kura_db:start_pool(kura_db_failing_repo) end),
        application:set_env(kura, repos, #{kura_db_failing_repo => #{read_only => true}}),
        ?assertEqual(true, kura_repo:read_only(kura_db_failing_repo))
    end).

a_repo_kura_did_not_start_reads_its_config_on_each_lookup_test() ->
    kura_db:forget_pools(),
    try
        application:set_env(kura, repos, #{kura_db_unstarted_repo => #{read_only => false}}),
        ?assertEqual(false, kura_repo:read_only(kura_db_unstarted_repo)),
        application:set_env(kura, repos, #{kura_db_unstarted_repo => #{read_only => true}}),
        ?assertEqual(true, kura_repo:read_only(kura_db_unstarted_repo)),
        ?assertEqual(kura_db_unstarted_repo, kura_db:get_pool(kura_db_unstarted_repo))
    after
        application:unset_env(kura, repos)
    end.

a_repo_kura_did_not_start_runs_its_init_on_each_lookup_test() ->
    kura_db:forget_pools(),
    application:set_env(kura, repos, #{kura_db_init_repo => #{}}),
    try
        ?assertEqual(kura_db_init_pool, kura_db:get_pool(kura_db_init_repo)),
        ok = kura_db_counting_init_repo:reset(),
        application:set_env(kura, repos, #{kura_db_counting_init_repo => #{pool => counted_pool}}),
        ?assertEqual(counted_pool, kura_db:get_pool(kura_db_counting_init_repo)),
        ?assertError({init_called_again, 2}, kura_db:get_pool(kura_db_counting_init_repo))
    after
        ok = kura_db_counting_init_repo:reset(),
        application:unset_env(kura, repos)
    end.

with_counting_repo(Run) ->
    Repo = kura_db_counting_init_repo,
    ok = kura_db_counting_init_repo:reset(),
    Config = #{
        pool => counted_pool,
        driver_module => ?MODULE,
        dialect => kura_dialect_pg,
        read_only => false,
        uuid_version => v7
    },
    try
        with_pool_answer({ok, self()}, Repo, Config, fun() -> Run(Repo) end)
    after
        kura_db_counting_init_repo:reset()
    end.

query(?MODULE, counted_pool, _SQL, _Params, _Opts) ->
    #{rows => [], num_rows => 0}.

a_failed_start_on_a_new_pool_stops_queries_reaching_the_old_one_test() ->
    with_pool_answer({ok, self()}, #{pool => old_pool}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        application:set_env(kura, repos, #{
            kura_db_failing_repo => #{pool => new_pool, pool_module => ?MODULE}
        }),
        put({?MODULE, pool_answer}, {error, econnrefused}),
        _ = with_captured_logs(fun() ->
            ?assertEqual({error, econnrefused}, kura_repo_worker:start(kura_db_failing_repo))
        end),
        ?assertError({pool_not_started, new_pool}, kura_db:get_pool(kura_db_failing_repo)),
        put({?MODULE, pool_answer}, {ok, self()}),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        ?assertEqual(new_pool, kura_db:get_pool(kura_db_failing_repo))
    end).

a_raised_start_on_a_new_pool_stops_queries_reaching_the_old_one_test() ->
    with_pool_answer({ok, self()}, #{pool => old_pool}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        application:set_env(kura, repos, #{
            kura_db_failing_repo => #{pool => new_pool, pool_module => ?MODULE}
        }),
        put({?MODULE, pool_answer}, {raise, boom}),
        _ = with_captured_logs(fun() ->
            ?assertError({pool_start_failed, _, _}, kura_repo_worker:start(kura_db_failing_repo))
        end),
        ?assertError({pool_not_started, new_pool}, kura_db:get_pool(kura_db_failing_repo))
    end).

a_failed_restart_on_the_same_pool_keeps_the_settings_test() ->
    with_pool_answer({ok, self()}, #{pool => old_pool}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        put({?MODULE, pool_answer}, {error, econnrefused}),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_failing_repo) end),
        ?assertEqual(old_pool, kura_db:get_pool(kura_db_failing_repo))
    end).

a_restart_where_only_init_fails_keeps_the_settings_test() ->
    with_counting_repo(fun(Repo) ->
        ?assertEqual(ok, kura_repo_worker:start(Repo)),
        _ = with_captured_logs(fun() ->
            ?assertMatch({error, {init_failed, _}}, kura_repo_worker:start(Repo))
        end),
        ?assertEqual(counted_pool, kura_db:get_pool(Repo))
    end).

%%----------------------------------------------------------------------
%% A pool belongs to the repo that started it (#201)
%%----------------------------------------------------------------------

a_second_repo_naming_a_running_pool_is_refused_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        Events = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end),
        ?assertMatch(
            [
                #{
                    level := error,
                    msg :=
                        {report, #{
                            msg := ~"kura: pool did not start",
                            repo := kura_db_other_repo,
                            pool := shared_pool,
                            reason := {pool_owned_by, kura_db_owner_repo}
                        }}
                }
            ],
            Events
        ),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_owner_repo))
    end).

a_refused_repo_does_not_reach_the_pool_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_other_repo) end),
        ?assertError({pool_owned_by, kura_db_owner_repo}, kura_db:get_pool(kura_db_other_repo)),
        ?assertError(
            {pool_owned_by, kura_db_owner_repo}, kura_db:query(kura_db_other_repo, ~"SELECT 1", [])
        )
    end).

boot_logs_a_refused_pool_and_carries_on_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_app:start_repo_pool(kura_db_owner_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        Events = with_captured_logs(fun() ->
            ?assertEqual(ok, kura_app:start_repo_pool(kura_db_other_repo))
        end),
        ?assertMatch(
            [#{msg := {report, #{reason := {pool_owned_by, kura_db_owner_repo}}}}], Events
        )
    end).

the_owner_starting_its_pool_again_is_ok_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        Events = with_captured_logs(fun() ->
            ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo))
        end),
        ?assertEqual([], Events)
    end).

a_running_pool_kura_did_not_start_goes_to_the_first_repo_test() ->
    with_two_repos(fun() ->
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        _ = with_captured_logs(fun() ->
            ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo))
        end),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end)
    end).

a_pool_module_answering_ok_for_a_running_name_does_not_move_the_owner_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        erase({?MODULE, pool_name}),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end),
        ?assertEqual(undefined, get({?MODULE, pool_name})),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_owner_repo)),
        ?assertError({pool_owned_by, kura_db_owner_repo}, kura_db:get_pool(kura_db_other_repo))
    end).

a_pool_stopped_outside_kura_stays_owned_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ok = stop_pool(shared_pool),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo))
    end).

stop_pool_stops_the_pool_and_releases_it_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_owner_repo)),
        ?assertEqual(shared_pool, get({?MODULE, stopped})),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo)),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_other_repo))
    end).

stop_pool_drops_the_stored_settings_test() ->
    with_pool_answer({ok, self()}, #{read_only => false}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        application:set_env(kura, repos, #{
            kura_db_failing_repo => #{read_only => true, pool_module => ?MODULE}
        }),
        ?assertEqual(false, kura_repo:read_only(kura_db_failing_repo)),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_failing_repo)),
        ?assertEqual(true, kura_repo:read_only(kura_db_failing_repo))
    end).

stop_pool_of_a_repo_kura_did_not_start_stops_nothing_test() ->
    with_pool_answer({ok, self()}, fun() ->
        ?assertEqual(ok, kura_db:stop_pool(kura_db_failing_repo)),
        ?assertEqual(undefined, get({?MODULE, stopped}))
    end).

stop_pool_of_a_refused_repo_leaves_the_owner_pool_running_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_other_repo) end),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_other_repo)),
        ?assertEqual(undefined, get({?MODULE, stopped})),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_owner_repo)),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end)
    end).

a_refused_repo_reaches_its_own_pool_once_started_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        put({?MODULE, pool_answer}, {error, {already_started, self()}}),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_other_repo) end),
        application:set_env(kura, repos, #{
            kura_db_other_repo => #{pool => own_pool, pool_module => ?MODULE}
        }),
        put({?MODULE, pool_answer}, {ok, self()}),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo)),
        ?assertEqual(own_pool, kura_db:get_pool(kura_db_other_repo))
    end).

concurrent_starts_of_one_pool_name_leave_one_owner_test() ->
    Repos = [list_to_atom("kura_db_racing_repo_" ++ integer_to_list(N)) || N <- lists:seq(1, 5)],
    Config = #{pool => raced_pool, pool_module => ?MODULE, answer_after => 20},
    kura_db:forget_pools(),
    application:set_env(kura, repos, #{Repo => Config || Repo <- Repos}),
    Self = self(),
    try
        _ = with_captured_logs(fun() ->
            Racers = [
                {Repo, spawn_monitor(fun() -> Self ! {Repo, kura_repo_worker:start(Repo)} end)}
             || Repo <- Repos
            ],
            put({?MODULE, raced}, [raced(Repo, Monitor) || {Repo, {_Pid, Monitor}} <- Racers])
        end),
        Results = erase({?MODULE, raced}),
        [Owner] = [Repo || {Repo, ok} <- Results],
        ?assertEqual(
            [{pool_owned_by, Owner} || _ <- tl(Repos)],
            [Reason || {_Repo, {error, Reason}} <- Results]
        )
    after
        application:unset_env(kura, repos),
        kura_db:forget_pools()
    end.

raced(Repo, Monitor) ->
    receive
        {Repo, Result} ->
            erlang:demonitor(Monitor, [flush]),
            {Repo, Result};
        {'DOWN', Monitor, process, _Pid, Reason} when Reason =/= normal ->
            error({racer_crashed, Repo, Reason})
    after 5000 -> {Repo, timeout}
    end.

a_repo_started_on_a_new_pool_stops_and_releases_the_old_one_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_other_repo) end),
        application:set_env(kura, repos, #{
            kura_db_owner_repo => #{pool => moved_pool, pool_module => ?MODULE},
            kura_db_other_repo => #{pool => shared_pool, pool_module => ?MODULE, user => "other"}
        }),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ?assertEqual(shared_pool, get({?MODULE, stopped})),
        ?assertEqual(moved_pool, kura_db:get_pool(kura_db_owner_repo)),
        erase({?MODULE, pool_config}),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo)),
        ?assertEqual(#{user => "other"}, get({?MODULE, pool_config})),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_other_repo))
    end).

an_old_pool_that_does_not_stop_stays_owned_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        application:set_env(kura, repos, #{
            kura_db_owner_repo => #{pool => moved_pool, pool_module => ?MODULE},
            kura_db_other_repo => #{pool => shared_pool, pool_module => ?MODULE}
        }),
        put({?MODULE, stop_answer}, {raise, stuck}),
        ?assertError(stuck, kura_repo_worker:start(kura_db_owner_repo)),
        erase({?MODULE, stop_answer}),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end)
    end).

stop_pool_after_a_failed_move_stops_the_pool_the_repo_owns_test() ->
    with_pool_answer({ok, self()}, #{pool => old_pool}, fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_failing_repo)),
        application:set_env(kura, repos, #{
            kura_db_failing_repo => #{pool => new_pool, pool_module => ?MODULE}
        }),
        put({?MODULE, pool_answer}, {error, econnrefused}),
        _ = with_captured_logs(fun() -> kura_repo_worker:start(kura_db_failing_repo) end),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_failing_repo)),
        ?assertEqual(old_pool, get({?MODULE, stopped}))
    end).

a_repo_kura_did_not_start_cannot_query_through_another_repos_pool_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ?assertError({pool_owned_by, kura_db_owner_repo}, kura_db:get_pool(kura_db_other_repo)),
        ?assertError(
            {pool_owned_by, kura_db_owner_repo}, kura_db:query(kura_db_other_repo, ~"SELECT 1", [])
        ),
        ?assertError(
            {pool_owned_by, kura_db_owner_repo},
            kura_db:transaction(kura_db_other_repo, fun() -> ok end)
        )
    end).

a_stopped_repo_cannot_query_the_pool_another_repo_started_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_owner_repo)),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo)),
        ?assertError({pool_owned_by, kura_db_other_repo}, kura_db:get_pool(kura_db_owner_repo))
    end).

a_stop_between_a_move_and_its_retire_still_frees_the_old_pool_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ok = moved_to(moved_pool),
        Self = self(),
        holding_lock(shared_pool, fun(Release) ->
            {_Mover, Moving} = spawn_monitor(fun() ->
                Self ! {moved, kura_repo_worker:start(kura_db_owner_repo)}
            end),
            ok = until(fun() -> kura_db:get_pool(kura_db_owner_repo) =:= moved_pool end),
            {_Stopper, Stopping} = spawn_monitor(fun() ->
                ok = kura_db:stop_pool(kura_db_owner_repo)
            end),
            ok = until(fun() -> kura_db:held(kura_db_owner_repo) =:= none end),
            ok = Release(),
            ?assertEqual(
                ok,
                receive
                    {moved, Moved} -> Moved
                after 5000 -> timeout
                end
            ),
            ok = exited(Moving),
            ok = exited(Stopping)
        end),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo)),
        ?assertEqual(shared_pool, kura_db:get_pool(kura_db_other_repo))
    end).

a_move_whose_caller_dies_before_the_retire_leaves_the_old_pool_stoppable_test() ->
    with_two_repos(fun() ->
        ?assertEqual(ok, kura_repo_worker:start(kura_db_owner_repo)),
        ok = moved_to(moved_pool),
        holding_lock(shared_pool, fun(Release) ->
            {Mover, Moving} = spawn_monitor(fun() -> kura_repo_worker:start(kura_db_owner_repo) end),
            ok = until(fun() -> kura_db:get_pool(kura_db_owner_repo) =:= moved_pool end),
            exit(Mover, kill),
            receive
                {'DOWN', Moving, process, Mover, killed} -> ok
            end,
            Release()
        end),
        _ = with_captured_logs(fun() ->
            ?assertEqual(
                {error, {pool_owned_by, kura_db_owner_repo}},
                kura_repo_worker:start(kura_db_other_repo)
            )
        end),
        ?assertEqual(ok, kura_db:stop_pool(kura_db_owner_repo)),
        ?assertEqual(ok, kura_repo_worker:start(kura_db_other_repo))
    end).

%% Hold the lock kura_db takes on a pool name, from this process, so another
%% process's start or stop of it waits until Release() is called.
holding_lock(Pool, Run) ->
    Lock = {{kura_pool_start, Pool}, self()},
    true = global:set_lock(Lock, [node()]),
    Release = fun() ->
        true = global:del_lock(Lock, [node()]),
        ok
    end,
    try
        Run(Release)
    after
        Release()
    end.

moved_to(Pool) ->
    application:set_env(kura, repos, #{
        kura_db_owner_repo => #{pool => Pool, pool_module => ?MODULE},
        kura_db_other_repo => #{pool => shared_pool, pool_module => ?MODULE}
    }).

until(Done) ->
    until(Done, 500).

until(Done, 0) ->
    error({never, Done()});
until(Done, Tries) ->
    case Done() of
        true ->
            ok;
        false ->
            timer:sleep(5),
            until(Done, Tries - 1)
    end.

exited(Monitor) ->
    receive
        {'DOWN', Monitor, process, _Pid, normal} -> ok;
        {'DOWN', Monitor, process, _Pid, Reason} -> error({crashed, Reason})
    after 5000 -> error(timeout)
    end.

with_two_repos(Run) ->
    Shared = #{pool => shared_pool, pool_module => ?MODULE},
    with_pool_answer({ok, self()}, fun() ->
        application:set_env(kura, repos, #{
            kura_db_owner_repo => Shared, kura_db_other_repo => Shared
        }),
        Run()
    end).

stop_pool(Pool) ->
    case get({?MODULE, stop_answer}) of
        {raise, Reason} ->
            error(Reason);
        {start, Repo} ->
            erase({?MODULE, stop_answer}),
            ok = kura_repo_worker:start(Repo);
        undefined ->
            ok
    end,
    put({?MODULE, stopped}, Pool),
    ok.

start_pool(Pool, Config) ->
    put({?MODULE, pool_name}, Pool),
    put({?MODULE, pool_config}, Config),
    case get({?MODULE, pool_answer}) of
        undefined -> answer_after(maps:get(answer_after, Config, 0));
        {raise, Reason} -> error(Reason, [Pool, Config]);
        Answer -> Answer
    end.

%% A start from another process, as a pool module that answers {ok, Pid}
%% for a name already running would.
answer_after(Millis) ->
    timer:sleep(Millis),
    {ok, self()}.

log(#{meta := #{pid := Pid}} = Event, #{config := #{collector := Collector, pid := Pid}}) ->
    Collector ! {logged, Event};
log(_Event, _Config) ->
    ok.

with_pool_answer(Answer, Run) ->
    with_pool_answer(Answer, #{}, Run).

with_pool_answer(Answer, Config, Run) ->
    with_pool_answer(Answer, kura_db_failing_repo, Config, Run).

with_pool_answer(Answer, Repo, Config, Run) ->
    kura_db:forget_pools(),
    application:set_env(kura, repos, #{Repo => Config#{pool_module => ?MODULE}}),
    put({?MODULE, pool_answer}, Answer),
    try
        Run()
    after
        erase({?MODULE, pool_answer}),
        erase({?MODULE, pool_name}),
        erase({?MODULE, pool_config}),
        erase({?MODULE, stopped}),
        erase({?MODULE, stop_answer}),
        application:unset_env(kura, repos),
        kura_db:forget_pools()
    end.

logged_report(Answer, Config) ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(Answer, Config, fun() ->
            ?assertMatch(
                {error, _},
                kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config#{
                    pool_module => ?MODULE
                })
            )
        end)
    end),
    [#{msg := {report, Report}}] = Events,
    lists:flatten(io_lib:format("~p", [Report])).

logged_reason(Answer, Config) ->
    Events = with_captured_logs(fun() ->
        with_pool_answer(Answer, Config, fun() ->
            ?assertMatch(
                {error, _},
                kura_db:start_pool(kura_db_failing_repo, kura_db_failing_repo, Config#{
                    pool_module => ?MODULE
                })
            )
        end)
    end),
    [#{msg := {report, #{reason := Reason}}}] = Events,
    Reason.

with_captured_logs(Run) ->
    Config = #{config => #{collector => self(), pid => self()}, level => all},
    _ = logger:remove_handler(kura_db_start_pool_tests_capture),
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
