-module(kura_app_tests).
-include_lib("eunit/include/eunit.hrl").

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
