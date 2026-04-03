-module(kura_app_tests).
-include_lib("eunit/include/eunit.hrl").

stop_returns_ok_test() ->
    ?assertEqual(ok, kura_app:stop(undefined)).

stop_with_any_state_test() ->
    ?assertEqual(ok, kura_app:stop(some_state)).

pool_config_defaults_test() ->
    clear_kura_env(),
    Config = kura_app:pool_config(),
    ?assertEqual("localhost", maps:get(host, Config)),
    ?assertEqual(5432, maps:get(port, Config)),
    ?assertEqual("postgres", maps:get(database, Config)),
    ?assertEqual("postgres", maps:get(user, Config)),
    ?assertEqual("", maps:get(password, Config)),
    ?assertEqual(10, maps:get(pool_size, Config)),
    ?assertNot(maps:is_key(socket_options, Config)).

pool_config_custom_values_test() ->
    clear_kura_env(),
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
    application:set_env(kura, socket_options, [inet6]),
    Config = kura_app:pool_config(),
    ?assertEqual([inet6], maps:get(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_multiple_test() ->
    clear_kura_env(),
    application:set_env(kura, socket_options, [inet6, {recbuf, 8192}]),
    Config = kura_app:pool_config(),
    ?assertEqual([inet6, {recbuf, 8192}], maps:get(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_empty_list_test() ->
    clear_kura_env(),
    application:set_env(kura, socket_options, []),
    Config = kura_app:pool_config(),
    ?assertNot(maps:is_key(socket_options, Config)),
    clear_kura_env().

pool_config_socket_options_not_set_test() ->
    clear_kura_env(),
    Config = kura_app:pool_config(),
    ?assertNot(maps:is_key(socket_options, Config)),
    clear_kura_env().

clear_kura_env() ->
    Keys = [host, port, database, user, password, pool_size, socket_options],
    lists:foreach(fun(K) -> application:unset_env(kura, K) end, Keys).
