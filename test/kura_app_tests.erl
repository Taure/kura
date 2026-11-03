-module(kura_app_tests).
-include_lib("eunit/include/eunit.hrl").

stop_returns_ok_test() ->
    ?assertEqual(ok, kura_app:stop(undefined)).

stop_with_any_state_test() ->
    ?assertEqual(ok, kura_app:stop(some_state)).

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
