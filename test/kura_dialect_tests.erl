-module(kura_dialect_tests).
-moduledoc "What kura_dialect asks of a dialect module that may or may not answer.".

-include_lib("eunit/include/eunit.hrl").

-define(REPO, kura_dialect_tests_repo).
-define(OTHER, kura_test_sqlite_dialect).

unloaded(Module) ->
    _ = code:purge(Module),
    _ = code:delete(Module),
    _ = code:purge(Module),
    ?assertNot(erlang:module_loaded(Module)),
    Module.

placeholder_test_() ->
    Own = iolist_to_binary(?OTHER:placeholder(3)),
    [
        ?_assertNotEqual(~"$3", Own),
        ?_assertEqual(~"$3", iolist_to_binary(kura_dialect:placeholder(undefined, 3))),
        ?_assertEqual(~"$3", iolist_to_binary(kura_dialect:placeholder(kura_dialect_pg, 3))),
        {"a module with no placeholder/1", [
            ?_assertEqual(~"$3", iolist_to_binary(kura_dialect:placeholder(lists, 3))),
            ?_assertEqual(~"$3", iolist_to_binary(kura_dialect:placeholder(no_such_dialect, 3)))
        ]},
        ?_assertEqual(Own, iolist_to_binary(kura_dialect:placeholder(?OTHER, 3))),
        {"a dialect that is not loaded yet",
            ?_assertEqual(Own, iolist_to_binary(kura_dialect:placeholder(unloaded(?OTHER), 3)))}
    ].

loads_stored_test_() ->
    [
        ?_assertNot(kura_dialect:loads_stored(undefined)),
        ?_assertNot(kura_dialect:loads_stored(kura_dialect_pg)),
        ?_assertNot(kura_dialect:loads_stored(no_such_dialect)),
        ?_assert(kura_dialect:loads_stored(?OTHER)),
        {"a dialect that is not loaded yet", [
            ?_assert(kura_dialect:loads_stored(unloaded(?OTHER))),
            ?_assertNot(kura_dialect:loads_stored(unloaded(kura_test_edge_schema)))
        ]}
    ].

of_repo_test_() ->
    {foreach, fun() -> application:unset_env(kura, dialect) end,
        fun(_) ->
            application:unset_env(kura, dialect),
            application:unset_env(kura, repos)
        end,
        [
            {"the repo's own dialect", fun() ->
                configure(#{dialect => ?OTHER}),
                application:set_env(kura, dialect, kura_dialect_pg),
                ?assertEqual(?OTHER, kura_dialect:of_repo(?REPO))
            end},
            {"the application environment's, for a repo that names none", fun() ->
                configure(#{}),
                ?assertEqual(undefined, kura_dialect:of_repo(?REPO)),
                application:set_env(kura, dialect, ?OTHER),
                ?assertEqual(?OTHER, kura_dialect:of_repo(?REPO))
            end},
            {"neither, when the environment's is not a module name", fun() ->
                configure(#{}),
                application:set_env(kura, dialect, ~"kura_dialect_pg"),
                ?assertEqual(undefined, kura_dialect:of_repo(?REPO))
            end}
        ]}.

configure(Extra) ->
    application:set_env(kura, repos, #{?REPO => Extra#{pool => kura_dialect_tests_pool}}).
