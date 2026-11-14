-module(kura_repo_worker_statement_tests).
-moduledoc "The statements kura_repo_worker builds, read through a recording driver.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_statement_test_repo).

statement_test_() ->
    {foreach, fun() -> kura_test_recording_driver:configure(?REPO) end,
        fun(_) -> kura_test_recording_driver:unconfigure() end, [
            {"get binds is_nil as a key value", fun t_get_binds_unary_atoms/0},
            {"get_by binds is_nil and is_not_nil as values", fun t_get_by_binds_unary_atoms/0},
            {"reload binds is_not_nil as a key value", fun t_reload_binds_unary_atoms/0}
        ]}.

statements(Run) ->
    _ = Run(),
    kura_test_recording_driver:statements().

t_get_binds_unary_atoms() ->
    ?assertEqual(
        [{~"SELECT * FROM \"edge_items\" WHERE \"id\" = $1", [is_nil]}],
        statements(fun() -> kura_repo_worker:get(?REPO, kura_test_edge_schema, is_nil) end)
    ).

t_get_by_binds_unary_atoms() ->
    [
        ?assertEqual(
            [{~"SELECT * FROM \"edge_items\" WHERE \"note\" = $1 AND \"qty\" = $2", [Atom, 1]}],
            statements(fun() ->
                kura_repo_worker:get_by(?REPO, kura_test_edge_schema, [{note, Atom}, {qty, 1}])
            end)
        )
     || Atom <- [is_nil, is_not_nil]
    ].

t_reload_binds_unary_atoms() ->
    ?assertEqual(
        [{~"SELECT * FROM \"edge_items\" WHERE \"id\" = $1", [is_not_nil]}],
        statements(fun() ->
            kura_repo_worker:reload(?REPO, kura_test_edge_schema, #{id => is_not_nil})
        end)
    ).
