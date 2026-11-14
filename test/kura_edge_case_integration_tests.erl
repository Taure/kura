-module(kura_edge_case_integration_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

edge_case_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"empty in matches no row", fun t_empty_in/0},
            {"empty not_in matches every row, null included", fun t_empty_not_in/0},
            {"empty in deletes nothing", fun t_empty_in_delete_all/0},
            {"get_by compares with a value that spells an operator",
                fun t_get_by_binds_unary_atoms/0}
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    kura_test_repo:start(),
    ok = drop_tables(),
    {ok, _} = kura_test_repo:query(
        ~"""
        CREATE TABLE edge_items (
          id BIGSERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          note VARCHAR(255),
          qty INTEGER
        )
        """,
        []
    ),
    ok.

teardown(_) ->
    drop_tables().

drop_tables() ->
    {ok, _} = kura_test_repo:query(~"DROP TABLE IF EXISTS edge_items", []),
    ok.

reset() ->
    {ok, _} = kura_test_repo:delete_all(kura_query:from(kura_test_edge_schema)),
    ok.

insert_item(Params) ->
    kura_test_repo:insert(
        kura_changeset:cast(kura_test_edge_schema, #{}, Params, [name, note, qty])
    ).

names(Query) ->
    {ok, Rows} = kura_test_repo:all(kura_query:order_by(Query, [{name, asc}])),
    [Name || #{name := Name} <- Rows].

t_empty_in() ->
    ok = reset(),
    {ok, _} = insert_item(#{name => ~"a", qty => 1}),
    Q = kura_query:from(kura_test_edge_schema),
    ?assertEqual([], names(kura_query:where(Q, {qty, in, []}))),
    ?assertEqual([], names(kura_query:where(Q, {[name, qty], in, []}))),
    ?assertEqual([~"a"], names(kura_query:where(Q, {'not', {qty, in, []}}))).

t_empty_not_in() ->
    ok = reset(),
    {ok, _} = insert_item(#{name => ~"a", qty => 1}),
    {ok, _} = insert_item(#{name => ~"b"}),
    Q = kura_query:from(kura_test_edge_schema),
    ?assertEqual([~"a", ~"b"], names(kura_query:where(Q, {qty, not_in, []}))),
    ?assertEqual([~"a"], names(kura_query:where(Q, {qty, not_in, [2]}))).

t_empty_in_delete_all() ->
    ok = reset(),
    {ok, _} = insert_item(#{name => ~"a", qty => 1}),
    Q = kura_query:where(kura_query:from(kura_test_edge_schema), {qty, in, []}),
    ?assertEqual({ok, 0}, kura_test_repo:delete_all(Q)),
    ?assertEqual([~"a"], names(kura_query:from(kura_test_edge_schema))).

t_get_by_binds_unary_atoms() ->
    ok = reset(),
    {ok, _} = insert_item(#{name => ~"a", note => ~"set"}),
    From = kura_query:from(kura_test_edge_schema),
    ?assertEqual([~"a"], names(kura_query:where(From, {note, is_not_nil}))),
    ?assertEqual(
        {error, not_found}, kura_test_repo:get_by(kura_test_edge_schema, [{note, ~"is_not_nil"}])
    ),
    {ok, _} = insert_item(#{name => ~"b", note => ~"is_not_nil"}),
    ?assertMatch(
        {ok, #{name := ~"b"}},
        kura_test_repo:get_by(kura_test_edge_schema, [{note, ~"is_not_nil"}])
    ).
