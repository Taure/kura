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
                fun t_get_by_binds_unary_atoms/0},
            {"text matches treat wildcards as text", fun t_text_match_is_literal/0},
            {"text matches place the wildcard", fun t_text_match_position/0},
            {"case-insensitive text matches", fun t_text_match_ignores_case/0},
            {"escape_like makes a like pattern literal", fun t_escape_like/0}
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
    {ok, Rows} = kura_test_repo:all(Query),
    lists:sort([Name || #{name := Name} <- Rows]).

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

t_text_match_is_literal() ->
    ok = reset(),
    [{ok, _} = insert_item(#{name => N}) || N <- [~"50% off", ~"500 off", ~"a_b", ~"axb", ~"a!b"]],
    From = kura_query:from(kura_test_edge_schema),
    ?assertEqual([~"50% off"], names(kura_query:where(From, {name, contains, ~"0%"}))),
    ?assertEqual([~"a_b"], names(kura_query:where(From, {name, contains, ~"a_b"}))),
    ?assertEqual([~"a!b"], names(kura_query:where(From, {name, contains, ~"!"}))),
    ?assertEqual([], names(kura_query:where(From, {name, contains, ~"%%"}))),
    ?assertEqual(5, length(names(kura_query:where(From, {name, contains, ~""})))),
    ?assertEqual(
        [~"50% off", ~"500 off"], names(kura_query:where(From, {name, like, ~"50% off"}))
    ).

t_text_match_position() ->
    ok = reset(),
    [{ok, _} = insert_item(#{name => N}) || N <- [~"abc", ~"bca", ~"cab"]],
    From = kura_query:from(kura_test_edge_schema),
    ?assertEqual([~"abc"], names(kura_query:where(From, {name, starts_with, ~"ab"}))),
    ?assertEqual([~"cab"], names(kura_query:where(From, {name, ends_with, ~"ab"}))),
    ?assertEqual([~"abc", ~"cab"], names(kura_query:where(From, {name, contains, ~"ab"}))),
    ?assertEqual([~"bca"], names(kura_query:where(From, {'not', {name, contains, ~"ab"}}))).

t_text_match_ignores_case() ->
    ok = reset(),
    [{ok, _} = insert_item(#{name => N}) || N <- [~"Alice_1", ~"ALICEx1", ~"bob"]],
    From = kura_query:from(kura_test_edge_schema),
    ?assertEqual([], names(kura_query:where(From, {name, starts_with, ~"alice_"}))),
    ?assertEqual([~"Alice_1"], names(kura_query:where(From, {name, istarts_with, ~"alice_"}))),
    ?assertEqual([~"Alice_1"], names(kura_query:where(From, {name, icontains, ~"E_1"}))),
    ?assertEqual([~"Alice_1"], names(kura_query:where(From, {name, iends_with, ~"e_1"}))).

t_escape_like() ->
    ok = reset(),
    [{ok, _} = insert_item(#{name => N}) || N <- [~"100%", ~"1000", ~"a\\b", ~"axb"]],
    From = kura_query:from(kura_test_edge_schema),
    Pattern = fun(Text) -> <<(kura_query:escape_like(Text))/binary, "%">> end,
    ?assertEqual([~"100%"], names(kura_query:where(From, {name, like, Pattern(~"100%")}))),
    ?assertEqual([~"a\\b"], names(kura_query:where(From, {name, like, Pattern(~"a\\")}))),
    ?assertEqual([~"100%", ~"1000"], names(kura_query:where(From, {name, like, ~"100%"}))).
