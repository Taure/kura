-module(kura_join_integration_tests).
-moduledoc "Joins of tables that share column names, against PostgreSQL.".

-include_lib("eunit/include/eunit.hrl").

-define(AUTHOR, kura_test_join_author_schema).
-define(BOOK, kura_test_join_book_schema).
-define(REVIEW, kura_test_join_review_schema).

join_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"a join loads the from row, not the joined one", fun t_join_loads_the_from_row/0},
            {"a left join without a match still loads the from row",
                fun t_left_join_without_a_match/0},
            {"a join from a bare table returns that table's columns",
                fun t_join_from_a_bare_table/0},
            {"a condition on a joined schema's renamed field", fun t_where_on_a_joined_rename/0},
            {"an order on a joined schema's renamed field", fun t_order_on_a_joined_rename/0},
            {"a select of a joined schema's renamed field is keyed by the field",
                fun t_select_a_joined_rename/0},
            {"a group and an aggregate on a joined schema's renamed field",
                fun t_group_on_a_joined_rename/0},
            {"a join on a renamed column from a joined schema to the next",
                fun t_chained_join_on_renames/0},
            {"an order and a limit on a union apply to the union", fun t_union_order_and_limit/0},
            {"a union branch keeps its own order and limit", fun t_union_branch_limit/0},
            {"an order and an offset on an intersect and an except",
                fun t_intersect_and_except_order/0}
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    kura_test_repo:start(),
    ok = drop_tables(),
    {ok, _} = kura_test_repo:query(
        ~"""
        CREATE TABLE join_authors (
          id BIGSERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          note VARCHAR(255)
        )
        """,
        []
    ),
    {ok, _} = kura_test_repo:query(
        ~"""
        CREATE TABLE join_books (
          id BIGSERIAL PRIMARY KEY,
          written_by BIGINT NOT NULL,
          name VARCHAR(255) NOT NULL,
          book_rating INTEGER,
          note VARCHAR(255)
        )
        """,
        []
    ),
    {ok, _} = kura_test_repo:query(
        ~"""
        CREATE TABLE join_reviews (
          id BIGSERIAL PRIMARY KEY,
          book_id BIGINT NOT NULL,
          stars INTEGER
        )
        """,
        []
    ),
    {ok, Ann} = insert(?AUTHOR, #{name => ~"Ann", note => ~"author note"}),
    {ok, Bob} = insert(?AUTHOR, #{name => ~"Bob"}),
    {ok, _} = insert(?AUTHOR, #{name => ~"Cy"}),
    {ok, _} = insert(?BOOK, #{author_id => id(Bob), name => ~"Bees", rating => 2}),
    {ok, Birds} = insert(?BOOK, #{author_id => id(Bob), name => ~"Birds", rating => 5}),
    {ok, Ants} = insert(?BOOK, #{
        author_id => id(Ann), name => ~"Ants", rating => 4, note => ~"book note"
    }),
    {ok, _} = insert(?REVIEW, #{book_id => id(Birds), rating => 1}),
    {ok, _} = insert(?REVIEW, #{book_id => id(Ants), rating => 3}),
    ok.

teardown(_) ->
    drop_tables().

drop_tables() ->
    {ok, _} = kura_test_repo:query(
        ~"DROP TABLE IF EXISTS join_reviews, join_books, join_authors", []
    ),
    ok.

insert(Schema, Params) ->
    kura_test_repo:insert(kura_changeset:cast(Schema, #{}, Params, maps:keys(Params))).

id(#{id := Id}) -> Id.

books_of_authors() ->
    kura_query:join(kura_query:from(?AUTHOR), inner, ?BOOK, {id, author_id}).

t_join_loads_the_from_row() ->
    Q = kura_query:order_by(books_of_authors(), [{name, asc}]),
    {ok, Rows} = kura_test_repo:all(Q),
    ?assertEqual(
        [{1, ~"Ann", ~"author note"}, {2, ~"Bob", undefined}, {2, ~"Bob", undefined}],
        [{Id, Name, Note} || #{id := Id, name := Name, note := Note} <- Rows]
    ),
    ?assertEqual([[id, name, note]], lists:usort([lists:sort(maps:keys(Row)) || Row <- Rows])).

t_left_join_without_a_match() ->
    Q0 = kura_query:join(kura_query:from(?AUTHOR), left, ?BOOK, {id, author_id}),
    {ok, Rows} = kura_test_repo:all(kura_query:where(Q0, {name, ~"Cy"})),
    ?assertMatch([#{id := 3, name := ~"Cy"}], Rows).

t_join_from_a_bare_table() ->
    Q0 = kura_query:join(kura_query:from(join_authors), inner, ?BOOK, {id, author_id}),
    {ok, Rows} = kura_test_repo:all(kura_query:where(Q0, {book_rating, 4})),
    ?assertEqual([#{id => 1, name => ~"Ann", note => ~"author note"}], Rows).

t_where_on_a_joined_rename() ->
    Q = kura_query:where(books_of_authors(), {rating, '>', 3}),
    {ok, Rows} = kura_test_repo:all(Q),
    ?assertEqual([~"Ann", ~"Bob"], lists:sort([Name || #{name := Name} <- Rows])).

t_order_on_a_joined_rename() ->
    Q = kura_query:order_by(books_of_authors(), [{rating, desc}]),
    {ok, Rows} = kura_test_repo:all(Q),
    ?assertEqual([~"Bob", ~"Ann", ~"Bob"], [Name || #{name := Name} <- Rows]).

t_select_a_joined_rename() ->
    Q0 = kura_query:select(books_of_authors(), [name, rating]),
    {ok, Rows} = kura_test_repo:all(kura_query:order_by(Q0, [{rating, asc}])),
    ?assertEqual(
        [
            #{name => ~"Bob", rating => 2},
            #{name => ~"Ann", rating => 4},
            #{name => ~"Bob", rating => 5}
        ],
        Rows
    ).

t_group_on_a_joined_rename() ->
    Q0 = kura_query:group_by(books_of_authors(), [name]),
    Q1 = kura_query:select(Q0, [name, {max, rating}]),
    {ok, Rows} = kura_test_repo:all(kura_query:order_by(Q1, [{name, asc}])),
    ?assertEqual([#{name => ~"Ann", max => 4}, #{name => ~"Bob", max => 5}], Rows).

t_chained_join_on_renames() ->
    Q0 = kura_query:join(books_of_authors(), inner, ?REVIEW, {id, book_id}),
    {ok, Rows} = kura_test_repo:all(kura_query:order_by(Q0, [{name, asc}])),
    ?assertEqual([~"Ann", ~"Bob"], [Name || #{name := Name} <- Rows]),
    ?assertError(
        {kura, {ambiguous_field, rating, [~"book_rating", ~"stars"]}},
        kura_test_repo:all(kura_query:where(Q0, {rating, 3}))
    ).

authors(Names) ->
    kura_query:where(kura_query:from(?AUTHOR), {name, in, Names}).

author_names(Query) ->
    {ok, Rows} = kura_test_repo:all(Query),
    [Name || #{name := Name} <- Rows].

t_union_order_and_limit() ->
    Union = kura_query:union(authors([~"Cy"]), authors([~"Ann", ~"Bob"])),
    Ordered = kura_query:order_by(Union, [{name, desc}]),
    ?assertEqual([~"Cy", ~"Bob", ~"Ann"], author_names(Ordered)),
    ?assertEqual([~"Cy", ~"Bob"], author_names(kura_query:limit(Ordered, 2))),
    ?assertEqual([~"Bob"], author_names(kura_query:offset(kura_query:limit(Ordered, 1), 1))),
    ?assertEqual(
        [~"Ann", ~"Bob", ~"Cy"],
        author_names(
            kura_query:union_all(
                kura_query:order_by(authors([~"Cy"]), [{name, asc}]), authors([~"Ann", ~"Bob"])
            )
        )
    ).

t_union_branch_limit() ->
    Branch = kura_query:limit(
        kura_query:order_by(authors([~"Ann", ~"Bob"]), [{name, desc}]), 1
    ),
    Union = kura_query:order_by(kura_query:union(authors([~"Cy"]), Branch), [{name, asc}]),
    ?assertEqual([~"Bob", ~"Cy"], author_names(Union)).

t_intersect_and_except_order() ->
    All = authors([~"Ann", ~"Bob", ~"Cy"]),
    Intersect = kura_query:intersect(All, authors([~"Bob", ~"Cy"])),
    ?assertEqual(
        [~"Cy", ~"Bob"], author_names(kura_query:order_by(Intersect, [{name, desc}]))
    ),
    Except = kura_query:except(All, authors([~"Bob"])),
    ?assertEqual(
        [~"Cy"],
        author_names(kura_query:offset(kura_query:order_by(Except, [{name, asc}]), 1))
    ).
