-module(kura_column_mapping_tests).
-moduledoc "A field stored under a column of another name, in SQL and against PostgreSQL.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(S, kura_test_renamed_schema).

%%----------------------------------------------------------------------
%% Schema lookups
%%----------------------------------------------------------------------

renamed_columns_test() ->
    ?assertEqual(
        #{id => ~"item_id", title => ~"item_title", qty => ~"quantity"},
        kura_schema:renamed_columns(?S)
    ),
    ?assertEqual(
        #{~"item_id" => id, ~"item_title" => title, ~"quantity" => qty},
        kura_schema:fields_by_column(?S)
    ).

no_renamed_columns_test_() ->
    [
        ?_assertEqual(#{}, kura_schema:renamed_columns(Mod))
     || Mod <- [kura_test_edge_schema, not_a_schema_module, lists]
    ].

%%----------------------------------------------------------------------
%% Statements
%%----------------------------------------------------------------------

select_test() ->
    Q0 = kura_query:from(?S),
    Q1 = kura_query:where(Q0, {title, '=', ~"a"}),
    Q2 = kura_query:where(Q1, {'or', [{qty, '>', 1}, {note, is_nil}]}),
    Q3 = kura_query:order_by(Q2, [{qty, desc}, {note, asc}]),
    Q4 = kura_query:select(Q3, [id, title, note]),
    ?assertEqual(
        {
            <<
                "SELECT \"item_id\", \"item_title\", \"note\" FROM \"renamed_items\" "
                "WHERE \"item_title\" = $1 AND (\"quantity\" > $2 OR \"note\" IS NULL) "
                "ORDER BY \"quantity\" DESC, \"note\" ASC"
            >>,
            [~"a", 1]
        },
        kura_dialect_pg:to_sql(Q4)
    ).

aggregate_and_group_test() ->
    Q0 = kura_query:group_by(kura_query:from(?S), [title]),
    Q1 = kura_query:select(Q0, [title, {sum, qty}]),
    ?assertEqual(
        {
            <<
                "SELECT \"item_title\", sum(\"quantity\") AS \"sum\" FROM \"renamed_items\" "
                "GROUP BY \"item_title\""
            >>,
            []
        },
        kura_dialect_pg:to_sql(Q1)
    ).

join_test() ->
    Q0 = kura_query:join(kura_query:from(?S), inner, kura_test_edge_schema, {id, qty}),
    Q1 = kura_query:where(Q0, {title, '=', ~"a"}),
    ?assertEqual(
        {
            <<
                "SELECT \"renamed_items\".* FROM \"renamed_items\" INNER JOIN \"edge_items\" "
                "ON \"renamed_items\".\"item_id\" = \"edge_items\".\"qty\" "
                "WHERE \"renamed_items\".\"item_title\" = $1"
            >>,
            [~"a"]
        },
        kura_dialect_pg:to_sql(Q1)
    ),
    Q2 = kura_query:join(kura_query:from(kura_test_edge_schema), left, ?S, {qty, id}),
    ?assertEqual(
        {
            <<
                "SELECT \"edge_items\".* FROM \"edge_items\" LEFT JOIN \"renamed_items\" "
                "ON \"edge_items\".\"qty\" = \"renamed_items\".\"item_id\""
            >>,
            []
        },
        kura_dialect_pg:to_sql(Q2)
    ).

joined_schema_renames_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_edge_schema), left, ?S, {qty, id}),
    Q1 = kura_query:where(Q0, {'or', [{title, ~"a"}, {name, ~"b"}]}),
    Q2 = kura_query:order_by(Q1, [{title, desc}]),
    Q3 = kura_query:select(Q2, [name, title, {count, title}]),
    ?assertEqual(
        {
            <<
                "SELECT \"edge_items\".\"name\", \"item_title\" AS \"title\", "
                "count(\"item_title\") AS \"count\" "
                "FROM \"edge_items\" LEFT JOIN \"renamed_items\" "
                "ON \"edge_items\".\"qty\" = \"renamed_items\".\"item_id\" "
                "WHERE (\"item_title\" = $1 OR \"edge_items\".\"name\" = $2) "
                "ORDER BY \"item_title\" DESC"
            >>,
            [~"a", ~"b"]
        },
        kura_dialect_pg:to_sql(Q3)
    ).

a_from_field_is_never_read_as_a_joined_rename_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_edge_schema), left, ?S, {qty, id}),
    ?assertEqual(
        {
            <<
                "SELECT \"edge_items\".* FROM \"edge_items\" LEFT JOIN \"renamed_items\" "
                "ON \"edge_items\".\"qty\" = \"renamed_items\".\"item_id\" "
                "WHERE \"edge_items\".\"qty\" = $1 AND \"edge_items\".\"id\" = $2"
            >>,
            [1, 2]
        },
        kura_dialect_pg:to_sql(kura_query:where(kura_query:where(Q0, {qty, 1}), {id, 2}))
    ).

joined_renames_that_disagree_test() ->
    Q0 = kura_query:join(
        kura_query:from(kura_test_join_author_schema),
        inner,
        kura_test_join_book_schema,
        {id, author_id}
    ),
    Q1 = kura_query:join(Q0, inner, kura_test_join_review_schema, {id, book_id}),
    ?assertMatch({_, []}, kura_dialect_pg:to_sql(Q1)),
    ?assertError(
        {kura, {ambiguous_field, rating, [~"book_rating", ~"stars"]}},
        kura_dialect_pg:to_sql(kura_query:order_by(Q1, [{rating, asc}]))
    ).

order_by_an_alias_that_shadows_a_field_test() ->
    Q0 = kura_query:select_expr(kura_query:from(?S), [{qty, {fragment, ~"1 + 1", []}}]),
    Q1 = kura_query:order_by(Q0, [{qty, asc}, {title, asc}]),
    {SQL, []} = kura_dialect_pg:to_sql(Q1),
    ?assertEqual(
        ~"SELECT 1 + 1 AS \"qty\" FROM \"renamed_items\" ORDER BY \"qty\" ASC, \"item_title\" ASC",
        iolist_to_binary(SQL)
    ).

write_statements_test_() ->
    [
        ?_assertEqual(
            {
                <<
                    "INSERT INTO \"renamed_items\" (\"item_title\", \"quantity\") "
                    "VALUES ($1, $2) RETURNING *"
                >>,
                [~"a", 1]
            },
            kura_dialect_pg:insert(?S, [title, qty], #{title => ~"a", qty => 1})
        ),
        ?_assertEqual(
            {
                <<
                    "INSERT INTO \"renamed_items\" (\"item_title\", \"quantity\") "
                    "VALUES ($1, $2) ON CONFLICT (\"item_title\") "
                    "DO UPDATE SET \"quantity\" = $3 RETURNING *"
                >>,
                [~"a", 1, 1]
            },
            kura_dialect_pg:insert(?S, [title, qty], #{title => ~"a", qty => 1}, #{
                on_conflict => {title, {replace, [qty]}}
            })
        ),
        ?_assertEqual(
            {
                <<
                    "UPDATE \"renamed_items\" SET \"quantity\" = $1 "
                    "WHERE \"item_id\" = $2 RETURNING *"
                >>,
                [2, 7]
            },
            kura_dialect_pg:update(?S, [qty], #{qty => 2}, [{id, 7}])
        ),
        ?_assertEqual(
            {~"DELETE FROM \"renamed_items\" WHERE \"item_id\" = $1 RETURNING *", [7]},
            kura_dialect_pg:delete(?S, [{id, 7}])
        ),
        ?_assertEqual(
            {~"UPDATE \"renamed_items\" SET \"quantity\" = $1 WHERE \"item_title\" = $2", [
                0, ~"a"
            ]},
            kura_dialect_pg:update_all(
                kura_query:where(kura_query:from(?S), {title, '=', ~"a"}), #{qty => 0}
            )
        ),
        ?_assertEqual(
            {~"DELETE FROM \"renamed_items\" WHERE \"quantity\" < $1", [1]},
            kura_dialect_pg:delete_all(kura_query:where(kura_query:from(?S), {qty, '<', 1}))
        ),
        ?_assertEqual(
            {
                <<
                    "INSERT INTO \"renamed_items\" (\"item_title\", \"quantity\") "
                    "VALUES ($1, $2), ($3, $4) "
                    "ON CONFLICT (\"item_title\") DO UPDATE SET "
                    "\"quantity\" = EXCLUDED.\"quantity\" "
                    "RETURNING \"item_id\", \"note\""
                >>,
                [~"a", 1, ~"b", 2]
            },
            kura_dialect_pg:insert_all(
                ?S,
                [title, qty],
                [#{title => ~"a", qty => 1}, #{title => ~"b", qty => 2}],
                #{on_conflict => {title, {replace, [qty]}}, returning => [id, note]}
            )
        )
    ].

%%----------------------------------------------------------------------
%% Against PostgreSQL
%%----------------------------------------------------------------------

round_trip_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"insert, get, update and delete", fun t_changeset_writes/0},
            {"queries filter, order and select by field", fun t_queries/0},
            {"bulk writes", fun t_bulk_writes/0},
            {"upsert on a renamed column", fun t_upsert/0},
            {"a NOT NULL violation names the field", fun t_not_null_names_the_field/0},
            {"cursor pagination on a renamed column", fun t_cursor_paginate/0}
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    kura_test_repo:start(),
    ok = drop_tables(),
    {ok, _} = kura_test_repo:query(
        ~"""
        CREATE TABLE renamed_items (
          item_id BIGSERIAL PRIMARY KEY,
          item_title VARCHAR(255) NOT NULL UNIQUE,
          quantity INTEGER,
          note VARCHAR(255)
        )
        """,
        []
    ),
    ok.

teardown(_) ->
    drop_tables().

drop_tables() ->
    {ok, _} = kura_test_repo:query(~"DROP TABLE IF EXISTS renamed_items", []),
    ok.

reset() ->
    {ok, _} = kura_test_repo:delete_all(kura_query:from(?S)),
    ok.

changeset(Data, Params) ->
    kura_changeset:cast(?S, Data, Params, [title, qty, note]).

t_changeset_writes() ->
    reset(),
    {ok, Row} = kura_test_repo:insert(changeset(#{}, #{title => ~"a", qty => 1})),
    ?assertMatch(#{id := Id, title := ~"a", qty := 1, note := undefined} when is_integer(Id), Row),
    ?assertEqual([id, note, qty, title], lists:sort(maps:keys(Row))),
    #{id := Id} = Row,
    ?assertEqual({ok, Row}, kura_test_repo:get(?S, Id)),
    ?assertEqual({ok, Row}, kura_test_repo:get_by(?S, [{title, ~"a"}])),
    {ok, Updated} = kura_test_repo:update(changeset(Row, #{qty => 5, note => ~"n"})),
    ?assertEqual(Row#{qty => 5, note => ~"n"}, Updated),
    ?assertEqual({ok, Updated}, kura_test_repo:reload(?S, Row)),
    {ok, _} = kura_test_repo:delete(changeset(Updated, #{})),
    ?assertEqual({error, not_found}, kura_test_repo:get(?S, Id)).

t_queries() ->
    reset(),
    {ok, 3} = kura_test_repo:insert_all(?S, [
        #{title => ~"a", qty => 3}, #{title => ~"b", qty => 1}, #{title => ~"c", qty => 2}
    ]),
    Q0 = kura_query:where(kura_query:from(?S), {qty, '>=', 2}),
    Q1 = kura_query:order_by(Q0, [{qty, asc}]),
    {ok, Rows} = kura_test_repo:all(Q1),
    ?assertEqual([{~"c", 2}, {~"a", 3}], [{T, Q} || #{title := T, qty := Q} <- Rows]),
    {ok, Selected} = kura_test_repo:all(kura_query:select(Q1, [title])),
    ?assertEqual([#{title => ~"c"}, #{title => ~"a"}], Selected),
    {ok, [#{sum := Sum}]} = kura_test_repo:all(
        kura_query:select(kura_query:from(?S), [{sum, qty}])
    ),
    ?assertEqual(6, Sum).

t_bulk_writes() ->
    reset(),
    {ok, 2, Returned} = kura_repo_worker:insert_all(
        kura_test_repo,
        ?S,
        [#{title => ~"a", qty => 1}, #{title => ~"b", qty => 2}],
        #{returning => [id, title]}
    ),
    ?assertMatch([#{id := _, title := ~"a"}, #{id := _, title := ~"b"}], Returned),
    ?assertEqual(
        {ok, 1},
        kura_test_repo:update_all(
            kura_query:where(kura_query:from(?S), {title, '=', ~"a"}), #{qty => 9}
        )
    ),
    ?assertMatch({ok, #{qty := 9}}, kura_test_repo:get_by(?S, [{title, ~"a"}])),
    ?assertEqual(
        {ok, 1}, kura_test_repo:delete_all(kura_query:where(kura_query:from(?S), {qty, '<', 5}))
    ),
    ?assertMatch({ok, [#{title := ~"a"}]}, kura_test_repo:all(kura_query:from(?S))).

t_upsert() ->
    reset(),
    {ok, #{id := Id}} = kura_test_repo:insert(changeset(#{}, #{title => ~"a", qty => 1})),
    {ok, Upserted} = kura_repo_worker:insert(
        kura_test_repo, changeset(#{}, #{title => ~"a", qty => 4}), #{
            on_conflict => {title, {replace, [qty]}}
        }
    ),
    ?assertMatch(#{id := Id, title := ~"a", qty := 4}, Upserted).

t_not_null_names_the_field() ->
    reset(),
    {error, CS} = kura_test_repo:insert(changeset(#{}, #{qty => 1})),
    ?assertEqual([{title, ~"can't be blank"}], CS#kura_changeset.errors).

t_cursor_paginate() ->
    reset(),
    {ok, 3} = kura_test_repo:insert_all(?S, [
        #{title => ~"a", qty => 3}, #{title => ~"b", qty => 1}, #{title => ~"c", qty => 2}
    ]),
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, kura_query:from(?S), #{
        limit => 2, cursor_field => qty
    }),
    ?assertEqual([~"b", ~"c"], [T || #{title := T} <- maps:get(entries, Page)]),
    ?assertEqual({1, 2}, {maps:get(start_cursor, Page), maps:get(end_cursor, Page)}),
    {ok, Next} = kura_paginator:cursor_paginate(kura_test_repo, kura_query:from(?S), #{
        limit => 2, cursor_field => qty, 'after' => maps:get(end_cursor, Page)
    }),
    ?assertEqual([~"a"], [T || #{title := T} <- maps:get(entries, Next)]).
