-module(kura_query_sources_tests).
-moduledoc "kura_query:sources/1 and nested/1: what a query reads, at any depth.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(POST, kura_test_post_schema).
-define(USER, kura_test_schema).
-define(COMMENT, kura_test_comment_schema).

posts() -> kura_query:from(?POST).
users() -> kura_query:from(?USER).
comments() -> kura_query:from(?COMMENT).

%% A record built by hand can hold an entry the builders would not write.
%% What follows it is still read: the tenant check judges a query by this.
what_follows_an_entry_kura_cannot_read_is_read_test_() ->
    Odd = kura_test_typing:ill_typed(odd),
    [
        {"a join",
            ?_assertEqual(
                [{from, ?POST}, {join, ?USER}],
                kura_query:sources(
                    (posts())#kura_query{joins = [Odd, {inner, ?USER, {author_id, id}, undefined}]}
                )
            )},
        {"a CTE",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}],
                kura_query:sources((posts())#kura_query{ctes = [Odd, {~"people", users()}]})
            )},
        {"a combination",
            ?_assertEqual(
                [{from, ?POST}, {from, ?COMMENT}],
                kura_query:sources(
                    (posts())#kura_query{combinations = [Odd, {union, comments()}]}
                )
            )}
    ].

sources_test_() ->
    Sub = kura_query:select(users(), [id]),
    [
        {"a bare query", ?_assertEqual([{from, ?POST}], kura_query:sources(posts()))},
        {"no from", ?_assertEqual([], kura_query:sources(#kura_query{}))},
        {"a table name",
            ?_assertEqual(
                [{from, ~"posts"}, {from, posts}],
                kura_query:sources((posts())#kura_query{from = ~"posts"}) ++
                    kura_query:sources(kura_query:from(posts))
            )},
        {"joins, in the order they were added",
            ?_assertEqual(
                [{from, ?POST}, {join, ?USER}, {join, ~"comments"}],
                kura_query:sources(
                    kura_query:join(
                        kura_query:join(posts(), inner, ?USER, {author_id, id}),
                        left,
                        ~"comments",
                        {id, post_id}
                    )
                )
            )},
        {"an in subquery",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}],
                kura_query:sources(kura_query:where(posts(), {author_id, in, {subquery, Sub}}))
            )},
        {"exists and not_exists under and, or and not",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}, {from, ?COMMENT}],
                kura_query:sources(
                    kura_query:where(
                        posts(),
                        {'or', [
                            {id, '=', 1},
                            {'and', [{exists, {subquery, Sub}}]},
                            {'not', {not_exists, {subquery, comments()}}}
                        ]}
                    )
                )
            )},
        {"a having subquery",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}],
                kura_query:sources((posts())#kura_query{havings = [{id, in, {subquery, Sub}}]})
            )},
        {"a combination",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}, {from, ?COMMENT}],
                kura_query:sources(kura_query:union(kura_query:union(posts(), users()), comments()))
            )},
        {"a CTE, and a from that names it",
            ?_assertEqual(
                [{cte, recent}, {from, ?USER}],
                kura_query:sources(kura_query:with_cte(kura_query:from(recent), ~"recent", users()))
            )},
        {"a CTE the query does not read",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}],
                kura_query:sources(kura_query:with_cte(posts(), ~"recent", users()))
            )},
        {"a join that names a CTE, and a subquery that reads an outer one",
            ?_assertEqual(
                [{from, ?POST}, {cte, ~"recent"}, {from, ?USER}, {cte, recent}],
                kura_query:sources(
                    kura_query:where(
                        kura_query:join(
                            kura_query:with_cte(posts(), ~"recent", users()),
                            inner,
                            ~"recent",
                            {author_id, id}
                        ),
                        {id, in, {subquery, kura_query:from(recent)}}
                    )
                )
            )},
        {"a CTE's body that names the CTE reads the table",
            ?_assertEqual(
                [{cte, ~"posts"}, {from, ?POST}],
                kura_query:sources(
                    kura_query:with_cte(kura_query:from(~"posts"), ~"posts", posts())
                )
            )},
        {"a CTE's body reads the CTEs before it, and a table where a later one is named",
            ?_assertEqual(
                [{cte, ~"second"}, {from, ~"second"}, {cte, ~"first"}],
                kura_query:sources(
                    kura_query:with_cte(
                        kura_query:with_cte(
                            kura_query:from(~"second"), ~"first", kura_query:from(~"second")
                        ),
                        ~"second",
                        kura_query:from(~"first")
                    )
                )
            )},
        {"a subquery in a CTE's body sees what the body sees",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}, {from, ~"recent"}, {from, ?USER}, {cte, ~"recent"}],
                kura_query:sources(
                    kura_query:with_cte(
                        kura_query:with_cte(
                            posts(),
                            ~"recent",
                            kura_query:where(
                                users(), {id, in, {subquery, kura_query:from(~"recent")}}
                            )
                        ),
                        ~"later",
                        kura_query:where(
                            users(), {id, in, {subquery, kura_query:from(~"recent")}}
                        )
                    )
                )
            )},
        {"a schema names a CTE by its table, not by its module",
            ?_assertEqual(
                [{cte, ?POST}, {from, ?USER}, {from, ?POST}, {from, ?USER}],
                kura_query:sources(kura_query:with_cte(posts(), ~"posts", users())) ++
                    kura_query:sources(
                        kura_query:with_cte(posts(), atom_to_binary(?POST), users())
                    )
            )},
        {"a prefixed query reads tables, and the queries inside it still read its CTEs",
            ?_assertEqual(
                [{from, recent}, {join, ~"recent"}, {from, ?USER}, {cte, recent}],
                kura_query:sources(
                    kura_query:prefix(
                        kura_query:where(
                            kura_query:join(
                                kura_query:with_cte(kura_query:from(recent), ~"recent", users()),
                                inner,
                                ~"recent",
                                {id, id}
                            ),
                            {id, in, {subquery, kura_query:from(recent)}}
                        ),
                        ~"tenant_1"
                    )
                )
            )},
        {"three levels down",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}, {join, ?POST}, {from, ?COMMENT}],
                kura_query:sources(
                    kura_query:where(
                        posts(),
                        {id, in,
                            {subquery,
                                kura_query:where(
                                    kura_query:join(users(), inner, ?POST, {id, author_id}),
                                    {exists, {subquery, comments()}}
                                )}}
                    )
                )
            )}
    ].

preload_sources_test_() ->
    [
        {"an association's schema",
            ?_assertEqual(
                [{from, ?POST}, {preload, ?USER}],
                kura_query:sources(kura_query:preload(posts(), [author]))
            )},
        {"a many-to-many's schema and its join table",
            ?_assertEqual(
                [{from, ?POST}, {preload, kura_test_tag_schema}, {preload, ~"posts_tags"}],
                kura_query:sources(kura_query:preload(posts(), [tags]))
            )},
        {"nested and conditional preloads",
            ?_assertEqual(
                [{from, ?POST}, {preload, ?COMMENT}, {preload, ?POST}, {preload, ?USER}],
                kura_query:sources(
                    kura_query:preload(
                        posts(), kura_test_typing:ill_typed([{comments, [post]}, {author, #{}}])
                    )
                )
            )},
        {"each step of a through association",
            ?_assertEqual(
                [
                    {from, kura_test_through_user},
                    {preload, kura_test_through_post},
                    {preload, kura_test_through_comment}
                ],
                kura_query:sources(
                    kura_query:preload(kura_query:from(kura_test_through_user), [comments])
                )
            )},
        {"an association the schema does not have is not a source",
            ?_assertEqual(
                [{from, ?POST}], kura_query:sources(kura_query:preload(posts(), [no_such]))
            )},
        {"a table has no associations",
            ?_assertEqual(
                [{from, posts}],
                kura_query:sources(kura_query:preload(kura_query:from(posts), [author]))
            )},
        {"a nested query's preloads are never run",
            ?_assertEqual(
                [{from, ?POST}, {from, ?USER}],
                kura_query:sources(
                    kura_query:union(posts(), kura_query:preload(users(), [posts]))
                )
            )}
    ].

nested_test_() ->
    Sub = kura_query:select(users(), [id]),
    Q = kura_query:union(
        kura_query:with_cte(
            (kura_query:where(posts(), {'not', {id, in, {subquery, Sub}}}))#kura_query{
                havings = [{exists, {subquery, comments()}}]
            },
            ~"recent",
            users()
        ),
        comments()
    ),
    [
        ?_assertEqual([], kura_query:nested(posts())),
        ?_assertEqual([users(), comments(), Sub, comments()], kura_query:nested(Q)),
        {"only the queries directly inside",
            ?_assertEqual(
                [kura_query:where(users(), {exists, {subquery, comments()}})],
                kura_query:nested(
                    kura_query:union(
                        posts(), kura_query:where(users(), {exists, {subquery, comments()}})
                    )
                )
            )},
        {"an improper condition list ends the walk",
            ?_assertEqual(
                [],
                kura_query:nested((posts())#kura_query{
                    wheres = kura_test_typing:ill_typed(not_a_list)
                })
            )}
    ].
