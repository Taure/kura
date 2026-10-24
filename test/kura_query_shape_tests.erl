-module(kura_query_shape_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-behaviour(kura_dialect).

-export([
    to_sql/1,
    to_sql_from/2,
    insert/3,
    insert/4,
    update/4,
    delete/2,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4
]).

-define(REPO, shape_test_repo).

%% Every condition kind the PG dialect knows, built so that `N' changes
%% every bound value and nothing else. Each position gets its own value, so
%% a value bound into the wrong position changes the result.
queries(N) ->
    V = fun(K) -> N * 100 + K end,
    Sub = fun(K) -> #kura_query{from = orders, wheres = [{total, '>', V(K)}]} end,
    [
        {"bare equality", #kura_query{from = users, wheres = [{id, V(1)}]}},
        {"= and !=", #kura_query{from = users, wheres = [{id, '=', V(1)}, {age, '!=', V(2)}]}},
        {"ordering comparisons", #kura_query{
            from = users,
            wheres = [{age, '<', V(1)}, {age, '>', V(2)}, {age, '<=', V(3)}, {age, '>=', V(4)}]
        }},
        {"text matching", #kura_query{
            from = users,
            wheres = [
                {email, like, integer_to_binary(V(1))},
                {email, ilike, integer_to_binary(V(2))},
                {bio, matches, integer_to_binary(V(3))}
            ]
        }},
        {"in and not_in", #kura_query{
            from = users, wheres = [{role, in, [V(1), V(2)]}, {role, not_in, [V(3)]}]
        }},
        {"between", #kura_query{from = users, wheres = [{age, between, {V(1), V(2)}}]}},
        {"row-value in", #kura_query{
            from = users, wheres = [{[org_id, id], in, [{V(1), V(2)}, {V(3), V(4)}]}]
        }},
        {"single-column row in", #kura_query{from = users, wheres = [{[id], in, [{V(1)}, {V(2)}]}]}},
        {"and/or/not", #kura_query{
            from = users,
            wheres = [{'or', [{id, V(1)}, {'and', [{age, V(2)}, {'not', {role, V(3)}}]}]}]
        }},
        {"where fragment", #kura_query{
            from = users, wheres = [{fragment, ~"age > ? AND age < ?", [V(1), V(2)]}]
        }},
        {"is_nil", #kura_query{from = users, wheres = [{deleted_at, is_nil}, {id, V(1)}]}},
        {"is_not_nil", #kura_query{from = users, wheres = [{deleted_at, is_not_nil}, {id, V(1)}]}},
        {"in subquery", #kura_query{from = users, wheres = [{id, in, {subquery, Sub(1)}}]}},
        {"exists", #kura_query{from = users, wheres = [{exists, {subquery, Sub(1)}}, {id, V(2)}]}},
        {"not_exists", #kura_query{from = users, wheres = [{not_exists, {subquery, Sub(1)}}]}},
        {"limit and offset", #kura_query{from = users, limit = V(1), offset = V(2)}},
        {"having fragment", #kura_query{
            from = users,
            group_bys = [role],
            havings = [{fragment, ~"count(*) > ?", [V(1)]}]
        }},
        {"cte",
            kura_query:with_cte(
                #kura_query{from = users, wheres = [{id, V(1)}]},
                ~"recent",
                Sub(2)
            )},
        {"union",
            kura_query:union(Sub(1), #kura_query{from = orders, wheres = [{total, '<', V(2)}]})}
    ].

setup_pg() ->
    setup_dialect(kura_dialect_pg).

setup_dialect(Dialect) ->
    application:set_env(kura, repos, #{?REPO => #{dialect => Dialect}}),
    kura_query_cache:init(),
    kura_query_cache:flush().

cleanup(_) ->
    kura_query_cache:flush(),
    application:unset_env(kura, repos).

with_pg(Fun) ->
    with_dialect(kura_dialect_pg, Fun).

with_dialect(Dialect, Fun) ->
    setup_dialect(Dialect),
    try
        Fun()
    after
        cleanup(ok)
    end.

cached_matches_uncached_test_() ->
    {setup, fun setup_pg/0, fun cleanup/1, [
        {Title,
            ?_assertEqual(
                kura_query_compiler:to_sql(?REPO, Q),
                kura_query_compiler:to_sql_cached(?REPO, Q)
            )}
     || N <- [1, 2, 3],
        {Title, Q} <- queries(N)
    ]}.

%% Correct results alone would not show the cache is used: the uncacheable
%% fallback also compiles every query correctly.
every_pg_condition_kind_is_cached_as_a_template_test() ->
    with_pg(fun() ->
        _ = [kura_query_compiler:to_sql_cached(?REPO, Q) || N <- [1, 2], {_, Q} <- queries(N)],
        ?assertEqual(length(queries(1)), ets:info(kura_query_cache, size)),
        ?assertEqual([], ets:match_object(kura_query_cache, {'_', uncacheable}))
    end).

queries_differing_only_in_values_share_one_entry_test() ->
    with_pg(fun() ->
        _ = [
            kura_query_compiler:to_sql_cached(?REPO, Q)
         || N <- lists:seq(1, 50), {_, Q} <- queries(N)
        ],
        ?assertEqual(length(queries(1)), ets:info(kura_query_cache, size))
    end).

values_left_in_the_shape_are_carried_through_a_hit_test() ->
    with_pg(fun() ->
        Q = fun(Bonus, Id) ->
            #kura_query{
                from = users,
                select = {exprs, [{score, {fragment, ~"? + 1", [Bonus]}}]},
                wheres = [{id, Id}]
            }
        end,
        [
            ?assertEqual(
                kura_query_compiler:to_sql(?REPO, Q(B, I)),
                kura_query_compiler:to_sql_cached(?REPO, Q(B, I))
            )
         || {B, I} <- [{5, 1}, {5, 2}, {6, 3}, {6, 4}]
        ],
        ?assertEqual(2, ets:info(kura_query_cache, size))
    end).

is_nil_is_part_of_the_shape_test() ->
    with_pg(fun() ->
        Null = #kura_query{from = users, wheres = [{deleted_at, is_nil}]},
        Bound = #kura_query{from = users, wheres = [{deleted_at, ~"2026-01-01"}]},
        ?assertEqual(
            kura_query_compiler:to_sql(?REPO, Null), kura_query_compiler:to_sql_cached(?REPO, Null)
        ),
        ?assertEqual(
            kura_query_compiler:to_sql(?REPO, Bound),
            kura_query_compiler:to_sql_cached(?REPO, Bound)
        )
    end).

nil_stays_in_the_shape_test() ->
    {Shape, Values} = kura_query_shape:split(
        #kura_query{from = users, wheres = [{deleted_at, nil}, {role, in, [admin, nil]}]}
    ),
    ?assertEqual({admin}, Values),
    ?assertEqual(
        [{deleted_at, nil}, {role, in, [{'$kura_param', 1}, nil]}], Shape#kura_query.wheres
    ).

in_lists_of_different_lengths_are_different_shapes_test() ->
    with_pg(fun() ->
        Q = fun(Ids) -> #kura_query{from = users, wheres = [{id, in, Ids}]} end,
        [
            ?assertEqual(
                kura_query_compiler:to_sql(?REPO, Q(Ids)),
                kura_query_compiler:to_sql_cached(?REPO, Q(Ids))
            )
         || Ids <- [[1], [1, 2], [3], [1, 2, 3], [4, 5]]
        ],
        ?assertEqual(3, ets:info(kura_query_cache, size))
    end).

ambient_tenant_is_part_of_the_shape_key_test() ->
    with_pg(fun() ->
        Q = fun(N) ->
            kura_query:with_cte(
                #kura_query{from = outer_schema, wheres = [{id, N}]},
                ~"scoped",
                #kura_query{from = inner_schema}
            )
        end,
        try
            kura_tenant:put_tenant({prefix, ~"tenant_a"}),
            _ = kura_query_compiler:to_sql_cached(?REPO, Q(1)),
            kura_tenant:put_tenant({prefix, ~"tenant_b"}),
            ?assertEqual(
                kura_query_compiler:to_sql(?REPO, Q(2)),
                kura_query_compiler:to_sql_cached(?REPO, Q(2))
            )
        after
            kura_tenant:clear_tenant()
        end
    end).

%% This module's dialect writes bound values into the SQL text, the one
%% thing the shape cache cannot survive. The markers never reach the
%% parameter list, so the shape is marked uncacheable and every call
%% compiles its own query.
dialect_that_inlines_values_is_never_served_a_shape_test() ->
    with_dialect(?MODULE, fun() ->
        QA = #kura_query{from = users, wheres = [{id, 1}]},
        QB = #kura_query{from = users, wheres = [{id, 2}]},
        ?assertEqual(to_sql(QA), kura_query_compiler:to_sql_cached(?REPO, QA)),
        ?assertEqual(to_sql(QB), kura_query_compiler:to_sql_cached(?REPO, QB)),
        ?assertEqual(to_sql(QA), kura_query_compiler:to_sql_cached(?REPO, QA)),
        ?assertMatch([{_, uncacheable}], ets:tab2list(kura_query_cache))
    end).

%% The record types `limit' as an integer, so a dialect may guard on it and
%% crash on a marker. The shape is then uncacheable; the query still works.
dialect_that_rejects_a_marker_falls_back_to_the_query_test() ->
    with_dialect(?MODULE, fun() ->
        Q = #kura_query{from = users, limit = 5},
        ?assertEqual(to_sql(Q), kura_query_compiler:to_sql_cached(?REPO, Q)),
        ?assertEqual(to_sql(Q), kura_query_compiler:to_sql_cached(?REPO, Q)),
        ?assertError(badarg, kura_query_compiler:to_sql_cached(?REPO, Q#kura_query{limit = -1}))
    end).

split_returns_values_in_marker_order_test() ->
    {Shape, Values} = kura_query_shape:split(
        #kura_query{from = users, wheres = [{id, 7}, {age, between, {18, 65}}], limit = 10}
    ),
    ?assertEqual({7, 18, 65, 10}, Values),
    ?assertEqual(
        [{id, {'$kura_param', 1}}, {age, between, {{'$kura_param', 2}, {'$kura_param', 3}}}],
        Shape#kura_query.wheres
    ),
    ?assertEqual({'$kura_param', 4}, Shape#kura_query.limit).

valid_template_test_() ->
    M = fun(N) -> {'$kura_param', N} end,
    [
        ?_assert(kura_query_shape:valid_template([M(2), fixed, M(1)], 2)),
        ?_assert(kura_query_shape:valid_template([], 0)),
        ?_assertNot(kura_query_shape:valid_template([M(1)], 2)),
        ?_assertNot(kura_query_shape:valid_template([M(1), M(1)], 1)),
        ?_assertNot(kura_query_shape:valid_template([M(1), M(2)], 1)),
        ?_assertNot(kura_query_shape:valid_template([M(1), {wrapped, M(1)}], 1)),
        ?_assertNot(kura_query_shape:valid_template([M(1), [M(1)]], 1)),
        ?_assertNot(kura_query_shape:valid_template([M(1), #{k => M(1)}], 1))
    ].

to_sql(#kura_query{limit = Limit}) when Limit =/= undefined, not is_integer(Limit); Limit < 0 ->
    error(badarg);
to_sql(#kura_query{wheres = Wheres, limit = Limit}) ->
    {iolist_to_binary(io_lib:format("SELECT ~0p LIMIT ~0p", [Wheres, Limit])), []}.

to_sql_from(Q, Counter) ->
    {SQL, Params} = to_sql(Q),
    {SQL, Params, Counter}.

insert(_, _, _) -> {~"", []}.
insert(_, _, _, _) -> {~"", []}.
update(_, _, _, _) -> {~"", []}.
delete(_, _) -> {~"", []}.
update_all(_, _) -> {~"", []}.
delete_all(_) -> {~"", []}.
insert_all(_, _, _) -> {~"", []}.
insert_all(_, _, _, _) -> {~"", []}.
