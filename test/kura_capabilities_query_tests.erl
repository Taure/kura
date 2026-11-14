-module(kura_capabilities_query_tests).
-moduledoc "A query is refused when the repo's pool module does not declare what it needs.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_capabilities_query_test_repo).
-define(LIMITED, kura_test_limited_pool).

with_pool(PoolSettings, Tests) ->
    {setup,
        fun() ->
            application:set_env(kura, repos, #{
                ?REPO => maps:merge(#{dialect => kura_dialect_pg}, PoolSettings)
            }),
            kura_query_cache:flush()
        end,
        fun(_) ->
            application:unset_env(kura, repos),
            kura_query_cache:flush()
        end,
        Tests}.

from() -> kura_query:from(kura_test_vector_schema).

needing() ->
    Vector = kura_query:distance(embedding, l2, [1, 2, 3]),
    Matches = kura_query:where(from(), {content, matches, ~"erlang"}),
    [
        {full_text_search, Matches},
        {full_text_search,
            kura_query:where(from(), {'or', [{id, '=', 1}, {content, matches, ~"x"}]})},
        {full_text_search,
            kura_query:where(from(), {'not', {'and', [{content, matches, ~"x"}, {id, '>', 1}]}})},
        {full_text_search, (from())#kura_query{havings = [{content, matches, ~"x"}]}},
        {full_text_search, kura_query:where(from(), {id, in, {subquery, Matches}})},
        {full_text_search, kura_query:where(from(), {exists, {subquery, Matches}})},
        {full_text_search, kura_query:where(from(), {not_exists, {subquery, Matches}})},
        {full_text_search, kura_query:with_cte(from(), ~"found", Matches)},
        {full_text_search, kura_query:union(from(), Matches)},
        {window_functions,
            kura_query:select_expr(from(), [
                {position, kura_query:over(row_number, #{order_by => [{id, asc}]})}
            ])},
        {vector, kura_query:order_by(from(), [{Vector, asc}])},
        {vector, kura_query:select_expr(from(), [{distance, Vector}])},
        {vector, kura_query:where(from(), {Vector, '<', 0.5})},
        {select_for_update_skip_locked, kura_query:lock(from(), {for_update, skip_locked})},
        {select_for_update_skip_locked, kura_query:lock(from(), ~"FOR SHARE SKIP LOCKED")},
        {select_for_update_skip_locked, (from())#kura_query{lock = ~"for update skip locked"}}
    ].

label(Capability, Query) ->
    lists:flatten(io_lib:format("~s ~0p", [Capability, Query])).

undeclared_capability_is_refused_test_() ->
    with_pool(#{pool_module => ?LIMITED}, [
        {label(Capability, Query), fun() ->
            Error = {kura, {unsupported_capability, Capability, ?LIMITED}},
            ?assertError(Error, kura_query_compiler:to_sql(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_cached(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_cached(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_from(?REPO, Query, 1))
        end}
     || {Capability, Query} <- needing()
    ]).

bulk_write_is_refused_test_() ->
    Query = kura_query:where(from(), {content, matches, ~"erlang"}),
    Error = {kura, {unsupported_capability, full_text_search, ?LIMITED}},
    with_pool(#{pool_module => ?LIMITED}, [
        ?_assertError(Error, kura_query_compiler:delete_all(?REPO, Query)),
        ?_assertError(Error, kura_query_compiler:update_all(?REPO, Query, #{content => ~"x"}))
    ]).

query_needing_nothing_compiles_test_() ->
    Q0 = kura_query:where(from(), {'or', [{id, '=', 1}, {content, ilike, ~"%x%"}]}),
    Q1 = kura_query:lock(kura_query:order_by(Q0, [{id, desc}]), {for_update, nowait}),
    with_pool(#{pool_module => ?LIMITED}, [
        ?_assertMatch({_, [1, ~"%x%"]}, kura_query_compiler:to_sql(?REPO, Q1)),
        ?_assertMatch({_, [1, ~"%x%"]}, kura_query_compiler:to_sql_cached(?REPO, Q1))
    ]).

unchecked_pools_test_() ->
    [
        with_pool(PoolSettings, [
            {label(Capability, Query), fun() ->
                ?assertMatch({_, _}, kura_query_compiler:to_sql(?REPO, Query)),
                ?assertMatch({_, _}, kura_query_compiler:to_sql_cached(?REPO, Query))
            end}
         || {Capability, Query} <- needing()
        ])
     || PoolSettings <- [#{pool_module => kura_pool_minato}, #{pool_module => lists}, #{}]
    ].

check_query_test_() ->
    Query = kura_query:lock(from(), {for_update, skip_locked}),
    [
        ?_assertEqual(ok, kura_capabilities:check_query(kura_pool_minato, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(lists, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(?LIMITED, from())),
        ?_assertError(
            {kura, {unsupported_capability, select_for_update_skip_locked, kura_pool_ets}},
            kura_capabilities:check_query(kura_pool_ets, Query)
        )
    ].
