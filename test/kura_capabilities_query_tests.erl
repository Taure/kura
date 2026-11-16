-module(kura_capabilities_query_tests).
-moduledoc "A query is refused when the repo's pool module does not declare what it needs.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_capabilities_query_test_repo).
-define(UNDATED, kura_test_limited_pool).
-define(CURRENT, kura_test_current_pool).
-define(MISDATED, kura_test_misdated_pool).

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
        {window_functions,
            kura_query:select_expr(from(), [
                {id, id}, {position, kura_query:over(row_number, #{order_by => [{id, asc}]})}
            ])},
        {vector, kura_query:order_by(from(), [{Vector, asc}])},
        {vector, kura_query:select_expr(from(), [{distance, Vector}])},
        {vector, kura_query:select_expr(from(), [{id, id}, {distance, Vector}])},
        {vector, kura_query:where(from(), {Vector, '<', 0.5})},
        {select_for_update_skip_locked, kura_query:lock(from(), {for_update, skip_locked})},
        {select_for_update_skip_locked, kura_query:lock(from(), ~"FOR SHARE SKIP LOCKED")},
        {select_for_update_skip_locked, (from())#kura_query{lock = ~"for update skip locked"}},
        {select_for_update_skip_locked,
            kura_query:lock(
                from(), {for_update, #{tables => [~"kura_vector_items"], wait => skip_locked}}
            )},
        {select_for_update_skip_locked,
            kura_query:lock(from(), [
                {for_update, #{tables => [~"kura_vector_items"]}}, {for_share, skip_locked}
            ])},
        {row_locks,
            kura_query:lock(from(), [{for_update, #{tables => [~"kura_vector_items"]}}, for_share])},
        {row_locks, kura_query:lock(from(), for_update)},
        {row_locks, kura_query:lock(from(), {for_share, nowait})},
        {row_locks, kura_query:lock(from(), ~"FOR KEY SHARE")},
        {row_locks, kura_query:lock(from(), {fragment, ~"FOR UPDATE OF kura_vector_items"})},
        {row_locks, kura_query:with_cte(from(), ~"held", kura_query:lock(from(), for_update))},
        {distinct_on, kura_query:distinct(from(), [content])},
        {distinct_on,
            kura_query:where(
                from(), {id, in, {subquery, kura_query:distinct(from(), [content])}}
            )}
    ].

label(Capability, Query) ->
    lists:flatten(io_lib:format("~s ~0p", [Capability, Query])).

undeclared_capability_is_refused_test_() ->
    with_pool(#{pool_module => ?CURRENT}, [
        {label(Capability, Query), fun() ->
            Error = {kura, {unsupported_capability, Capability, ?CURRENT}},
            ?assertError(Error, kura_query_compiler:to_sql(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_cached(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_cached(?REPO, Query)),
            ?assertError(Error, kura_query_compiler:to_sql_from(?REPO, Query, 1))
        end}
     || {Capability, Query} <- needing()
    ]).

bulk_write_is_refused_test_() ->
    Query = kura_query:where(from(), {content, matches, ~"erlang"}),
    Error = {kura, {unsupported_capability, full_text_search, ?CURRENT}},
    with_pool(#{pool_module => ?CURRENT}, [
        ?_assertError(Error, kura_query_compiler:delete_all(?REPO, Query)),
        ?_assertError(Error, kura_query_compiler:update_all(?REPO, Query, #{content => ~"x"}))
    ]).

query_needing_nothing_compiles_test_() ->
    Q0 = kura_query:where(from(), {'or', [{id, '=', 1}, {content, ilike, ~"%x%"}]}),
    Q1 = kura_query:order_by(Q0, [{id, desc}]),
    with_pool(#{pool_module => ?CURRENT}, [
        ?_assertMatch({_, [1, ~"%x%"]}, kura_query_compiler:to_sql(?REPO, Q1)),
        ?_assertMatch({_, [1, ~"%x%"]}, kura_query_compiler:to_sql_cached(?REPO, Q1))
    ]).

%% A list written before kura checked anything, as kura_sqlite 1.3.1's is,
%% says nothing about what the backend cannot do: 2.30 sent every query to
%% it, and so does a later 2.x.
a_pool_without_a_capability_model_is_not_checked_test_() ->
    [
        with_pool(PoolSettings, [
            {label(Capability, Query), fun() ->
                ?assertMatch({_, _}, kura_query_compiler:to_sql(?REPO, Query)),
                ?assertMatch({_, _}, kura_query_compiler:to_sql_cached(?REPO, Query))
            end}
         || {Capability, Query} <- needing()
        ])
     || PoolSettings <- [
            #{pool_module => ?UNDATED},
            #{pool_module => kura_pool_minato},
            #{pool_module => kura_pool_ets},
            #{pool_module => lists},
            #{}
        ]
    ].

skip_locked_is_named_before_row_locks_test_() ->
    Query = kura_query:lock(from(), {for_update, skip_locked}),
    [
        ?_assertError(
            {kura, {unsupported_capability, select_for_update_skip_locked, ?CURRENT}},
            kura_capabilities:check_query(?CURRENT, Query)
        )
    ].

plain_distinct_needs_nothing_test_() ->
    with_pool(#{pool_module => ?CURRENT}, [
        ?_assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, kura_query:distinct(from())))
    ]).

check_test_() ->
    [
        ?_assertEqual(ok, kura_capabilities:check(?UNDATED, cursors)),
        ?_assertEqual(ok, kura_capabilities:check(?UNDATED, vector)),
        ?_assertEqual(ok, kura_capabilities:check(lists, cursors)),
        ?_assertEqual(ok, kura_capabilities:check(?CURRENT, returning)),
        ?_assertError(
            {kura, {unsupported_capability, cursors, ?CURRENT}},
            kura_capabilities:check(?CURRENT, cursors)
        ),
        ?_assertError(
            {kura, {unsupported_capability, vector, ?CURRENT}},
            kura_capabilities:check(?CURRENT, vector)
        ),
        ?_assertEqual(ok, kura_capabilities:check(?CURRENT, some_custom_flag))
    ].

stream_needs_cursors_test_() ->
    with_pool(#{pool_module => ?CURRENT}, [
        ?_assertError(
            {kura, {unsupported_capability, cursors, ?CURRENT}},
            kura_stream:stream(?REPO, from(), fun(_Batch) -> ok end)
        )
    ]).

check_query_test_() ->
    Query = kura_query:lock(from(), {for_update, skip_locked}),
    [
        ?_assertEqual(ok, kura_capabilities:check_query(kura_pool_minato, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(lists, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(?UNDATED, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(kura_pool_ets, Query)),
        ?_assertEqual(ok, kura_capabilities:check_query(?CURRENT, from())),
        ?_assertError(
            {kura, {unsupported_capability, select_for_update_skip_locked, ?CURRENT}},
            kura_capabilities:check_query(?CURRENT, Query)
        )
    ].

%%----------------------------------------------------------------------
%% The model a pool module declares
%%----------------------------------------------------------------------

model_test_() ->
    [
        ?_assertEqual(unchecked, kura_capabilities:model(?UNDATED)),
        ?_assertEqual(unchecked, kura_capabilities:model(lists)),
        ?_assertEqual(unchecked, kura_capabilities:model(kura_no_such_pool_module)),
        ?_assertEqual(1, kura_capabilities:model(?CURRENT)),
        ?_assertEqual(ok, kura_capabilities:validate(?CURRENT)),
        ?_assertEqual(ok, kura_capabilities:validate(?UNDATED)),
        ?_assertEqual(
            {error, {invalid_capability_model, ?MISDATED, 2}},
            kura_capabilities:validate(?MISDATED)
        )
    ].

%% erlang:function_exported/3 is false for a module nothing has loaded yet,
%% which a backend's pool module is when the first repo is configured.
a_module_not_yet_loaded_is_asked_test_() ->
    Unloaded = fun(Module) ->
        _ = code:purge(Module),
        _ = code:delete(Module),
        _ = code:purge(Module),
        false = erlang:module_loaded(Module),
        Module
    end,
    [
        ?_assertEqual(1, kura_capabilities:model(Unloaded(?CURRENT))),
        ?_assertEqual(unchecked, kura_capabilities:model(Unloaded(?UNDATED))),
        ?_assertEqual(
            {error, {invalid_capability_model, ?MISDATED, 2}},
            kura_capabilities:validate(Unloaded(?MISDATED))
        ),
        ?_assertError(
            {kura, {unsupported_capability, cursors, ?CURRENT}},
            kura_capabilities:check(Unloaded(?CURRENT), cursors)
        )
    ].

%% A model this kura does not know is named, in a check as well: it used to
%% be a function_clause when the first query was compiled.
an_unknown_model_is_named_test_() ->
    Invalid = {kura, {invalid_capability_model, ?MISDATED, 2}},
    [
        ?_assertError(Invalid, kura_capabilities:model(?MISDATED)),
        ?_assertError(Invalid, kura_capabilities:check(?MISDATED, cursors)),
        ?_assertError(Invalid, kura_capabilities:check_query(?MISDATED, from())),
        with_pool(#{pool_module => ?MISDATED}, [
            ?_assertError(Invalid, kura_query_compiler:to_sql(?REPO, from()))
        ])
    ].
