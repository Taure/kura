-module(kura_query_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

from_test() ->
    Q = kura_query:from(user),
    ?assertEqual(user, Q#kura_query.from).

from_defaults_test() ->
    Q = kura_query:from(user),
    ?assertEqual([], Q#kura_query.select),
    ?assertEqual([], Q#kura_query.wheres),
    ?assertEqual([], Q#kura_query.joins),
    ?assertEqual([], Q#kura_query.order_bys),
    ?assertEqual([], Q#kura_query.group_bys),
    ?assertEqual([], Q#kura_query.havings),
    ?assertEqual(undefined, Q#kura_query.limit),
    ?assertEqual(undefined, Q#kura_query.offset),
    ?assertEqual(false, Q#kura_query.distinct),
    ?assertEqual(undefined, Q#kura_query.lock),
    ?assertEqual(undefined, Q#kura_query.prefix),
    ?assertEqual([], Q#kura_query.preloads),
    ?assertEqual([], Q#kura_query.ctes),
    ?assertEqual([], Q#kura_query.combinations).

select_test() ->
    Q = kura_query:select(kura_query:from(user), [name, email]),
    ?assertEqual([name, email], Q#kura_query.select).

where_test() ->
    Q = kura_query:where(kura_query:from(user), {age, '>', 18}),
    ?assertEqual([{age, '>', 18}], Q#kura_query.wheres).

where_stacks_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:where(Q0, {age, '>', 18}),
    Q2 = kura_query:where(Q1, {active, true}),
    ?assertEqual(2, length(Q2#kura_query.wheres)).

join_test() ->
    Q = kura_query:join(kura_query:from(user), inner, post, {id, user_id}),
    ?assertEqual([{inner, post, {id, user_id}, undefined}], Q#kura_query.joins).

order_by_test() ->
    Q = kura_query:order_by(kura_query:from(user), [{name, asc}]),
    ?assertEqual([{name, asc}], Q#kura_query.order_bys).

limit_offset_test() ->
    Q = kura_query:offset(kura_query:limit(kura_query:from(user), 10), 20),
    ?assertEqual(10, Q#kura_query.limit),
    ?assertEqual(20, Q#kura_query.offset).

distinct_test() ->
    Q = kura_query:distinct(kura_query:from(user)),
    ?assertEqual(true, Q#kura_query.distinct).

distinct_on_test() ->
    Q = kura_query:distinct(kura_query:from(user), [email]),
    ?assertEqual([email], Q#kura_query.distinct).

count_test() ->
    Q = kura_query:count(kura_query:from(user)),
    ?assertEqual([{count, '*'}], Q#kura_query.select).

count_field_test() ->
    Q = kura_query:count(kura_query:from(user), email),
    ?assertEqual([{count, email}], Q#kura_query.select).

sum_test() ->
    Q = kura_query:sum(kura_query:from(user), score),
    ?assertEqual([{sum, score}], Q#kura_query.select).

composable_pipeline_test() ->
    Q = kura_query:from(user),
    Q1 = kura_query:where(Q, {age, '>', 18}),
    Q2 = kura_query:where(Q1, {'or', [{role, <<"admin">>}, {role, <<"moderator">>}]}),
    Q3 = kura_query:join(Q2, inner, post, {id, user_id}),
    Q4 = kura_query:select(Q3, [name, email]),
    Q5 = kura_query:order_by(Q4, [{name, asc}]),
    Q6 = kura_query:limit(Q5, 10),
    ?assertEqual(user, Q6#kura_query.from),
    ?assertEqual([name, email], Q6#kura_query.select),
    ?assertEqual(2, length(Q6#kura_query.wheres)),
    ?assertEqual(1, length(Q6#kura_query.joins)),
    ?assertEqual([{name, asc}], Q6#kura_query.order_bys),
    ?assertEqual(10, Q6#kura_query.limit).

%%----------------------------------------------------------------------
%% Encrypted fields cannot be filtered on
%%----------------------------------------------------------------------

where_rejects_an_encrypted_field_test() ->
    Q = kura_query:from(kura_test_encrypted_schema),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_query:where(Q, {ssn, <<"123-45-6789">>})
    ).

where_rejects_an_encrypted_field_with_an_operator_test() ->
    Q = kura_query:from(kura_test_encrypted_schema),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_query:where(Q, {ssn, like, <<"123%">>})
    ).

where_rejects_an_encrypted_field_nested_in_a_boolean_test() ->
    Q = kura_query:from(kura_test_encrypted_schema),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_query:where(Q, {'and', [{id, 1}, {'or', [{ssn, <<"x">>}]}]})
    ).

where_allows_a_plain_field_on_an_encrypted_schema_test() ->
    Q = kura_query:from(kura_test_encrypted_schema),
    ?assertMatch(#kura_query{}, kura_query:where(Q, {id, 1})).

where_allows_anything_on_a_schema_without_encrypted_fields_test() ->
    Q = kura_query:from(kura_test_schema),
    ?assertMatch(#kura_query{}, kura_query:where(Q, {email, <<"a@b.com">>})).

where_allows_a_bare_table_source_test() ->
    Q = kura_query:from(some_raw_table),
    ?assertMatch(#kura_query{}, kura_query:where(Q, {anything, 1})).

%%----------------------------------------------------------------------
%% distance/3
%%----------------------------------------------------------------------

-define(VEC_SCHEMA, kura_test_vector_schema).

distance_dumps_the_vector_to_text_test() ->
    ?assertEqual(
        {distance, embedding, cosine, ~"[1.0,2.5,-3.0]"},
        kura_query:distance(embedding, cosine, [1, 2.5, -3])
    ).

distance_accepts_every_metric_test_() ->
    [
        ?_assertMatch({distance, embedding, M, _}, kura_query:distance(embedding, M, [1.0]))
     || M <- [l2, cosine, inner_product]
    ].

distance_rejects_an_unknown_metric_test() ->
    ?assertError(
        {kura, {invalid_distance_metric, l1}},
        kura_query:distance(embedding, unknown_metric(), [1.0])
    ).

distance_rejects_a_list_of_non_numbers_test() ->
    ?assertError(
        {kura, {invalid_vector, embedding, _}}, kura_query:distance(embedding, l2, not_numbers())
    ).

distance_rejects_an_empty_vector_test() ->
    ?assertError({kura, {invalid_vector, embedding, _}}, kura_query:distance(embedding, l2, [])).

distance_rejects_a_preformatted_binary_test() ->
    ?assertError(
        {kura, {invalid_vector, embedding, _}},
        kura_query:distance(embedding, l2, preformatted_vector())
    ).

%% A raw list of small integers would bind as iodata, `[91,50,93]' as the
%% text "[2]". Dumping at build time binds the numbers.
distance_never_binds_an_integer_list_as_iodata_test() ->
    ?assertEqual(
        {distance, embedding, l2, ~"[91.0,50.0,93.0]"},
        kura_query:distance(embedding, l2, [91, 50, 93])
    ).

order_by_accepts_a_distance_on_a_vector_field_test() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Q = kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{D, asc}, {id, desc}]),
    ?assertEqual([{D, asc}, {id, desc}], Q#kura_query.order_bys).

select_expr_accepts_a_distance_on_a_vector_field_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Q = kura_query:select_expr(kura_query:from(?VEC_SCHEMA), [{id, id}, {dist, D}]),
    ?assertEqual({exprs, [{id, id}, {dist, D}]}, Q#kura_query.select).

distance_on_an_undimensioned_vector_field_takes_any_dimension_test() ->
    D = kura_query:distance(any_embedding, l2, lists:seq(1, 1536)),
    Q = kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{D, asc}]),
    ?assertEqual([{D, asc}], Q#kura_query.order_bys).

distance_requires_a_schema_from_test_() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    [
        ?_assertError(
            {kura, {distance_requires_schema, items}},
            kura_query:order_by(kura_query:from(items), [{D, asc}])
        ),
        ?_assertError(
            {kura, {distance_requires_schema, items}},
            kura_query:select_expr(kura_query:from(items), [{dist, D}])
        )
    ].

distance_on_a_non_vector_field_raises_test_() ->
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, {not_a_vector_field, F}},
            kura_query:order_by(Q, [{kura_query:distance(F, l2, [1, 2, 3]), asc}])
        )
     || F <- [content, no_such_field]
    ].

distance_of_the_wrong_dimension_raises_test_() ->
    Q = kura_query:from(?VEC_SCHEMA),
    D = kura_query:distance(embedding, cosine, [1, 2]),
    [
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:order_by(Q, [{D, asc}])
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:select_expr(Q, [{dist, D}])
        )
    ].

hand_built_distance_node_raises_test() ->
    ?assertError(
        {kura, {invalid_distance, _}},
        kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{hand_built_distance(), asc}])
    ).

order_by_without_a_distance_still_takes_a_table_atom_test() ->
    Q = kura_query:order_by(kura_query:from(items), [{name, asc}]),
    ?assertEqual([{name, asc}], Q#kura_query.order_bys).

%% Inputs outside distance/3's types, as a caller could still pass them.
-spec unknown_metric() -> dynamic().
unknown_metric() -> l1.

-spec not_numbers() -> dynamic().
not_numbers() -> [1.0, a].

-spec preformatted_vector() -> dynamic().
preformatted_vector() -> ~"[1,2,3]".

-spec hand_built_distance() -> dynamic().
hand_built_distance() -> {distance, embedding, l2, [1]}.

%% Thresholds land in PR2. Until then a distance outside order_by and
%% select_expr would leave its vector in the cache key and crash the
%% dialect, so the builder refuses it.
distance_outside_order_by_and_select_raises_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {D, '<', 0.3})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {D, between, {0.1, 0.3}})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {'and', [{id, 1}, {D, '<', 0.3}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {'or', [{'not', {D, '>', 0.9}}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {content, D})
        ),
        ?_assertError({kura, {distance_not_supported_here, where}}, kura_query:where(Q, D)),
        ?_assertError(
            {kura, {distance_not_supported_here, having}}, kura_query:having(Q, {D, '<', 0.3})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, group_by}},
            kura_query:group_by(Q, [content, untyped_distance()])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:over(rank, #{order_by => [{untyped_distance(), asc}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:over(row_number, #{partition_by => [untyped_distance()]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:over({min, untyped_distance()}, #{})
        )
    ].

%% Only a field or value position is inspected: a bound value that
%% happens to look like a distance node is data.
distance_shaped_values_in_lists_are_left_alone_test() ->
    Q = kura_query:where(kura_query:from(items), {tag, in, [{distance, a, l2, ~"x"}]}),
    ?assertEqual([{tag, in, [{distance, a, l2, ~"x"}]}], Q#kura_query.wheres).

%% group_by/2 and over/2 are typed for field atoms; a caller can still pass
%% a distance.
-spec untyped_distance() -> dynamic().
untyped_distance() -> kura_query:distance(embedding, cosine, [1, 2, 3]).
