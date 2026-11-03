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
        {kura, {invalid_distance, embedding}},
        kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{hand_built_distance(), asc}])
    ).

%% The reason names the field, or nothing when the field is not an atom,
%% so the vector never lands in it.
hand_built_distance_on_a_non_atom_field_raises_test() ->
    ?assertError(
        {kura, invalid_distance},
        kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{non_atom_field_distance(), asc}])
    ).

%% A list comprehension over an improper list would raise a bad_generator
%% carrying the tail, the distance and its vector with it.
improper_distance_lists_raise_without_the_vector_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, malformed_order_by},
            kura_query:order_by(Q, [{id, asc} | improper({D, asc})])
        ),
        ?_assertError(
            {kura, malformed_select},
            kura_query:select_expr(Q, [{id, id} | improper({dist, D})])
        ),
        ?_assertError(
            {kura, malformed_order_by},
            kura_query:check_distances(Q#kura_query{order_bys = [{id, asc} | improper({D, asc})]})
        ),
        ?_assertError(
            {kura, malformed_select},
            kura_query:check_distances(Q#kura_query{select = {exprs, [{id, id} | improper({d, D})]}})
        )
    ].

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

-spec non_atom_field_distance() -> dynamic().
non_atom_field_distance() -> {distance, "embedding", l2, ~"[1.0,2.0,3.0]"}.

-spec improper(term()) -> dynamic().
improper(Term) -> Term.

%% A distance anywhere but select_expr, order_by or the left side of a
%% where or having threshold would leave its vector in the cache key and
%% crash the dialect, so the builder refuses it.
distance_outside_order_by_select_and_thresholds_raises_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {D, between, {0.1, 0.3}})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {'and', [{id, 1}, {D, '=', 0.3}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {'or', [{'not', {D, '!=', 0.9}}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {D, 0.3})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {D, like, ~"x"})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {D, in, [0.3]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {D, is_nil})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {D, in, {subquery, kura_query:from(?VEC_SCHEMA)}})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {id, '<', D})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {content, D})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {id, in, [D]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {id, not_in, [1, 2, D]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {content, between, {0.1, D}})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {content, between, {D, 0.3}})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {[id, content], in, [{1, D}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {[id, D], in, [{1, ~"x"}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}}, kura_query:where(Q, {tags, [D]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {'and', [{id, 1}, {id, in, [D]}]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, having}},
            kura_query:having(Q, {id, in, [D]})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, select}}, kura_query:select(Q, [id, D])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, select}}, kura_query:select(Q, [{sum, D}])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, select}},
            kura_query:select(Q, [id | improper(D)])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, select}},
            kura_query:sum(Q, untyped_distance())
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, select}},
            kura_query:max(Q, untyped_distance())
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, count}},
            kura_query:count(Q, untyped_distance())
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, distinct}},
            kura_query:distinct(Q, [id, untyped_distance()])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, join}},
            kura_query:join(Q, inner, ?VEC_SCHEMA, {untyped_distance(), id})
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, join}},
            kura_query:join(Q, left, ?VEC_SCHEMA, {id, untyped_distance()}, other)
        ),
        ?_assertError({kura, {distance_not_supported_here, where}}, kura_query:where(Q, D)),
        ?_assertError(
            {kura, {distance_not_supported_here, having}}, kura_query:having(Q, {D, '=', 0.3})
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

%% A distance-shaped element of a list value would be bound as a parameter,
%% so it is refused like a distance. An improper list value is refused
%% rather than left for the dialect to crash on with its elements.
distance_shaped_values_in_lists_are_refused_test_() ->
    Q = kura_query:from(items),
    [
        ?_assertError(
            {kura, {distance_not_supported_here, where}},
            kura_query:where(Q, {tag, in, [{distance, a, l2, ~"x"}]})
        ),
        ?_assertError(
            {kura, unrecognised_condition},
            kura_query:where(Q, {tag, in, [~"a" | improper(~"b")]})
        )
    ].

plain_list_values_are_still_accepted_test() ->
    Q = kura_query:where(kura_query:from(items), {tag, in, [~"a", {1, 2}, [3]]}),
    ?assertEqual([{tag, in, [~"a", {1, 2}, [3]]}], Q#kura_query.wheres).

hand_built_distance_with_an_unknown_metric_raises_test() ->
    ?assertError(
        {kura, {invalid_distance, _}},
        kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{bad_metric_distance(), asc}])
    ).

-spec bad_metric_distance() -> dynamic().
bad_metric_distance() -> {distance, embedding, l1, ~"[1.0,2.0,3.0]"}.

%% group_by/2 and over/2 are typed for field atoms; a caller can still pass
%% a distance.
-spec untyped_distance() -> dynamic().
untyped_distance() -> kura_query:distance(embedding, cosine, [1, 2, 3]).

%% pgvector's limit. A larger vector used to be cast and dumped in full
%% before the server rejected it.
distance_caps_the_dimension_at_16000_test_() ->
    [
        ?_assertMatch(
            {distance, any_embedding, l2, _},
            kura_query:distance(any_embedding, l2, lists:duplicate(16000, 1.0))
        ),
        ?_assertError(
            {kura, {invalid_vector, any_embedding, _}},
            kura_query:distance(any_embedding, l2, lists:duplicate(16001, 1.0))
        ),
        ?_assertError(
            {kura, {invalid_vector, any_embedding, _}},
            kura_query:distance(any_embedding, l2, lists:duplicate(2000000, 1.0))
        )
    ].

distance_rejects_an_improper_list_test() ->
    ?assertError(
        {kura, {invalid_vector, embedding, _}},
        kura_query:distance(embedding, l2, improper_vector())
    ).

distance_rejects_an_element_beyond_the_float_range_test() ->
    ?assertError(
        {kura, {invalid_vector, embedding, _}}, kura_query:distance(embedding, l2, [1 bsl 2000])
    ).

-spec improper_vector() -> dynamic().
improper_vector() -> [1.0, 2.0 | not_a_list()].

-spec not_a_list() -> dynamic().
not_a_list() -> 3.0.

%% A window built by hand, not through over/2, still may not carry a
%% distance.
select_expr_rejects_a_distance_in_a_hand_built_window_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:select_expr(Q, [{r, {over, rank, #{order_by => [{D, asc}]}}}])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:select_expr(Q, [{r, {over, row_number, #{partition_by => [D]}}}])
        ),
        ?_assertError(
            {kura, {distance_not_supported_here, over}},
            kura_query:select_expr(Q, [{m, {over, {min, D}, #{}}}])
        )
    ].

too_many_dimensions_names_the_limit_test() ->
    ?assertError(
        {kura, {invalid_vector, any_embedding, ~"has more than 16000 dimensions"}},
        kura_query:distance(any_embedding, l2, lists:duplicate(16001, 1.0))
    ).

%%----------------------------------------------------------------------
%% kura_dialect_pg:prepare_query/1 dumps where values through the from
%% schema's field types
%%----------------------------------------------------------------------

dumped(Q) ->
    (kura_dialect_pg:prepare_query(Q))#kura_query.wheres.

where_stores_the_condition_as_given_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {active, '!=', ~"1"}),
    ?assertEqual([{active, '!=', ~"1"}], Q#kura_query.wheres).

where_dumps_an_enum_atom_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {status, active}),
    ?assertEqual([{status, ~"active"}], dumped(Q)).

where_dumps_enum_atoms_under_operators_and_logic_test() ->
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'or', [
            {status, in, [active, banned]},
            {'not', {status, '!=', inactive}},
            {status, not_in, [banned]}
        ]}
    ),
    ?assertEqual(
        [
            {'or', [
                {status, in, [~"active", ~"banned"]},
                {'not', {status, '!=', ~"inactive"}},
                {status, not_in, [~"banned"]}
            ]}
        ],
        dumped(Q)
    ).

where_casts_a_value_in_cast_form_test() ->
    Q0 = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q0, {id, ~"42"}),
    Q2 = kura_query:where(Q1, {age, between, {~"18", 30}}),
    Q3 = kura_query:where(Q2, {inserted_at, '>', ~"2026-01-02T03:04:05Z"}),
    ?assertEqual(
        [
            {id, 42},
            {age, between, {18, 30}},
            {inserted_at, '>', {{2026, 1, 2}, {3, 4, 5}}}
        ],
        dumped(Q3)
    ).

where_keeps_a_value_the_type_cannot_take_test() ->
    Q0 = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q0, {status, 42}),
    Q2 = kura_query:where(Q1, {age, ~"not a number"}),
    ?assertEqual([{status, 42}, {age, ~"not a number"}], dumped(Q2)).

where_leaves_unary_like_and_fragment_conditions_alone_test() ->
    Conds = [
        {status, is_nil},
        {name, like, ~"A%"},
        {fragment, ~"lower(name) = ?", [~"a"]},
        {status, undefined}
    ],
    Q = where_all(kura_query:from(kura_test_schema), Conds),
    ?assertEqual(Conds, dumped(Q)).

where_all(Q, []) -> Q;
where_all(Q, [C | Rest]) -> where_all(kura_query:where(Q, C), Rest).

where_binds_a_bare_table_or_cte_value_as_given_test() ->
    Q1 = kura_query:where(kura_query:from(users), {status, active}),
    Q2 = kura_query:where(kura_query:from(~"ranked"), {status, active}),
    ?assertEqual([{status, active}], dumped(Q1)),
    ?assertEqual([{status, active}], dumped(Q2)).

where_binds_a_field_the_from_schema_lacks_as_given_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q = kura_query:where(Q0, {status, active}),
    ?assertEqual([{status, active}], dumped(Q)).

from_accepts_a_binary_name_test() ->
    ?assertEqual(~"ranked", (kura_query:from(~"ranked"))#kura_query.from).

where_binds_jsonb_values_as_given_test() ->
    Values = [~"active", ~"123", ~"true", 123, true, #{a => 1}, [1, 2]],
    Q = where_all(kura_query:from(kura_test_schema), [{metadata, V} || V <- Values]),
    ?assertEqual([{metadata, V} || V <- Values], dumped(Q)).

where_binds_an_embed_value_as_given_test() ->
    Q = kura_query:where(kura_query:from(kura_test_profile), {address, #{city => ~"Oslo"}}),
    ?assertEqual([{address, #{city => ~"Oslo"}}], dumped(Q)).

where_keeps_a_datetime_with_an_offset_or_fraction_test() ->
    Values = [
        ~"2026-01-02T03:04:05+02:00",
        ~"2026-01-02T03:04:05-05:00",
        ~"2026-01-02T03:04:05.123Z",
        ~"2026-01-02T03:04:05,5",
        ~"2026-01-02T"
    ],
    Q = where_all(kura_query:from(kura_test_schema), [{inserted_at, '>', V} || V <- Values]),
    ?assertEqual([{inserted_at, '>', V} || V <- Values], dumped(Q)).

where_casts_a_space_separated_datetime_test() ->
    Q = kura_query:where(
        kura_query:from(kura_test_type_schema), {happened_at, ~"2026-01-02 03:04:05"}
    ),
    ?assertEqual([{happened_at, {{2026, 1, 2}, {3, 4, 5}}}], dumped(Q)).

where_binds_a_custom_value_as_given_test() ->
    Q0 = kura_query:from(kura_test_custom_schema),
    Q = where_all(Q0, [{price, {dollars, 5}}, {price, 500}, {email, ~"A@Example.com"}]),
    ?assertEqual(
        [{price, {dollars, 5}}, {price, 500}, {email, ~"A@Example.com"}], dumped(Q)
    ).

where_binds_strings_uuids_and_arrays_as_given_test() ->
    Q0 = kura_query:from(kura_test_type_schema),
    Conds = [
        {name, foo}, {ref_id, <<1:128>>}, {tags, [~"a"]}, {tags, in, [~"a"]}, {status, ~"active"}
    ],
    Q = where_all(Q0, Conds),
    ?assertEqual(Conds, dumped(Q)).

where_casts_a_binary_boolean_and_float_test() ->
    Q = where_all(kura_query:from(kura_test_schema), [{active, ~"true"}, {score, '>', ~"1.5"}]),
    ?assertEqual([{active, true}, {score, '>', 1.5}], dumped(Q)).

where_binds_a_long_precision_decimal_binary_as_given_test() ->
    Price = ~"12345678901234567.89",
    Conds = [{price, '=', Price}, {price, in, [Price, ~"0.1"]}, {price, between, {~"1", Price}}],
    Q = where_all(kura_query:from(kura_test_decimal_schema), Conds),
    ?assertEqual(Conds, dumped(Q)).

where_keeps_undefined_and_null_in_an_enum_in_list_test() ->
    Q = kura_query:where(
        kura_query:from(kura_test_schema), {status, in, [active, undefined, null]}
    ),
    ?assertEqual([{status, in, [~"active", undefined, null]}], dumped(Q)).

prepare_query_converts_nested_queries_against_their_own_schema_test() ->
    Sub = kura_query:where(kura_query:from(kura_test_schema), {active, ~"1"}),
    Q0 = kura_query:from(kura_test_post_schema),
    Q1 = kura_query:where(Q0, {author_id, in, {subquery, kura_query:select(Sub, [id])}}),
    Q2 = kura_query:where(Q1, {exists, {subquery, Sub}}),
    Q3 = kura_query:with_cte(Q2, ~"c", Sub),
    Q = kura_query:union(Q3, Sub),
    P = kura_dialect_pg:prepare_query(Q),
    [{author_id, in, {subquery, In}}, {exists, {subquery, Exists}}] = P#kura_query.wheres,
    [{~"c", Cte}] = P#kura_query.ctes,
    [{union, Union}] = P#kura_query.combinations,
    [?assertMatch(#kura_query{wheres = [{active, true}]}, N) || N <- [In, Exists, Cte, Union]].

prepare_query_is_idempotent_test() ->
    Q = where_all(kura_query:from(kura_test_schema), [
        {active, ~"true"}, {status, active}, {age, in, [~"1", 2]}
    ]),
    Once = kura_dialect_pg:prepare_query(Q),
    ?assertEqual(Once, kura_dialect_pg:prepare_query(Once)).

where_binds_an_oversized_number_binary_as_given_test() ->
    Long = binary:copy(~"9", 21),
    LongFloat = <<"1.", (binary:copy(~"5", 63))/binary>>,
    Q = where_all(kura_query:from(kura_test_schema), [
        {age, Long}, {id, in, [Long]}, {score, '>', LongFloat}
    ]),
    ?assertEqual([{age, Long}, {id, in, [Long]}, {score, '>', LongFloat}], dumped(Q)).

where_casts_a_number_binary_at_the_size_limit_test() ->
    Twenty = binary:copy(~"9", 20),
    Q = kura_query:where(kura_query:from(kura_test_schema), {id, Twenty}),
    ?assertEqual([{id, binary_to_integer(Twenty)}], dumped(Q)).

%%----------------------------------------------------------------------
%% Distance thresholds in where/2 and having/2
%%----------------------------------------------------------------------

where_accepts_a_distance_threshold_for_each_comparison_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    [
        ?_assertEqual(
            [{D, Op, 0.35}],
            (kura_query:where(kura_query:from(?VEC_SCHEMA), {D, Op, 0.35}))#kura_query.wheres
        )
     || Op <- ['<', '<=', '>', '>=']
    ].

having_accepts_a_distance_threshold_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Q = kura_query:having(kura_query:from(?VEC_SCHEMA), {D, '<', 1}),
    ?assertEqual([{D, '<', 1}], Q#kura_query.havings).

where_accepts_a_threshold_nested_in_a_boolean_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Cond = {'or', [{id, 1}, {'and', [{'not', {D, '>=', 2}}, {content, ~"x"}]}]},
    Q = kura_query:where(kura_query:from(?VEC_SCHEMA), Cond),
    ?assertEqual([Cond], Q#kura_query.wheres).

where_accepts_a_threshold_on_an_undimensioned_field_test() ->
    D = kura_query:distance(any_embedding, l2, lists:seq(1, 1536)),
    Q = kura_query:where(kura_query:from(?VEC_SCHEMA), {D, '<', 3}),
    ?assertEqual([{D, '<', 3}], Q#kura_query.wheres).

distance_threshold_is_checked_against_the_schema_test_() ->
    Q = kura_query:from(?VEC_SCHEMA),
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Short = kura_query:distance(embedding, cosine, [1, 2]),
    [
        ?_assertError(
            {kura, {distance_requires_schema, items}},
            kura_query:where(kura_query:from(items), {D, '<', 0.3})
        ),
        ?_assertError(
            {kura, {distance_requires_schema, items}},
            kura_query:having(kura_query:from(items), {D, '<', 0.3})
        ),
        ?_assertError(
            {kura, {not_a_vector_field, content}},
            kura_query:where(Q, {kura_query:distance(content, l2, [1, 2, 3]), '<', 0.3})
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:where(Q, {Short, '<', 0.3})
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:having(Q, {'not', {Short, '>', 0.3}})
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:where(Q, {'and', [{D, '<', 0.3}, {Short, '<', 0.3}]})
        ),
        ?_assertError(
            {kura, {invalid_distance, embedding}},
            kura_query:where(Q, {hand_built_distance(), '<', 0.3})
        ),
        ?_assertError(
            {kura, {invalid_distance, embedding}},
            kura_query:where(Q, {bad_metric_distance(), '<', 0.3})
        )
    ].

%% The reason names the field, never the threshold or the vector.
distance_threshold_must_be_a_number_test_() ->
    Q = kura_query:from(?VEC_SCHEMA),
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    [
        ?_assertError(
            {kura, {invalid_distance_threshold, embedding}},
            kura_query:where(Q, {D, '<', ill_typed(T)})
        )
     || T <- [~"0.3", nil, undefined, [0.3], {subquery, Q}, D]
    ] ++
        [
            ?_assertError(
                {kura, {invalid_distance_threshold, embedding}},
                kura_query:having(Q, {D, '>', ill_typed(~"1")})
            ),
            ?_assertError(
                {kura, invalid_distance},
                kura_query:where(Q, {non_atom_field_distance(), '<', ill_typed(~"1")})
            ),
            ?_assertError(
                {kura, {invalid_distance, embedding}},
                kura_query:where(Q, {bad_metric_distance(), '<', ill_typed(~"1")})
            ),
            ?_assertError(
                {kura, {distance_requires_schema, items}},
                kura_query:where(kura_query:from(items), {D, '<', ill_typed(~"1")})
            )
        ].

%% A distance threshold inside a subquery is checked against the
%% subquery's own schema, when the subquery is built and again when the
%% outer condition takes it.
where_checks_the_distances_of_a_subquery_test_() ->
    Short = {distance, embedding, l2, ~"[1.0,2.0]"},
    Sub = #kura_query{from = ?VEC_SCHEMA, select = [id], wheres = [{Short, '<', 1}]},
    Q = kura_query:from(?VEC_SCHEMA),
    [
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:where(Q, {id, in, {subquery, Sub}})
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:where(Q, {exists, {subquery, Sub}})
        ),
        ?_assertError(
            {kura, {vector_dimension_mismatch, embedding, 3, 2}},
            kura_query:having(Q, {not_exists, {subquery, Sub}})
        )
    ].

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) -> Term.

%% The compiler's check walks structure, not bound values; where/2 and
%% having/2 refuse a distance in a value position.
compiler_check_skips_value_positions_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Q = #kura_query{
        from = ?VEC_SCHEMA,
        wheres = [{id, in, [D]}, {[id, content], in, [{1, D}]}],
        havings = [{id, between, {D, D}}]
    },
    ?assertEqual(ok, kura_query:check_distances(Q)).

compiler_check_refuses_a_distance_bound_as_the_value_test_() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Plain = #kura_query{from = ?VEC_SCHEMA},
    Ordered = Plain#kura_query{order_bys = [{id, asc}]},
    Where = {kura, {distance_not_supported_here, where}},
    Having = {kura, {distance_not_supported_here, having}},
    [
        ?_assertError(
            Where, kura_query:check_distances(Plain#kura_query{wheres = [{content, '=', D}]})
        ),
        ?_assertError(Where, kura_query:check_distances(Plain#kura_query{wheres = [{content, D}]})),
        ?_assertError(
            Where, kura_query:check_distances(Ordered#kura_query{wheres = [{'not', {id, '>', D}}]})
        ),
        ?_assertError(
            Having,
            kura_query:check_distances(Plain#kura_query{havings = [{'or', [{id, 1}, {id, '<', D}]}]})
        ),
        ?_assertError(Having, kura_query:check_distances(Ordered#kura_query{havings = [{id, D}]}))
    ].

compiler_check_walks_thresholds_on_both_paths_test_() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Bad = {kura, {invalid_distance_threshold, embedding}},
    Plain = #kura_query{from = ?VEC_SCHEMA},
    Ordered = Plain#kura_query{order_bys = [{id, asc}]},
    Sub = Plain#kura_query{wheres = [{D, '<', nil}]},
    [
        ?_assertEqual(ok, kura_query:check_distances(Plain#kura_query{wheres = [{D, '<', 0.5}]})),
        ?_assertEqual(ok, kura_query:check_distances(Ordered#kura_query{havings = [{D, '>=', 1}]})),
        ?_assertError(Bad, kura_query:check_distances(Plain#kura_query{wheres = [{D, '>=', nil}]})),
        ?_assertError(
            Bad, kura_query:check_distances(Ordered#kura_query{havings = [{D, '>', nil}]})
        ),
        ?_assertError(
            Bad,
            kura_query:check_distances(Plain#kura_query{wheres = [{D, '<', 1}, {D, '<=', nil}]})
        ),
        ?_assertError(
            Bad,
            kura_query:check_distances(Plain#kura_query{wheres = [{not_exists, {subquery, Sub}}]})
        ),
        ?_assertError(
            Bad, kura_query:check_distances(Plain#kura_query{wheres = [{exists, {subquery, Sub}}]})
        )
    ].
