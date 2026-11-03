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

%% Distance thresholds are planned in #122. Until then a distance outside order_by and
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
%% where/2 dumps values through the from schema's field types
%%----------------------------------------------------------------------

where_dumps_an_enum_atom_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {status, active}),
    ?assertEqual([{status, ~"active"}], Q#kura_query.wheres).

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
        Q#kura_query.wheres
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
        Q3#kura_query.wheres
    ).

where_keeps_a_value_the_type_cannot_take_test() ->
    Q0 = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q0, {status, 42}),
    Q2 = kura_query:where(Q1, {age, ~"not a number"}),
    ?assertEqual([{status, 42}, {age, ~"not a number"}], Q2#kura_query.wheres).

where_leaves_unary_like_and_fragment_conditions_alone_test() ->
    Conds = [
        {status, is_nil},
        {name, like, ~"A%"},
        {fragment, ~"lower(name) = ?", [~"a"]},
        {status, undefined}
    ],
    Q = where_all(kura_query:from(kura_test_schema), Conds),
    ?assertEqual(Conds, Q#kura_query.wheres).

where_all(Q, []) -> Q;
where_all(Q, [C | Rest]) -> where_all(kura_query:where(Q, C), Rest).

where_binds_a_bare_table_or_cte_value_as_given_test() ->
    Q1 = kura_query:where(kura_query:from(users), {status, active}),
    Q2 = kura_query:where(kura_query:from(~"ranked"), {status, active}),
    ?assertEqual([{status, active}], Q1#kura_query.wheres),
    ?assertEqual([{status, active}], Q2#kura_query.wheres).

where_binds_a_field_the_from_schema_lacks_as_given_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q = kura_query:where(Q0, {status, active}),
    ?assertEqual([{status, active}], Q#kura_query.wheres).

from_accepts_a_binary_name_test() ->
    ?assertEqual(~"ranked", (kura_query:from(~"ranked"))#kura_query.from).
