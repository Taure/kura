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
