-module(kura_query_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

from_test() ->
    Q = kura_query:from(user),
    ?assertEqual(user, Q#kura_query.from).

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
