-module(kura_query_compiler_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% SELECT compilation
%%----------------------------------------------------------------------

simple_select_test() ->
    Q = kura_query:from(user),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\"">>, SQL),
    ?assertEqual([], Params).

select_fields_test() ->
    Q = kura_query:select(kura_query:from(user), [name, email]),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT \"name\", \"email\" FROM \"user\"">>, SQL).

select_from_schema_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"users\"">>, SQL).

%%----------------------------------------------------------------------
%% WHERE
%%----------------------------------------------------------------------

where_equality_test() ->
    Q = kura_query:where(kura_query:from(user), {name, <<"Alice">>}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\" = $1">>, SQL),
    ?assertEqual([<<"Alice">>], Params).

where_comparison_test() ->
    Q = kura_query:where(kura_query:from(user), {age, '>', 18}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" > $1">>, SQL),
    ?assertEqual([18], Params).

where_multiple_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:where(Q0, {age, '>', 18}),
    Q2 = kura_query:where(Q1, {active, true}),
    {SQL, Params} = kura_query_compiler:to_sql(Q2),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" > $1 AND \"active\" = $2">>, SQL),
    ?assertEqual([18, true], Params).

where_or_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'or', [{role, <<"admin">>}, {role, <<"mod">>}]}
    ),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE (\"role\" = $1 OR \"role\" = $2)">>, SQL),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

where_and_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'and', [{age, '>', 18}, {active, true}]}
    ),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE (\"age\" > $1 AND \"active\" = $2)">>, SQL),
    ?assertEqual([18, true], Params).

where_not_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'not', {active, false}}
    ),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE NOT (\"active\" = $1)">>, SQL),
    ?assertEqual([false], Params).

where_in_test() ->
    Q = kura_query:where(kura_query:from(user), {role, in, [<<"admin">>, <<"mod">>]}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"role\" IN ($1, $2)">>, SQL),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

where_not_in_test() ->
    Q = kura_query:where(kura_query:from(user), {role, not_in, [<<"banned">>]}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"role\" NOT IN ($1)">>, SQL),
    ?assertEqual([<<"banned">>], Params).

where_is_nil_test() ->
    Q = kura_query:where(kura_query:from(user), {deleted_at, is_nil}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"deleted_at\" IS NULL">>, SQL),
    ?assertEqual([], Params).

where_is_not_nil_test() ->
    Q = kura_query:where(kura_query:from(user), {email, is_not_nil}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"email\" IS NOT NULL">>, SQL),
    ?assertEqual([], Params).

where_between_test() ->
    Q = kura_query:where(kura_query:from(user), {age, between, {18, 65}}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" BETWEEN $1 AND $2">>, SQL),
    ?assertEqual([18, 65], Params).

where_like_test() ->
    Q = kura_query:where(kura_query:from(user), {name, like, <<"%alice%">>}),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\" LIKE $1">>, SQL),
    ?assertEqual([<<"%alice%">>], Params).

where_fragment_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {fragment, <<"lower(name) = ?">>, [<<"alice">>]}
    ),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE lower(name) = $1">>, SQL),
    ?assertEqual([<<"alice">>], Params).

%%----------------------------------------------------------------------
%% JOIN
%%----------------------------------------------------------------------

join_test() ->
    Q = kura_query:join(kura_query:from(user), inner, post, {id, user_id}),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"INNER JOIN \"post\"">>) =/= nomatch).

left_join_test() ->
    Q = kura_query:join(kura_query:from(user), left, post, {id, user_id}),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"LEFT JOIN \"post\"">>) =/= nomatch).

%%----------------------------------------------------------------------
%% ORDER BY
%%----------------------------------------------------------------------

order_by_test() ->
    Q = kura_query:order_by(kura_query:from(user), [{name, asc}, {age, desc}]),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"ORDER BY \"name\" ASC, \"age\" DESC">>) =/= nomatch).

%%----------------------------------------------------------------------
%% GROUP BY + HAVING
%%----------------------------------------------------------------------

group_by_test() ->
    Q = kura_query:group_by(kura_query:from(user), [role]),
    Q2 = kura_query:count(Q),
    {SQL, _} = kura_query_compiler:to_sql(Q2),
    ?assert(binary:match(SQL, <<"GROUP BY \"role\"">>) =/= nomatch).

having_test() ->
    Q = kura_query:group_by(kura_query:from(user), [role]),
    Q2 = kura_query:having(Q, {age, '>', 5}),
    Q3 = kura_query:count(Q2),
    {SQL, Params} = kura_query_compiler:to_sql(Q3),
    ?assert(binary:match(SQL, <<"HAVING \"age\" > $">>) =/= nomatch),
    ?assertEqual([5], Params).

%%----------------------------------------------------------------------
%% LIMIT / OFFSET
%%----------------------------------------------------------------------

limit_test() ->
    Q = kura_query:limit(kura_query:from(user), 10),
    {SQL, Params} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"LIMIT $1">>) =/= nomatch),
    ?assertEqual([10], Params).

limit_offset_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:limit(Q0, 10),
    Q2 = kura_query:offset(Q1, 20),
    {SQL, Params} = kura_query_compiler:to_sql(Q2),
    ?assert(binary:match(SQL, <<"LIMIT $1">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"OFFSET $2">>) =/= nomatch),
    ?assertEqual([10, 20], Params).

%%----------------------------------------------------------------------
%% DISTINCT
%%----------------------------------------------------------------------

distinct_test() ->
    Q = kura_query:distinct(kura_query:from(user)),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"SELECT DISTINCT *">>) =/= nomatch).

distinct_on_test() ->
    Q = kura_query:distinct(kura_query:from(user), [email]),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"SELECT DISTINCT ON (\"email\") *">>) =/= nomatch).

%%----------------------------------------------------------------------
%% LOCK
%%----------------------------------------------------------------------

lock_test() ->
    Q = kura_query:lock(kura_query:from(user), <<"FOR UPDATE">>),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assert(binary:match(SQL, <<"FOR UPDATE">>) =/= nomatch).

%%----------------------------------------------------------------------
%% PREFIX (schema)
%%----------------------------------------------------------------------

prefix_test() ->
    Q = kura_query:prefix(kura_query:from(user), <<"tenant_1">>),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"tenant_1\".\"user\"">>, SQL).

%%----------------------------------------------------------------------
%% Aggregates
%%----------------------------------------------------------------------

count_star_test() ->
    Q = kura_query:count(kura_query:from(user)),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT count(*) AS \"count\" FROM \"user\"">>, SQL).

count_field_test() ->
    Q = kura_query:count(kura_query:from(user), email),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT count(\"email\") AS \"count\" FROM \"user\"">>, SQL).

sum_test() ->
    Q = kura_query:sum(kura_query:from(user), score),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    ?assertEqual(<<"SELECT sum(\"score\") AS \"sum\" FROM \"user\"">>, SQL).

%%----------------------------------------------------------------------
%% INSERT
%%----------------------------------------------------------------------

insert_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema, [name, email], #{name => <<"Alice">>, email => <<"a@b.com">>}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) RETURNING *">>, SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>], Params).

%%----------------------------------------------------------------------
%% UPDATE
%%----------------------------------------------------------------------

update_test() ->
    {SQL, Params} = kura_query_compiler:update(
        kura_test_schema, [name], #{name => <<"Bob">>}, {id, 1}
    ),
    ?assertEqual(<<"UPDATE \"users\" SET \"name\" = $1 WHERE \"id\" = $2 RETURNING *">>, SQL),
    ?assertEqual([<<"Bob">>, 1], Params).

%%----------------------------------------------------------------------
%% DELETE
%%----------------------------------------------------------------------

delete_test() ->
    {SQL, Params} = kura_query_compiler:delete(kura_test_schema, id, 1),
    ?assertEqual(<<"DELETE FROM \"users\" WHERE \"id\" = $1 RETURNING *">>, SQL),
    ?assertEqual([1], Params).

%%----------------------------------------------------------------------
%% INSERT with ON CONFLICT (upsert)
%%----------------------------------------------------------------------

insert_on_conflict_nothing_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {email, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") DO NOTHING RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>], Params).

insert_on_conflict_constraint_nothing_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{constraint, <<"uq_email">>}, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT \"uq_email\" DO NOTHING RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>], Params).

insert_on_conflict_replace_all_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {email, replace_all}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") DO UPDATE SET \"name\" = $3 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Alice">>], Params).

insert_on_conflict_replace_fields_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {email, {replace, [name]}}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") DO UPDATE SET \"name\" = $3 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Alice">>], Params).

insert_no_opts_fallback_test() ->
    {SQL, Params} = kura_query_compiler:insert(
        kura_test_schema, [name], #{name => <<"Alice">>}, #{}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\") VALUES ($1) RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>], Params).

%%----------------------------------------------------------------------
%% UPDATE ALL
%%----------------------------------------------------------------------

update_all_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {age, '>', 18}),
    {SQL, Params} = kura_query_compiler:update_all(Q, #{active => false}),
    ?assertEqual(<<"UPDATE \"users\" SET \"active\" = $1 WHERE \"age\" > $2">>, SQL),
    ?assertEqual([false, 18], Params).

update_all_no_where_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, Params} = kura_query_compiler:update_all(Q, #{role => <<"guest">>}),
    ?assertEqual(<<"UPDATE \"users\" SET \"role\" = $1">>, SQL),
    ?assertEqual([<<"guest">>], Params).

%%----------------------------------------------------------------------
%% DELETE ALL
%%----------------------------------------------------------------------

delete_all_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {active, false}),
    {SQL, Params} = kura_query_compiler:delete_all(Q),
    ?assertEqual(<<"DELETE FROM \"users\" WHERE \"active\" = $1">>, SQL),
    ?assertEqual([false], Params).

delete_all_no_where_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, Params} = kura_query_compiler:delete_all(Q),
    ?assertEqual(<<"DELETE FROM \"users\"">>, SQL),
    ?assertEqual([], Params).

%%----------------------------------------------------------------------
%% INSERT ALL
%%----------------------------------------------------------------------

insert_all_test() ->
    Rows = [
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{name => <<"Bob">>, email => <<"b@b.com">>}
    ],
    {SQL, Params} = kura_query_compiler:insert_all(kura_test_schema, [name, email], Rows),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2), ($3, $4)">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Bob">>, <<"b@b.com">>], Params).

%%----------------------------------------------------------------------
%% Complex query
%%----------------------------------------------------------------------

complex_query_test() ->
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:select(Q, [name, email]),
    Q2 = kura_query:where(Q1, {age, '>', 18}),
    Q3 = kura_query:where(Q2, {active, true}),
    Q4 = kura_query:order_by(Q3, [{name, asc}]),
    Q5 = kura_query:limit(Q4, 10),
    Q6 = kura_query:offset(Q5, 5),
    {SQL, Params} = kura_query_compiler:to_sql(Q6),
    Expected = <<
        "SELECT \"name\", \"email\" FROM \"users\""
        " WHERE \"age\" > $1 AND \"active\" = $2"
        " ORDER BY \"name\" ASC"
        " LIMIT $3 OFFSET $4"
    >>,
    ?assertEqual(Expected, SQL),
    ?assertEqual([18, true, 10, 5], Params).
