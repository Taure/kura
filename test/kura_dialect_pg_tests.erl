-module(kura_dialect_pg_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% SELECT compilation
%%----------------------------------------------------------------------

simple_select_test() ->
    Q = kura_query:from(user),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\"">>, SQL),
    ?assertEqual([], Params).

select_fields_test() ->
    Q = kura_query:select(kura_query:from(user), [name, email]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT \"name\", \"email\" FROM \"user\"">>, SQL).

select_from_schema_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"users\"">>, SQL).

%%----------------------------------------------------------------------
%% Window functions
%%----------------------------------------------------------------------

window_row_number_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {rn,
            kura_query:over(row_number, #{
                partition_by => [category], order_by => [{amount, desc}]
            })}
    ]),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<
            "SELECT row_number() OVER (PARTITION BY \"category\" ORDER BY \"amount\" DESC) "
            "AS \"rn\" FROM \"sales\""
        >>,
        SQL
    ),
    ?assertEqual([], Params).

composite_in_where_test() ->
    Q = kura_query:where(
        kura_query:from(kura_test_composite_schema),
        {[org_id, user_id], in, [{<<"o1">>, <<"u1">>}, {<<"o2">>, <<"u2">>}]}
    ),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"(\"org_id\", \"user_id\") IN ((")),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"$1, $2), ($3, $4)")),
    ?assertEqual([<<"o1">>, <<"u1">>, <<"o2">>, <<"u2">>], Params).

single_col_composite_in_unwraps_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {[id], in, [{1}, {2}]}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"\"id\" IN ($1, $2)")),
    ?assertEqual([1, 2], Params).

window_agg_running_total_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {category, category},
        {total,
            kura_query:over({sum, amount}, #{
                partition_by => [category], order_by => [{day, asc}]
            })}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<
            "SELECT \"category\" AS \"category\", sum(\"amount\") OVER "
            "(PARTITION BY \"category\" ORDER BY \"day\" ASC) AS \"total\" FROM \"sales\""
        >>,
        SQL
    ).

window_rank_order_only_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {r, kura_query:over(rank, #{order_by => [{score, desc}]})}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT rank() OVER (ORDER BY \"score\" DESC) AS \"r\" FROM \"sales\"">>,
        SQL
    ).

window_count_partition_only_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {cnt, kura_query:over({count, '*'}, #{partition_by => [category]})}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT count(*) OVER (PARTITION BY \"category\") AS \"cnt\" FROM \"sales\"">>,
        SQL
    ).

window_dense_rank_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {dr, kura_query:over(dense_rank, #{order_by => [{score, desc}]})}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT dense_rank() OVER (ORDER BY \"score\" DESC) AS \"dr\" FROM \"sales\"">>,
        SQL
    ).

window_agg_avg_min_max_test() ->
    Expected = fun(Fn) ->
        <<"SELECT ", Fn/binary,
            "(\"amount\") OVER (PARTITION BY \"category\") AS \"v\" FROM \"sales\"">>
    end,
    Compile = fun(Agg) ->
        Q = kura_query:select_expr(kura_query:from(sales), [
            {v, kura_query:over({Agg, amount}, #{partition_by => [category]})}
        ]),
        element(1, kura_dialect_pg:to_sql(Q))
    end,
    ?assertEqual(Expected(<<"avg">>), Compile(avg)),
    ?assertEqual(Expected(<<"min">>), Compile(min)),
    ?assertEqual(Expected(<<"max">>), Compile(max)).

window_count_field_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {c, kura_query:over({count, amount}, #{partition_by => [category]})}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT count(\"amount\") OVER (PARTITION BY \"category\") AS \"c\" FROM \"sales\"">>,
        SQL
    ).

window_empty_spec_test() ->
    Q = kura_query:select_expr(kura_query:from(sales), [
        {rn, kura_query:over(row_number, #{})}
    ]),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT row_number() OVER () AS \"rn\" FROM \"sales\"">>, SQL).

%%----------------------------------------------------------------------
%% WHERE
%%----------------------------------------------------------------------

where_equality_test() ->
    Q = kura_query:where(kura_query:from(user), {name, <<"Alice">>}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\" = $1">>, SQL),
    ?assertEqual([<<"Alice">>], Params).

where_comparison_test() ->
    Q = kura_query:where(kura_query:from(user), {age, '>', 18}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" > $1">>, SQL),
    ?assertEqual([18], Params).

where_multiple_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:where(Q0, {age, '>', 18}),
    Q2 = kura_query:where(Q1, {active, true}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q2),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" > $1 AND \"active\" = $2">>, SQL),
    ?assertEqual([18, true], Params).

where_or_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'or', [{role, <<"admin">>}, {role, <<"mod">>}]}
    ),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE (\"role\" = $1 OR \"role\" = $2)">>, SQL),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

where_and_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'and', [{age, '>', 18}, {active, true}]}
    ),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE (\"age\" > $1 AND \"active\" = $2)">>, SQL),
    ?assertEqual([18, true], Params).

where_not_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {'not', {active, false}}
    ),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE NOT (\"active\" = $1)">>, SQL),
    ?assertEqual([false], Params).

where_in_test() ->
    Q = kura_query:where(kura_query:from(user), {role, in, [<<"admin">>, <<"mod">>]}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"role\" IN ($1, $2)">>, SQL),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

where_not_in_test() ->
    Q = kura_query:where(kura_query:from(user), {role, not_in, [<<"banned">>]}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"role\" NOT IN ($1)">>, SQL),
    ?assertEqual([<<"banned">>], Params).

where_is_nil_test() ->
    Q = kura_query:where(kura_query:from(user), {deleted_at, is_nil}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"deleted_at\" IS NULL">>, SQL),
    ?assertEqual([], Params).

where_is_not_nil_test() ->
    Q = kura_query:where(kura_query:from(user), {email, is_not_nil}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"email\" IS NOT NULL">>, SQL),
    ?assertEqual([], Params).

where_between_test() ->
    Q = kura_query:where(kura_query:from(user), {age, between, {18, 65}}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"age\" BETWEEN $1 AND $2">>, SQL),
    ?assertEqual([18, 65], Params).

where_like_test() ->
    Q = kura_query:where(kura_query:from(user), {name, like, <<"%alice%">>}),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\" LIKE $1">>, SQL),
    ?assertEqual([<<"%alice%">>], Params).

where_fragment_test() ->
    Q = kura_query:where(
        kura_query:from(user),
        {fragment, <<"lower(name) = ?">>, [<<"alice">>]}
    ),
    {SQL, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE lower(name) = $1">>, SQL),
    ?assertEqual([<<"alice">>], Params).

where_matches_test() ->
    Q = kura_query:where(kura_query:from(article), {body, matches, <<"erlang database">>}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"article\" WHERE to_tsvector(\"body\") @@ plainto_tsquery($1)">>,
        SQL
    ),
    ?assertEqual([<<"erlang database">>], Params).

%%----------------------------------------------------------------------
%% JOIN
%%----------------------------------------------------------------------

join_test() ->
    Q = kura_query:join(kura_query:from(user), inner, post, {id, user_id}),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"INNER JOIN \"post\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ON \"user\".\"id\" = \"post\".\"user_id\"">>) =/= nomatch).

left_join_test() ->
    Q = kura_query:join(kura_query:from(user), left, post, {id, user_id}),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"LEFT JOIN \"post\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ON \"user\".\"id\" = \"post\".\"user_id\"">>) =/= nomatch).

join_chained_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:join(Q0, inner, post, {id, user_id}),
    Q2 = kura_query:join(Q1, inner, comment, {id, post_id}),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q2),
    ?assert(binary:match(SQL, <<"ON \"user\".\"id\" = \"post\".\"user_id\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ON \"post\".\"id\" = \"comment\".\"post_id\"">>) =/= nomatch).

join_with_alias_test() ->
    Q = kura_query:join(kura_query:from(user), inner, post, {id, user_id}, p),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"INNER JOIN \"post\" AS \"p\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ON \"user\".\"id\" = \"p\".\"user_id\"">>) =/= nomatch).

join_schema_module_test() ->
    Q = kura_query:join(
        kura_query:from(kura_test_schema), inner, kura_test_post_schema, {id, author_id}
    ),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"INNER JOIN \"posts\"">>) =/= nomatch).

%%----------------------------------------------------------------------
%% ORDER BY
%%----------------------------------------------------------------------

order_by_test() ->
    Q = kura_query:order_by(kura_query:from(user), [{name, asc}, {age, desc}]),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"ORDER BY \"name\" ASC, \"age\" DESC">>) =/= nomatch).

%%----------------------------------------------------------------------
%% GROUP BY + HAVING
%%----------------------------------------------------------------------

group_by_test() ->
    Q = kura_query:group_by(kura_query:from(user), [role]),
    Q2 = kura_query:count(Q),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q2),
    ?assert(binary:match(SQL, <<"GROUP BY \"role\"">>) =/= nomatch).

having_test() ->
    Q = kura_query:group_by(kura_query:from(user), [role]),
    Q2 = kura_query:having(Q, {age, '>', 5}),
    Q3 = kura_query:count(Q2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q3),
    ?assert(binary:match(SQL, <<"HAVING \"age\" > $">>) =/= nomatch),
    ?assertEqual([5], Params).

%%----------------------------------------------------------------------
%% LIMIT / OFFSET
%%----------------------------------------------------------------------

limit_test() ->
    Q = kura_query:limit(kura_query:from(user), 10),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"LIMIT $1">>) =/= nomatch),
    ?assertEqual([10], Params).

limit_offset_test() ->
    Q0 = kura_query:from(user),
    Q1 = kura_query:limit(Q0, 10),
    Q2 = kura_query:offset(Q1, 20),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q2),
    ?assert(binary:match(SQL, <<"LIMIT $1">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"OFFSET $2">>) =/= nomatch),
    ?assertEqual([10, 20], Params).

%%----------------------------------------------------------------------
%% DISTINCT
%%----------------------------------------------------------------------

distinct_test() ->
    Q = kura_query:distinct(kura_query:from(user)),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"SELECT DISTINCT *">>) =/= nomatch).

distinct_on_test() ->
    Q = kura_query:distinct(kura_query:from(user), [email]),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"SELECT DISTINCT ON (\"email\") *">>) =/= nomatch).

%%----------------------------------------------------------------------
%% LOCK
%%----------------------------------------------------------------------

lock_test() ->
    Q = kura_query:lock(kura_query:from(user), <<"FOR UPDATE">>),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assert(binary:match(SQL, <<"FOR UPDATE">>) =/= nomatch).

%%----------------------------------------------------------------------
%% PREFIX (schema)
%%----------------------------------------------------------------------

prefix_test() ->
    Q = kura_query:prefix(kura_query:from(user), <<"tenant_1">>),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"tenant_1\".\"user\"">>, SQL).

%%----------------------------------------------------------------------
%% Aggregates
%%----------------------------------------------------------------------

count_star_test() ->
    Q = kura_query:count(kura_query:from(user)),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT count(*) AS \"count\" FROM \"user\"">>, SQL).

count_field_test() ->
    Q = kura_query:count(kura_query:from(user), email),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT count(\"email\") AS \"count\" FROM \"user\"">>, SQL).

sum_test() ->
    Q = kura_query:sum(kura_query:from(user), score),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT sum(\"score\") AS \"sum\" FROM \"user\"">>, SQL).

%%----------------------------------------------------------------------
%% INSERT
%%----------------------------------------------------------------------

insert_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
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
    {SQL, Params} = kura_dialect_pg:update(
        kura_test_schema, [name], #{name => <<"Bob">>}, [{id, 1}]
    ),
    ?assertEqual(<<"UPDATE \"users\" SET \"name\" = $1 WHERE \"id\" = $2 RETURNING *">>, SQL),
    ?assertEqual([<<"Bob">>, 1], Params).

update_composite_key_test() ->
    {SQL, Params} = kura_dialect_pg:update(
        kura_test_composite_schema, [role], #{role => <<"admin">>}, [
            {org_id, <<"o">>}, {user_id, <<"u">>}
        ]
    ),
    ?assertEqual(
        <<
            "UPDATE \"memberships\" SET \"role\" = $1 "
            "WHERE \"org_id\" = $2 AND \"user_id\" = $3 RETURNING *"
        >>,
        SQL
    ),
    ?assertEqual([<<"admin">>, <<"o">>, <<"u">>], Params).

%%----------------------------------------------------------------------
%% DELETE
%%----------------------------------------------------------------------

delete_test() ->
    {SQL, Params} = kura_dialect_pg:delete(kura_test_schema, [{id, 1}]),
    ?assertEqual(<<"DELETE FROM \"users\" WHERE \"id\" = $1 RETURNING *">>, SQL),
    ?assertEqual([1], Params).

delete_composite_key_test() ->
    {SQL, Params} = kura_dialect_pg:delete(kura_test_composite_schema, [
        {org_id, <<"o">>}, {user_id, <<"u">>}
    ]),
    ?assertEqual(
        <<"DELETE FROM \"memberships\" WHERE \"org_id\" = $1 AND \"user_id\" = $2 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"o">>, <<"u">>], Params).

%%----------------------------------------------------------------------
%% INSERT with ON CONFLICT (upsert)
%%----------------------------------------------------------------------

insert_on_conflict_nothing_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
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
    {SQL, Params} = kura_dialect_pg:insert(
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
    {SQL, Params} = kura_dialect_pg:insert(
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

insert_on_conflict_constraint_replace_all_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{constraint, <<"uq_email">>}, replace_all}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT \"uq_email\" DO UPDATE SET \"name\" = $3, \"email\" = $4 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Alice">>, <<"a@b.com">>], Params).

insert_on_conflict_constraint_replace_fields_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{constraint, <<"uq_email">>}, {replace, [name]}}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT \"uq_email\" DO UPDATE SET \"name\" = $3 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Alice">>], Params).

insert_on_conflict_replace_fields_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
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

insert_on_conflict_partial_index_nothing_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{columns, [email], #{where => <<"deleted_at IS NULL">>}}, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") WHERE deleted_at IS NULL DO NOTHING RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>], Params).

insert_on_conflict_partial_index_replace_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{
            on_conflict =>
                {{columns, [email], #{where => <<"deleted_at IS NULL">>}}, {replace, [name]}}
        }
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") WHERE deleted_at IS NULL DO UPDATE SET \"name\" = $3 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, <<"Alice">>], Params).

insert_on_conflict_partial_index_replace_all_test() ->
    {SQL, _Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{columns, [email], #{where => <<"deleted_at IS NULL">>}}, replace_all}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") WHERE deleted_at IS NULL DO UPDATE SET \"name\" = $3 RETURNING *">>,
        SQL
    ).

insert_on_conflict_empty_opts_has_no_predicate_test() ->
    {SQL, _Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{columns, [email], #{}}, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\") DO NOTHING RETURNING *">>,
        SQL
    ).

insert_on_conflict_malformed_opts_raises_test() ->
    Insert = fun(Opts) ->
        kura_dialect_pg:insert(
            kura_test_schema,
            [name, email],
            #{name => <<"Alice">>, email => <<"a@b.com">>},
            #{on_conflict => {{columns, [email], Opts}, nothing}}
        )
    end,
    ?assertError({kura, {invalid_on_conflict_opts, _}}, Insert(#{where => "scope IS NULL"})),
    ?assertError({kura, {invalid_on_conflict_opts, _}}, Insert(#{wehre => <<"scope IS NULL">>})).

insert_all_on_conflict_nothing_test() ->
    {SQL, Params} = kura_dialect_pg:insert_all(
        kura_test_schema,
        [name, email],
        [#{name => <<"A">>, email => <<"a@b.com">>}, #{name => <<"B">>, email => <<"b@b.com">>}],
        #{on_conflict => {email, nothing}}
    ),
    ?assertEqual(
        <<
            "INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2), ($3, $4) "
            "ON CONFLICT (\"email\") DO NOTHING"
        >>,
        SQL
    ),
    ?assertEqual([<<"A">>, <<"a@b.com">>, <<"B">>, <<"b@b.com">>], Params).

insert_all_on_conflict_replaces_from_excluded_test() ->
    %% One DO UPDATE clause covers every row, so the SET values must come from
    %% EXCLUDED rather than from any single row's parameters.
    {SQL, Params} = kura_dialect_pg:insert_all(
        kura_test_schema,
        [name, email],
        [#{name => <<"A">>, email => <<"a@b.com">>}, #{name => <<"B">>, email => <<"b@b.com">>}],
        #{on_conflict => {email, replace_all}}
    ),
    ?assertEqual(
        <<
            "INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2), ($3, $4) "
            "ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
        >>,
        SQL
    ),
    ?assertEqual([<<"A">>, <<"a@b.com">>, <<"B">>, <<"b@b.com">>], Params).

insert_on_conflict_guard_test() ->
    {SQL, _Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => ~"Alice", email => ~"a@b.com"},
        #{on_conflict => {email, replace_all, #{guard => [email]}}}
    ),
    ?assertEqual(
        <<
            "INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) "
            "ON CONFLICT (\"email\") DO UPDATE SET \"name\" = $3 "
            "WHERE \"users\".\"email\" = EXCLUDED.\"email\" RETURNING *"
        >>,
        SQL
    ).

insert_all_on_conflict_guard_test() ->
    {SQL, _Params} = kura_dialect_pg:insert_all(
        kura_test_schema,
        [name, email],
        [#{name => ~"A", email => ~"a@b.com"}, #{name => ~"B", email => ~"b@b.com"}],
        #{on_conflict => {email, replace_all, #{guard => [email]}}}
    ),
    ?assertEqual(
        <<
            "INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2), ($3, $4) "
            "ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\" "
            "WHERE \"users\".\"email\" = EXCLUDED.\"email\""
        >>,
        SQL
    ).

insert_on_conflict_guard_rejects_other_opts_test() ->
    ?assertError(
        {kura, {invalid_on_conflict_opts, _}},
        kura_dialect_pg:insert(
            kura_test_schema,
            [name, email],
            #{name => ~"Alice", email => ~"a@b.com"},
            #{on_conflict => {email, replace_all, #{guard => [email], extra => true}}}
        )
    ).

update_numbers_key_clauses_after_the_set_test() ->
    {SQL, Params} = kura_dialect_pg:update(
        t, [name], #{name => ~"n"}, [{org_id, ~"o"}, {user_id, ~"u"}, {lock_version, 3}]
    ),
    ?assertEqual(
        ~"UPDATE \"t\" SET \"name\" = $1 WHERE \"org_id\" = $2 AND \"user_id\" = $3 AND \"lock_version\" = $4 RETURNING *",
        SQL
    ),
    ?assertEqual([~"n", ~"o", ~"u", 3], Params).

insert_all_on_conflict_partial_index_with_returning_test() ->
    {SQL, _} = kura_dialect_pg:insert_all(
        kura_test_schema,
        [name, email],
        [#{name => <<"A">>, email => <<"a@b.com">>}],
        #{
            on_conflict =>
                {{columns, [email], #{where => <<"deleted_at IS NULL">>}}, {replace, [name]}},
            returning => true
        }
    ),
    ?assertEqual(
        <<
            "INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) "
            "ON CONFLICT (\"email\") WHERE deleted_at IS NULL "
            "DO UPDATE SET \"name\" = EXCLUDED.\"name\" RETURNING *"
        >>,
        SQL
    ).

insert_on_conflict_columns_nothing_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{columns, [name, email]}, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"name\", \"email\") DO NOTHING RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>], Params).

insert_on_conflict_columns_replace_all_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email, age],
        #{name => <<"Alice">>, email => <<"a@b.com">>, age => 30},
        #{on_conflict => {{columns, [name, email]}, replace_all}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\", \"age\") VALUES ($1, $2, $3) ON CONFLICT (\"name\", \"email\") DO UPDATE SET \"age\" = $4 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, 30, 30], Params).

insert_on_conflict_columns_replace_fields_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email, age],
        #{name => <<"Alice">>, email => <<"a@b.com">>, age => 30},
        #{on_conflict => {{columns, [name, email]}, {replace, [age]}}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\", \"age\") VALUES ($1, $2, $3) ON CONFLICT (\"name\", \"email\") DO UPDATE SET \"age\" = $4 RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"Alice">>, <<"a@b.com">>, 30, 30], Params).

insert_on_conflict_columns_preserves_order_test() ->
    %% Column order in the conflict target must match the index definition;
    %% reversing should produce a different SQL clause.
    {SQL, _Params} = kura_dialect_pg:insert(
        kura_test_schema,
        [name, email],
        #{name => <<"Alice">>, email => <<"a@b.com">>},
        #{on_conflict => {{columns, [email, name]}, nothing}}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) ON CONFLICT (\"email\", \"name\") DO NOTHING RETURNING *">>,
        SQL
    ).

insert_no_opts_fallback_test() ->
    {SQL, Params} = kura_dialect_pg:insert(
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
    {SQL, Params} = kura_dialect_pg:update_all(Q, #{active => false}),
    ?assertEqual(<<"UPDATE \"users\" SET \"active\" = $1 WHERE \"age\" > $2">>, SQL),
    ?assertEqual([false, 18], Params).

update_all_no_where_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, Params} = kura_dialect_pg:update_all(Q, #{role => <<"guest">>}),
    ?assertEqual(<<"UPDATE \"users\" SET \"role\" = $1">>, SQL),
    ?assertEqual([<<"guest">>], Params).

%%----------------------------------------------------------------------
%% DELETE ALL
%%----------------------------------------------------------------------

delete_all_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {active, false}),
    {SQL, Params} = kura_dialect_pg:delete_all(Q),
    ?assertEqual(<<"DELETE FROM \"users\" WHERE \"active\" = $1">>, SQL),
    ?assertEqual([false], Params).

delete_all_no_where_test() ->
    Q = kura_query:from(kura_test_schema),
    {SQL, Params} = kura_dialect_pg:delete_all(Q),
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
    {SQL, Params} = kura_dialect_pg:insert_all(kura_test_schema, [name, email], Rows),
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
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q6),
    Expected = <<
        "SELECT \"name\", \"email\" FROM \"users\""
        " WHERE \"age\" > $1 AND \"active\" = $2"
        " ORDER BY \"name\" ASC"
        " LIMIT $3 OFFSET $4"
    >>,
    ?assertEqual(Expected, SQL),
    ?assertEqual([18, true, 10, 5], Params).

%%----------------------------------------------------------------------
%% to_sql_from/2 (keystone refactor)
%%----------------------------------------------------------------------

to_sql_from_start_counter_test() ->
    Q = kura_query:where(kura_query:from(user), {name, <<"Alice">>}),
    {SQL, Params, NextCounter} = kura_dialect_pg:to_sql_from(Q, 5),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\" = $5">>, SQL),
    ?assertEqual([<<"Alice">>], Params),
    ?assertEqual(6, NextCounter).

%%----------------------------------------------------------------------
%% Subqueries in WHERE
%%----------------------------------------------------------------------

subquery_in_test() ->
    SubQ = kura_query:select(kura_query:from(post), [user_id]),
    Q = kura_query:where(kura_query:from(user), {id, in, {subquery, SubQ}}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"post\")">>,
        SQL
    ),
    ?assertEqual([], Params).

subquery_in_with_where_test() ->
    SubQ = kura_query:where(
        kura_query:select(kura_query:from(post), [user_id]),
        {published, true}
    ),
    Q = kura_query:where(kura_query:from(user), {id, in, {subquery, SubQ}}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"post\" WHERE \"published\" = $1)">>,
        SQL
    ),
    ?assertEqual([true], Params).

exists_subquery_test() ->
    SubQ = kura_query:where(kura_query:from(post), {user_id, 1}),
    Q = kura_query:where(kura_query:from(user), {exists, {subquery, SubQ}}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE EXISTS (SELECT * FROM \"post\" WHERE \"user_id\" = $1)">>,
        SQL
    ),
    ?assertEqual([1], Params).

not_exists_subquery_test() ->
    SubQ = kura_query:where(kura_query:from(post), {user_id, 1}),
    Q = kura_query:where(kura_query:from(user), {not_exists, {subquery, SubQ}}),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE NOT EXISTS (SELECT * FROM \"post\" WHERE \"user_id\" = $1)">>,
        SQL
    ),
    ?assertEqual([1], Params).

%%----------------------------------------------------------------------
%% Window Functions (select_expr)
%%----------------------------------------------------------------------

select_expr_row_number_test() ->
    Q = kura_query:select_expr(kura_query:from(user), [
        {row_num, {fragment, <<"ROW_NUMBER() OVER (ORDER BY id)">>, []}}
    ]),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT ROW_NUMBER() OVER (ORDER BY id) AS \"row_num\" FROM \"user\"">>,
        SQL
    ),
    ?assertEqual([], Params).

select_expr_sum_over_test() ->
    Q = kura_query:select_expr(kura_query:from(user), [
        {total, {fragment, <<"SUM(score) OVER (PARTITION BY role)">>, []}}
    ]),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT SUM(score) OVER (PARTITION BY role) AS \"total\" FROM \"user\"">>,
        SQL
    ),
    ?assertEqual([], Params).

select_expr_with_params_test() ->
    Q = kura_query:select_expr(kura_query:from(user), [
        {result, {fragment, <<"CASE WHEN age > ? THEN 'senior' ELSE 'junior' END">>, [50]}}
    ]),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT CASE WHEN age > $1 THEN 'senior' ELSE 'junior' END AS \"result\" FROM \"user\"">>,
        SQL
    ),
    ?assertEqual([50], Params).

%%----------------------------------------------------------------------
%% CTEs (WITH clause)
%%----------------------------------------------------------------------

single_cte_test() ->
    CteQ = kura_query:where(kura_query:from(user), {active, true}),
    Q = kura_query:with_cte(kura_query:from(active_users), <<"active_users">>, CteQ),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"WITH \"active_users\" AS (SELECT * FROM \"user\" WHERE \"active\" = $1) SELECT * FROM \"active_users\"">>,
        SQL
    ),
    ?assertEqual([true], Params).

multiple_ctes_test() ->
    CteQ1 = kura_query:where(kura_query:from(user), {active, true}),
    CteQ2 = kura_query:where(kura_query:from(user), {role, <<"admin">>}),
    Q0 = kura_query:from(active_users),
    Q1 = kura_query:with_cte(Q0, <<"active_users">>, CteQ1),
    Q2 = kura_query:with_cte(Q1, <<"admin_users">>, CteQ2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q2),
    ?assertEqual(
        <<"WITH \"active_users\" AS (SELECT * FROM \"user\" WHERE \"active\" = $1), \"admin_users\" AS (SELECT * FROM \"user\" WHERE \"role\" = $2) SELECT * FROM \"active_users\"">>,
        SQL
    ),
    ?assertEqual([true, <<"admin">>], Params).

cte_name_is_quoted_test() ->
    CteQ = kura_query:from(user),
    Q = kura_query:with_cte(kura_query:from(evil), <<"evil\") SELECT * FROM secrets --">>, CteQ),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"WITH \"evil\"\") SELECT * FROM secrets --\" AS (SELECT * FROM \"user\") SELECT * FROM \"evil\"">>,
        SQL
    ).

%%----------------------------------------------------------------------
%% Identifier quoting
%%----------------------------------------------------------------------

quote_ident_doubles_embedded_quotes_test() ->
    Q = kura_query:where(kura_query:from(user), {'name" = \'x\' OR "1', 1}),
    {<<SQL/binary>>, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"name\"\" = 'x' OR \"\"1\" = $1">>, SQL).

%%----------------------------------------------------------------------
%%----------------------------------------------------------------------
%% UNION / INTERSECT / EXCEPT
%%----------------------------------------------------------------------

union_test() ->
    Q1 = kura_query:where(kura_query:from(user), {role, <<"admin">>}),
    Q2 = kura_query:where(kura_query:from(user), {role, <<"mod">>}),
    Q = kura_query:union(Q1, Q2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"role\" = $1 UNION SELECT * FROM \"user\" WHERE \"role\" = $2">>,
        SQL
    ),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

union_all_test() ->
    Q1 = kura_query:where(kura_query:from(user), {role, <<"admin">>}),
    Q2 = kura_query:where(kura_query:from(user), {role, <<"mod">>}),
    Q = kura_query:union_all(Q1, Q2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"role\" = $1 UNION ALL SELECT * FROM \"user\" WHERE \"role\" = $2">>,
        SQL
    ),
    ?assertEqual([<<"admin">>, <<"mod">>], Params).

intersect_test() ->
    Q1 = kura_query:where(kura_query:from(user), {active, true}),
    Q2 = kura_query:where(kura_query:from(user), {role, <<"admin">>}),
    Q = kura_query:intersect(Q1, Q2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"active\" = $1 INTERSECT SELECT * FROM \"user\" WHERE \"role\" = $2">>,
        SQL
    ),
    ?assertEqual([true, <<"admin">>], Params).

except_test() ->
    Q1 = kura_query:where(kura_query:from(user), {active, true}),
    Q2 = kura_query:where(kura_query:from(user), {role, <<"banned">>}),
    Q = kura_query:except(Q1, Q2),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"active\" = $1 EXCEPT SELECT * FROM \"user\" WHERE \"role\" = $2">>,
        SQL
    ),
    ?assertEqual([true, <<"banned">>], Params).

chained_union_test() ->
    Q1 = kura_query:where(kura_query:from(user), {role, <<"a">>}),
    Q2 = kura_query:where(kura_query:from(user), {role, <<"b">>}),
    Q3 = kura_query:where(kura_query:from(user), {role, <<"c">>}),
    Q = kura_query:union(kura_query:union(Q1, Q2), Q3),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"role\" = $1 UNION SELECT * FROM \"user\" WHERE \"role\" = $2 UNION SELECT * FROM \"user\" WHERE \"role\" = $3">>,
        SQL
    ),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], Params).

%%----------------------------------------------------------------------
%% Query Scopes
%%----------------------------------------------------------------------

scope_single_test() ->
    Active = fun(Q) -> kura_query:where(Q, {active, true}) end,
    Q = kura_query:scope(kura_query:from(user), Active),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT * FROM \"user\" WHERE \"active\" = $1">>, SQL),
    ?assertEqual([true], Params).

scope_chained_test() ->
    Active = fun(Q) -> kura_query:where(Q, {active, true}) end,
    Admin = fun(Q) -> kura_query:where(Q, {role, <<"admin">>}) end,
    Q = kura_query:scope(kura_query:scope(kura_query:from(user), Active), Admin),
    {<<SQL/binary>>, Params} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<"SELECT * FROM \"user\" WHERE \"active\" = $1 AND \"role\" = $2">>,
        SQL
    ),
    ?assertEqual([true, <<"admin">>], Params).

%%----------------------------------------------------------------------
%% INSERT ALL with RETURNING
%%----------------------------------------------------------------------

insert_all_returning_true_test() ->
    Rows = [#{name => <<"A">>, email => <<"a@b.com">>}],
    {SQL, Params} = kura_dialect_pg:insert_all(
        kura_test_schema, [name, email], Rows, #{returning => true}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) RETURNING *">>,
        SQL
    ),
    ?assertEqual([<<"A">>, <<"a@b.com">>], Params).

insert_all_returning_fields_test() ->
    Rows = [#{name => <<"A">>, email => <<"a@b.com">>}],
    {SQL, Params} = kura_dialect_pg:insert_all(
        kura_test_schema, [name, email], Rows, #{returning => [id, name]}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2) RETURNING \"id\", \"name\"">>,
        SQL
    ),
    ?assertEqual([<<"A">>, <<"a@b.com">>], Params).

insert_all_no_returning_test() ->
    Rows = [#{name => <<"A">>, email => <<"a@b.com">>}],
    {SQL, _} = kura_dialect_pg:insert_all(
        kura_test_schema, [name, email], Rows, #{}
    ),
    ?assertEqual(
        <<"INSERT INTO \"users\" (\"name\", \"email\") VALUES ($1, $2)">>,
        SQL
    ).

format_default_map_is_a_jsonb_literal_test() ->
    ?assertEqual(
        <<"'{\"a\":1}'::jsonb">>, kura_dialect_pg:format_default(#{~"a" => 1})
    ).

format_default_list_is_a_quoted_jsonb_literal_test() ->
    ?assertEqual(
        <<"'[\"it''s\",2]'::jsonb">>, kura_dialect_pg:format_default([~"it's", 2])
    ).

%%----------------------------------------------------------------------
%% A table named by a binary
%%----------------------------------------------------------------------

binary_table_insert_all_test() ->
    {SQL, Params} = kura_dialect_pg:insert_all(~"posts_tags", [post_id], [#{post_id => 1}]),
    ?assertEqual(~"INSERT INTO \"posts_tags\" (\"post_id\") VALUES ($1)", iolist_to_binary(SQL)),
    ?assertEqual([1], Params).

binary_table_select_test() ->
    {SQL, []} = kura_dialect_pg:to_sql(#kura_query{from = ~"posts_tags"}),
    ?assertEqual(~"SELECT * FROM \"posts_tags\"", iolist_to_binary(SQL)).

%%----------------------------------------------------------------------
%% Vector distance
%%----------------------------------------------------------------------

-define(VEC_SCHEMA, kura_test_vector_schema).

distance_operator_per_metric_test_() ->
    [
        ?_assertEqual(
            {
                <<"SELECT \"id\" AS \"id\", \"embedding\" ", Op/binary,
                    " $1 AS \"dist\" FROM \"kura_vector_items\" ORDER BY \"embedding\" ", Op/binary,
                    " $2 ASC">>,
                [~"[1.0,2.0,3.0]", ~"[1.0,2.0,3.0]"]
            },
            begin
                D = kura_query:distance(embedding, Metric, [1, 2, 3]),
                Q = kura_query:select_expr(kura_query:from(?VEC_SCHEMA), [{id, id}, {dist, D}]),
                kura_dialect_pg:to_sql(kura_query:order_by(Q, [{D, asc}]))
            end
        )
     || {Metric, Op} <- [{l2, ~"<->"}, {cosine, ~"<=>"}, {inner_product, ~"<#>"}]
    ].

distance_order_by_mixes_with_fields_test() ->
    D = kura_query:distance(embedding, cosine, [0.5, 0, 1]),
    Q = kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{id, desc}, {D, asc}, {content, asc}]),
    ?assertEqual(
        {
            <<
                "SELECT * FROM \"kura_vector_items\" ORDER BY \"id\" DESC, "
                "\"embedding\" <=> $1 ASC, \"content\" ASC"
            >>,
            [~"[0.5,0.0,1.0]"]
        },
        kura_dialect_pg:to_sql(Q)
    ).

%% Each clause numbers its placeholders in SQL order: select, where,
%% having, order by, limit, offset. The params must come out in that order.
distance_params_are_numbered_between_having_and_limit_test() ->
    D1 = kura_query:distance(embedding, l2, [1, 2, 3]),
    D2 = kura_query:distance(any_embedding, inner_product, [4, 5]),
    Q0 = kura_query:select_expr(kura_query:from(?VEC_SCHEMA), [{content, content}, {dist, D1}]),
    Q1 = kura_query:where(Q0, {content, like, ~"a%"}),
    Q2 = kura_query:where(Q1, {fragment, ~"id BETWEEN ? AND ?", [1, 99]}),
    Q3 = kura_query:having(
        kura_query:group_by(Q2, [content, embedding]), {fragment, ~"count(*) > ?", [0]}
    ),
    Q4 = kura_query:order_by(Q3, [{D2, asc}, {D1, desc}]),
    Q5 = kura_query:offset(kura_query:limit(Q4, 10), 20),
    ?assertEqual(
        {
            <<
                "SELECT \"content\" AS \"content\", \"embedding\" <-> $1 AS \"dist\" "
                "FROM \"kura_vector_items\" WHERE \"content\" LIKE $2 AND id BETWEEN $3 AND $4 "
                "GROUP BY \"content\", \"embedding\" HAVING count(*) > $5 "
                "ORDER BY \"any_embedding\" <#> $6 ASC, \"embedding\" <-> $7 DESC "
                "LIMIT $8 OFFSET $9"
            >>,
            [~"[1.0,2.0,3.0]", ~"a%", 1, 99, 0, ~"[4.0,5.0]", ~"[1.0,2.0,3.0]", 10, 20]
        },
        kura_dialect_pg:to_sql(Q5)
    ).

distance_in_a_cte_continues_the_numbering_test() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Nearest = kura_query:limit(kura_query:order_by(kura_query:from(?VEC_SCHEMA), [{D, asc}]), 5),
    Q = kura_query:where(
        kura_query:with_cte(kura_query:from(?VEC_SCHEMA), ~"nearest", Nearest), {id, 7}
    ),
    ?assertEqual(
        {
            <<
                "WITH \"nearest\" AS (SELECT * FROM \"kura_vector_items\" "
                "ORDER BY \"embedding\" <=> $1 ASC LIMIT $2) "
                "SELECT * FROM \"kura_vector_items\" WHERE \"id\" = $3"
            >>,
            [~"[1.0,2.0,3.0]", 5, 7]
        },
        kura_dialect_pg:to_sql(Q)
    ).

distance_before_a_select_fragment_keeps_param_order_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Q0 = kura_query:select_expr(kura_query:from(?VEC_SCHEMA), [
        {dist, D}, {score, {fragment, ~"? + ?", [7, 8]}}
    ]),
    ?assertEqual(
        {
            <<
                "SELECT \"embedding\" <-> $1 AS \"dist\", $2 + $3 AS \"score\" "
                "FROM \"kura_vector_items\" LIMIT $4"
            >>,
            [~"[1.0,2.0,3.0]", 7, 8, 3]
        },
        kura_dialect_pg:to_sql(kura_query:limit(Q0, 3))
    ).

%%----------------------------------------------------------------------
%% Columns after a join
%%----------------------------------------------------------------------

join_qualifies_from_schema_columns_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q1 = kura_query:select(Q0, [id, title, name]),
    Q2 = kura_query:where(Q1, {id, 1}),
    Q3 = kura_query:order_by(Q2, [{inserted_at, desc}]),
    {SQL, Params} = kura_dialect_pg:to_sql(Q3),
    ?assertEqual(
        <<
            "SELECT \"posts\".\"id\", \"posts\".\"title\", \"name\" FROM \"posts\" "
            "INNER JOIN \"users\" ON \"posts\".\"author_id\" = \"users\".\"id\" "
            "WHERE \"posts\".\"id\" = $1 ORDER BY \"posts\".\"inserted_at\" DESC"
        >>,
        SQL
    ),
    ?assertEqual([1], Params).

join_qualifies_group_by_aggregates_and_nested_conditions_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q1 = kura_query:select(Q0, [author_id, {count, id}]),
    Q2 = kura_query:where(Q1, {'or', [{id, in, [1, 2]}, {'not', {title, is_nil}}]}),
    Q3 = kura_query:group_by(Q2, [author_id]),
    {SQL, _} = kura_dialect_pg:to_sql(Q3),
    ?assertEqual(
        <<
            "SELECT \"posts\".\"author_id\", count(\"posts\".\"id\") AS \"count\" FROM \"posts\" "
            "INNER JOIN \"users\" ON \"posts\".\"author_id\" = \"users\".\"id\" "
            "WHERE (\"posts\".\"id\" IN ($1, $2) OR NOT (\"posts\".\"title\" IS NULL)) "
            "GROUP BY \"posts\".\"author_id\""
        >>,
        SQL
    ).

join_select_expr_and_distinct_are_qualified_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q1 = kura_query:distinct(
        kura_query:select_expr(Q0, [{post_id, id}, {user_id, {fragment, ~"\"users\".\"id\"", []}}]),
        [id]
    ),
    {SQL, _} = kura_dialect_pg:to_sql(Q1),
    ?assertEqual(
        <<
            "SELECT DISTINCT ON (\"posts\".\"id\") \"posts\".\"id\" AS \"post_id\", "
            "\"users\".\"id\" AS \"user_id\" FROM \"posts\" "
            "INNER JOIN \"users\" ON \"posts\".\"author_id\" = \"users\".\"id\""
        >>,
        SQL
    ).

no_join_leaves_columns_bare_test() ->
    Q = kura_query:where(kura_query:select(kura_query:from(kura_test_post_schema), [id]), {id, 1}),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(<<"SELECT \"id\" FROM \"posts\" WHERE \"id\" = $1">>, SQL).

join_from_bare_table_leaves_columns_bare_test() ->
    Q = kura_query:where(
        kura_query:join(kura_query:from(user), inner, post, {id, user_id}), {id, 1}
    ),
    {SQL, _} = kura_dialect_pg:to_sql(Q),
    ?assertEqual(
        <<
            "SELECT * FROM \"user\" INNER JOIN \"post\" ON \"user\".\"id\" = \"post\".\"user_id\" "
            "WHERE \"id\" = $1"
        >>,
        SQL
    ).

join_leaves_the_attribute_tenant_field_bare_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q = kura_query:where(Q0, {author_id, 7}),
    {SQL, _} = kura_tenant:with_tenant({attribute, {author_id, 7}}, fun() ->
        kura_dialect_pg:to_sql(Q)
    end),
    ?assertEqual(
        <<
            "SELECT * FROM \"posts\" "
            "INNER JOIN \"users\" ON \"posts\".\"author_id\" = \"users\".\"id\" "
            "WHERE \"author_id\" = $1"
        >>,
        SQL
    ).

join_leaves_an_order_by_alias_bare_test() ->
    Q0 = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    Q1 = kura_query:select_expr(Q0, [{title, {fragment, ~"upper(\"posts\".\"title\")", []}}]),
    Q2 = kura_query:order_by(Q1, [{title, asc}, {inserted_at, desc}]),
    {SQL, _} = kura_dialect_pg:to_sql(Q2),
    ?assertEqual(
        <<
            "SELECT upper(\"posts\".\"title\") AS \"title\" FROM \"posts\" "
            "INNER JOIN \"users\" ON \"posts\".\"author_id\" = \"users\".\"id\" "
            "ORDER BY \"title\" ASC, \"posts\".\"inserted_at\" DESC"
        >>,
        SQL
    ).

qualified_columns_test_() ->
    Joined = kura_query:join(kura_query:from(kura_test_post_schema), inner, kura_test_schema, {
        author_id, id
    }),
    [
        ?_assertEqual(
            undefined, kura_dialect:qualified_columns(kura_query:from(kura_test_post_schema))
        ),
        ?_assertEqual(
            undefined,
            kura_dialect:qualified_columns(
                kura_query:join(kura_query:from(user), inner, post, {id, user_id})
            )
        ),
        ?_assertMatch(
            {~"posts", #{id := id, author_id := integer}},
            kura_dialect:qualified_columns(Joined)
        ),
        ?_test(begin
            {~"posts", Fields} = kura_tenant:with_tenant({attribute, {author_id, 1}}, fun() ->
                kura_dialect:qualified_columns(Joined)
            end),
            ?assertNot(is_map_key(author_id, Fields))
        end)
    ].

distance_threshold_operator_per_comparison_test_() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    [
        ?_assertEqual(
            {
                <<"SELECT * FROM \"kura_vector_items\" WHERE \"embedding\" <=> $1 ",
                    (atom_to_binary(Op))/binary, " $2">>,
                [~"[1.0,2.0,3.0]", 0.35]
            },
            kura_dialect_pg:to_sql(kura_query:where(kura_query:from(?VEC_SCHEMA), {D, Op, 0.35}))
        )
     || Op <- ['<', '<=', '>', '>=']
    ].

distance_threshold_operator_per_metric_test_() ->
    [
        ?_assertEqual(
            {
                <<"SELECT * FROM \"kura_vector_items\" WHERE \"embedding\" ", Op/binary,
                    " $1 < $2">>,
                [~"[1.0,2.0,3.0]", 1]
            },
            kura_dialect_pg:to_sql(
                kura_query:where(
                    kura_query:from(?VEC_SCHEMA),
                    {kura_query:distance(embedding, Metric, [1, 2, 3]), '<', 1}
                )
            )
        )
     || {Metric, Op} <- [{l2, ~"<->"}, {cosine, ~"<=>"}, {inner_product, ~"<#>"}]
    ].

%% The threshold's two params sit in the where's place in the numbering,
%% before the having, order by, limit and offset that follow.
distance_threshold_params_are_numbered_with_order_by_and_limit_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Far = kura_query:distance(any_embedding, inner_product, [4, 5]),
    Q0 = kura_query:select_expr(kura_query:from(?VEC_SCHEMA), [{content, content}, {dist, D}]),
    Q1 = kura_query:where(Q0, {content, like, ~"a%"}),
    Q2 = kura_query:where(Q1, {'and', [{D, '<', 0.5}, {'not', {Far, '>=', -1}}]}),
    Q3 = kura_query:having(kura_query:group_by(Q2, [content, embedding]), {D, '<=', 0.75}),
    Q4 = kura_query:order_by(Q3, [{D, asc}]),
    Q5 = kura_query:offset(kura_query:limit(Q4, 10), 20),
    ?assertEqual(
        {
            <<
                "SELECT \"content\" AS \"content\", \"embedding\" <-> $1 AS \"dist\" "
                "FROM \"kura_vector_items\" WHERE \"content\" LIKE $2 AND "
                "(\"embedding\" <-> $3 < $4 AND NOT (\"any_embedding\" <#> $5 >= $6)) "
                "GROUP BY \"content\", \"embedding\" HAVING \"embedding\" <-> $7 <= $8 "
                "ORDER BY \"embedding\" <-> $9 ASC LIMIT $10 OFFSET $11"
            >>,
            [
                ~"[1.0,2.0,3.0]",
                ~"a%",
                ~"[1.0,2.0,3.0]",
                0.5,
                ~"[4.0,5.0]",
                -1,
                ~"[1.0,2.0,3.0]",
                0.75,
                ~"[1.0,2.0,3.0]",
                10,
                20
            ]
        },
        kura_dialect_pg:to_sql(Q5)
    ).

distance_threshold_in_a_subquery_continues_the_numbering_test() ->
    D = kura_query:distance(embedding, cosine, [1, 2, 3]),
    Near = kura_query:select(kura_query:where(kura_query:from(?VEC_SCHEMA), {D, '<', 0.2}), [id]),
    Q = kura_query:where(
        kura_query:where(kura_query:from(?VEC_SCHEMA), {content, ~"x"}),
        {id, in, {subquery, Near}}
    ),
    ?assertEqual(
        {
            <<
                "SELECT * FROM \"kura_vector_items\" WHERE \"content\" = $1 AND \"id\" IN "
                "(SELECT \"id\" FROM \"kura_vector_items\" WHERE \"embedding\" <=> $2 < $3)"
            >>,
            [~"x", ~"[1.0,2.0,3.0]", 0.2]
        },
        kura_dialect_pg:to_sql(Q)
    ).

distance_threshold_in_delete_all_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    {SQL, Params} = kura_dialect_pg:delete_all(
        kura_query:where(kura_query:from(?VEC_SCHEMA), {D, '>', 2})
    ),
    ?assertEqual(
        {~"DELETE FROM \"kura_vector_items\" WHERE \"embedding\" <-> $1 > $2", [~"[1.0,2.0,3.0]", 2]},
        {iolist_to_binary(SQL), Params}
    ).

distance_threshold_under_a_join_qualifies_the_vector_column_test() ->
    D = kura_query:distance(embedding, l2, [1, 2, 3]),
    Q0 = kura_query:join(kura_query:from(?VEC_SCHEMA), inner, kura_test_schema, {id, id}),
    Q1 = kura_query:order_by(kura_query:where(Q0, {D, '<', 2}), [{D, asc}]),
    {SQL, Params} = kura_dialect_pg:to_sql(Q1),
    ?assertEqual(
        <<
            "SELECT * FROM \"kura_vector_items\" "
            "INNER JOIN \"users\" ON \"kura_vector_items\".\"id\" = \"users\".\"id\" "
            "WHERE \"kura_vector_items\".\"embedding\" <-> $1 < $2 "
            "ORDER BY \"kura_vector_items\".\"embedding\" <-> $3 ASC"
        >>,
        iolist_to_binary(SQL)
    ),
    ?assertEqual([~"[1.0,2.0,3.0]", 2, ~"[1.0,2.0,3.0]"], Params).
