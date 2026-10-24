-module(kura_tenant_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(S, kura_tenant_test_schema).
-define(REPO, kura_tenant_test_repo).
-define(T, {org_id, 7}).

setup() ->
    application:set_env(kura, repos, #{?REPO => #{dialect => kura_dialect_pg}}).

cleanup() ->
    application:unset_env(kura, repos),
    kura_tenant:clear_tenant().

%%----------------------------------------------------------------------
%% kura_tenant process dictionary tests
%%----------------------------------------------------------------------

put_get_tenant_test() ->
    kura_tenant:clear_tenant(),
    ?assertEqual(undefined, kura_tenant:get_tenant()),
    kura_tenant:put_tenant({prefix, <<"tenant_1">>}),
    ?assertEqual({prefix, <<"tenant_1">>}, kura_tenant:get_tenant()),
    kura_tenant:clear_tenant(),
    ?assertEqual(undefined, kura_tenant:get_tenant()).

put_tenant_refuses_what_would_scope_nothing_test() ->
    kura_tenant:clear_tenant(),
    [
        ?assertError({invalid_tenant, Bad}, erlang:apply(kura_tenant, put_tenant, [Bad]))
     || Bad <- [
            {attribute, {"org_id", 1}},
            {attribute, org_id, 1},
            {prefix, "acme"},
            {attribute, {org_id, is_nil}},
            {attribute, {org_id, is_not_nil}},
            acme
        ]
    ],
    ?assertEqual(undefined, kura_tenant:get_tenant()).

put_tenant_accepts_undefined_as_no_tenant_test() ->
    kura_tenant:clear_tenant(),
    ?assertEqual(undefined, kura_tenant:put_tenant(undefined)),
    ?assertEqual(undefined, kura_tenant:get_tenant()).

get_tenant_refuses_a_malformed_stored_tenant_test() ->
    _ = erlang:put(kura_tenant, {prefix, "acme"}),
    try
        ?assertError({invalid_tenant, {prefix, "acme"}}, kura_tenant:get_tenant())
    after
        erlang:erase(kura_tenant)
    end.

with_tenant_test() ->
    kura_tenant:clear_tenant(),
    Result = kura_tenant:with_tenant({prefix, <<"tmp">>}, fun() ->
        kura_tenant:get_tenant()
    end),
    ?assertEqual({prefix, <<"tmp">>}, Result),
    ?assertEqual(undefined, kura_tenant:get_tenant()).

with_tenant_restores_previous_test() ->
    kura_tenant:put_tenant({prefix, <<"original">>}),
    kura_tenant:with_tenant({prefix, <<"nested">>}, fun() ->
        ?assertEqual({prefix, <<"nested">>}, kura_tenant:get_tenant())
    end),
    ?assertEqual({prefix, <<"original">>}, kura_tenant:get_tenant()),
    kura_tenant:clear_tenant().

attribute_tenant_test() ->
    kura_tenant:clear_tenant(),
    kura_tenant:put_tenant({attribute, {org_id, 42}}),
    ?assertEqual({attribute, {org_id, 42}}, kura_tenant:get_tenant()),
    kura_tenant:clear_tenant().

%%----------------------------------------------------------------------
%% Compiler prefix integration tests
%%----------------------------------------------------------------------

select_with_prefix_from_query_test() ->
    setup(),
    try
        kura_tenant:clear_tenant(),
        Q = kura_query:prefix(kura_query:from(?S), <<"acme">>),
        {SQL, _} = kura_query_compiler:to_sql(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

select_with_prefix_from_process_dict_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"globex">>}),
        Q = kura_query:from(?S),
        {SQL, _} = kura_query_compiler:to_sql(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"globex\".\"posts\"">>))
    after
        cleanup()
    end.

select_without_prefix_test() ->
    setup(),
    try
        kura_tenant:clear_tenant(),
        Q = kura_query:from(?S),
        {SQL, _} = kura_query_compiler:to_sql(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"FROM \"posts\"">>)),
        ?assertEqual(nomatch, binary:match(SQLBin, <<"\".\"posts\"">>))
    after
        cleanup()
    end.

insert_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        {SQL, _} = kura_query_compiler:insert(?REPO, ?S, [title], #{title => <<"Hello">>}),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

update_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        {SQL, _} = kura_query_compiler:update(
            ?REPO, ?S, [title], #{title => <<"Updated">>}, [{id, 1}]
        ),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

delete_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        {SQL, _} = kura_query_compiler:delete(?REPO, ?S, [{id, 1}]),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

delete_all_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        Q = kura_query:from(?S),
        {SQL, _} = kura_query_compiler:delete_all(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

update_all_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        Q = kura_query:from(?S),
        {SQL, _} = kura_query_compiler:update_all(?REPO, Q, #{title => <<"New">>}),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

insert_all_with_prefix_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"acme">>}),
        {SQL, _} = kura_query_compiler:insert_all(?REPO, ?S, [title], [
            #{title => <<"A">>}, #{title => <<"B">>}
        ]),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>))
    after
        cleanup()
    end.

explicit_prefix_overrides_process_dict_test() ->
    setup(),
    try
        kura_tenant:put_tenant({prefix, <<"process_dict">>}),
        Q = kura_query:prefix(kura_query:from(?S), <<"explicit">>),
        {SQL, _} = kura_query_compiler:to_sql(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"explicit\".\"posts\"">>)),
        ?assertEqual(nomatch, binary:match(SQLBin, <<"process_dict">>))
    after
        cleanup()
    end.

scope_query_without_attribute_tenant_is_identity_test() ->
    Q = nested_query(),
    ?assertEqual(Q, kura_tenant:scope_query(undefined, Q)),
    ?assertEqual(Q, kura_tenant:scope_query({prefix, ~"acme"}, Q)).

scope_query_scopes_every_nested_query_test() ->
    Scoped = kura_tenant:scope_query({attribute, ?T}, nested_query()),
    ?assertEqual(6, count_queries(Scoped)),
    ?assertEqual(6, count_scoped(Scoped)).

scope_query_appends_after_existing_conditions_test() ->
    Q = kura_query:where(kura_query:from(?S), {title, ~"a"}),
    #kura_query{wheres = Wheres} = kura_tenant:scope_query({attribute, ?T}, Q),
    ?assertEqual([{title, ~"a"}, ?T], Wheres).

compiled_sql_without_tenant_is_unchanged_test() ->
    setup(),
    try
        Q = nested_query(),
        ?assertEqual(
            kura_dialect_pg:to_sql(Q),
            kura_query_compiler:to_sql(?REPO, Q)
        )
    after
        cleanup()
    end.

nested_query() ->
    Sub = fun(Title) -> {subquery, kura_query:where(kura_query:from(?S), {title, Title})} end,
    Cte = kura_query:union(kura_query:from(?S), kura_query:from(?S)),
    Q0 = kura_query:where(
        kura_query:from(?S), {'and', [{exists, Sub(~"a")}, {'or', [{'not', {id, in, Sub(~"b")}}]}]}
    ),
    Q1 = kura_query:having(Q0, {not_exists, Sub(~"c")}),
    kura_query:with_cte(Q1, ~"cte", Cte).

count_queries(#kura_query{} = Q) ->
    1 + count_nested(Q, fun count_queries/1).

count_scoped(#kura_query{wheres = Wheres} = Q) ->
    Own =
        case lists:last(Wheres) of
            ?T -> 1;
            _ -> 0
        end,
    Own + count_nested(Q, fun count_scoped/1).

count_nested(#kura_query{wheres = W, havings = H, ctes = Ctes, combinations = Cs}, Count) ->
    lists:sum(
        [Count(Sub) || Sub <- subqueries(W ++ H)] ++
            [Count(Cte) || {_, Cte} <- Ctes] ++
            [Count(C) || {_, C} <- Cs]
    ).

subqueries(Conditions) ->
    lists:append([subqueries_in(C) || C <- Conditions]).

subqueries_in({Op, Cs}) when Op =:= 'and'; Op =:= 'or' -> subqueries(Cs);
subqueries_in({'not', C}) -> subqueries_in(C);
subqueries_in({_, in, {subquery, Q}}) -> [Q];
subqueries_in({E, {subquery, Q}}) when E =:= exists; E =:= not_exists -> [Q];
subqueries_in(_) -> [].
