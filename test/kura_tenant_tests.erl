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

an_attribute_tenant_with_no_value_is_refused_everywhere_test_() ->
    [
        {atom_to_list(Missing), fun() ->
            kura_tenant:clear_tenant(),
            Bad = {attribute, {org_id, Missing}},
            Refused = {invalid_tenant, Bad},
            ?assertError(Refused, kura_tenant:put_tenant(Bad)),
            ?assertError(Refused, kura_tenant:with_tenant(Bad, fun() -> ran end)),
            ?assertError(Refused, kura_tenant:with_tenant(Bad, {erlang, self, []})),
            ?assertError(Refused, kura_tenant:adopt({Bad, false})),
            ?assertEqual(undefined, kura_tenant:get_tenant()),
            ?assertEqual({undefined, false}, kura_tenant:current_scope())
        end}
     || Missing <- [undefined, null, nil]
    ].

an_attribute_tenant_whose_field_heads_a_condition_is_refused_everywhere_test_() ->
    Sub = {subquery, kura_query:from(?S)},
    [
        {atom_to_list(Head), fun() ->
            kura_tenant:clear_tenant(),
            Bad = kura_test_typing:ill_typed({attribute, {Head, Rest}}),
            Refused = {invalid_tenant, Bad},
            ?assertError(Refused, kura_tenant:put_tenant(Bad)),
            ?assertError(Refused, kura_tenant:with_tenant(Bad, fun() -> ran end)),
            ?assertError(Refused, kura_tenant:with_tenant(Bad, {erlang, self, []})),
            ?assertError(Refused, kura_tenant:adopt({Bad, false})),
            ?assertEqual(undefined, kura_tenant:get_tenant()),
            _ = erlang:put(kura_tenant, Bad),
            ?assertError(Refused, kura_tenant:get_tenant()),
            ?assertError(Refused, kura_tenant:current_scope()),
            _ = erlang:erase(kura_tenant)
        end}
     || {Head, Rest} <- [
            {'or', [{org_id, 7}, {org_id, is_not_nil}]},
            {'and', []},
            {'not', {org_id, 7}},
            {exists, Sub},
            {not_exists, Sub},
            {fragment, ~"1 = 1"}
        ]
    ].

an_attribute_tenant_on_a_plain_field_is_taken_as_it_is_test_() ->
    [
        fun() ->
            kura_tenant:clear_tenant(),
            Tenant = {attribute, {Field, Value}},
            ?assertEqual(undefined, kura_tenant:put_tenant(Tenant)),
            ?assertEqual(Tenant, kura_tenant:get_tenant()),
            ?assertEqual(Tenant, kura_tenant:with_tenant(Tenant, fun kura_tenant:get_tenant/0)),
            ?assertEqual(Tenant, kura_tenant:clear_tenant())
        end
     || {Field, Value} <- [
            {org_id, 7},
            {org_id, ~"acme"},
            {org_id, [1, 2]},
            {org_id, #{id => 7}},
            {org_id, true},
            {in, 7},
            {between, {1, 2}},
            {'or_id', 7}
        ]
    ].

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
    Result = kura_tenant:with_tenant({prefix, ~"tmp"}, fun kura_tenant:get_tenant/0),
    ?assertEqual({prefix, ~"tmp"}, Result),
    ?assertEqual(undefined, kura_tenant:get_tenant()).

with_tenant_restores_previous_test() ->
    kura_tenant:put_tenant({prefix, <<"original">>}),
    kura_tenant:with_tenant({prefix, <<"nested">>}, fun() ->
        ?assertEqual({prefix, <<"nested">>}, kura_tenant:get_tenant())
    end),
    ?assertEqual({prefix, <<"original">>}, kura_tenant:get_tenant()),
    kura_tenant:clear_tenant().

with_tenant_ends_on_every_exit_test_() ->
    Tenant = {prefix, ~"scoped"},
    [
        {Label, fun() ->
            kura_tenant:clear_tenant(),
            ?assertException(Class, boom, kura_tenant:with_tenant(Tenant, Raise)),
            ?assertEqual(undefined, kura_tenant:get_tenant())
        end}
     || {Label, Class, Raise} <- [
            {"error", error, fun() -> error(boom) end},
            {"throw", throw, fun() -> throw(boom) end},
            {"exit", exit, fun() -> exit(boom) end}
        ]
    ].

with_tenant_that_is_refused_leaves_the_process_as_it_was_test() ->
    kura_tenant:put_tenant({prefix, ~"original"}),
    try
        ?assertError(
            {invalid_tenant, {prefix, "list"}},
            kura_tenant:with_tenant(kura_test_typing:ill_typed({prefix, "list"}), fun() -> ok end)
        ),
        ?assertEqual({prefix, ~"original"}, kura_tenant:get_tenant())
    after
        kura_tenant:clear_tenant()
    end.

%% A value put in the process dictionary by hand must not make the scope's
%% own tenant outlive it.
with_tenant_over_an_invalid_stored_value_test() ->
    erlang:put(kura_tenant, not_a_tenant),
    try
        ?assertEqual(
            {prefix, ~"scoped"},
            kura_tenant:with_tenant({prefix, ~"scoped"}, fun kura_tenant:get_tenant/0)
        ),
        ?assertEqual(not_a_tenant, erlang:get(kura_tenant)),
        ?assertEqual(ok, kura_tenant:without_tenant(fun() -> ok end)),
        ?assertEqual(not_a_tenant, erlang:get(kura_tenant))
    after
        erlang:erase(kura_tenant)
    end.

a_tenant_put_inside_a_scope_ends_with_it_test() ->
    kura_tenant:clear_tenant(),
    kura_tenant:with_tenant({prefix, ~"outer"}, fun() ->
        kura_tenant:put_tenant({prefix, ~"inner"})
    end),
    ?assertEqual(undefined, kura_tenant:get_tenant()),
    kura_tenant:without_tenant(fun() -> kura_tenant:put_tenant({prefix, ~"inner"}) end),
    ?assertEqual(undefined, kura_tenant:get_tenant()).

scopes_take_a_module_function_and_arguments_test() ->
    kura_tenant:clear_tenant(),
    ?assertEqual(
        {prefix, ~"mfa"},
        kura_tenant:with_tenant({prefix, ~"mfa"}, {kura_tenant, get_tenant, []})
    ),
    ?assertEqual(undefined, kura_tenant:get_tenant()),
    kura_tenant:put_tenant({prefix, ~"original"}),
    try
        ?assertEqual(undefined, kura_tenant:without_tenant({kura_tenant, get_tenant, []})),
        ?assertEqual([3, 2, 1], kura_tenant:without_tenant({lists, reverse, [[1, 2, 3]]})),
        ?assertEqual({prefix, ~"original"}, kura_tenant:get_tenant())
    after
        kura_tenant:clear_tenant()
    end.

a_scope_is_handed_to_another_process_test() ->
    kura_tenant:clear_tenant(),
    Adopted = fun(Scope) ->
        Caller = self(),
        Pid = spawn(fun() ->
            ok = kura_tenant:adopt(Scope),
            Caller ! {self(), kura_tenant:get_tenant(), kura_tenant:current_scope()}
        end),
        receive
            {Pid, Tenant, Seen} -> {Tenant, Seen}
        end
    end,
    Tenant = {attribute, {org_id, 1}},
    ?assertEqual({undefined, kura_tenant:current_scope()}, Adopted(kura_tenant:current_scope())),
    ?assertMatch(
        {Tenant, _},
        kura_tenant:with_tenant(Tenant, fun() -> Adopted(kura_tenant:current_scope()) end)
    ),
    Unscoped = kura_tenant:without_tenant(fun kura_tenant:current_scope/0),
    ?assertEqual({undefined, Unscoped}, Adopted(Unscoped)),
    ?assertNotEqual(Unscoped, kura_tenant:current_scope()).

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

a_tenant_field_that_is_no_column_of_the_schema_is_refused_test_() ->
    NoColumn = {attribute, {no_such_field, 7}},
    From = kura_query:from(?S),
    Edge = kura_query:from(kura_test_edge_schema),
    Refused = [
        {"a query", NoColumn, From},
        {"a query with conditions", NoColumn, kura_query:where(From, {title, ~"a"})},
        {"a branch of a union", {attribute, ?T}, kura_query:union(From, Edge)},
        {"the second of two branches", {attribute, ?T},
            kura_query:union(kura_query:union(From, From), Edge)},
        {"a branch of a branch", {attribute, ?T},
            kura_query:union_all(From, kura_query:except(From, Edge))}
    ],
    Read = [
        {"a join can bring the column in",
            kura_query:join(From, inner, kura_test_edge_schema, {id, id})},
        {"a CTE can bring the column in",
            kura_query:with_cte(From, ~"posts", kura_query:from(~"other"))},
        {"a table that is no schema", kura_query:from(~"posts")},
        {"a name that is no schema", kura_query:from(posts)},
        {"a subquery reads the outer query's column",
            kura_query:where(
                kura_query:from(~"posts"), {id, in, {subquery, kura_query:from(?S)}}
            )}
    ],
    [
        {Name, fun() ->
            ?assertError({invalid_tenant, Tenant}, kura_tenant:scope_query(Tenant, Q))
        end}
     || {Name, Tenant, Q} <- Refused
    ] ++
        [
            {Name, fun() ->
                ?assertMatch(
                    #kura_query{wheres = [_ | _]}, kura_tenant:scope_query(NoColumn, Q)
                )
            end}
         || {Name, Q} <- Read
        ] ++
        [
            {"a field of the schema scopes it", fun() ->
                ?assertMatch(
                    #kura_query{wheres = [?T]}, kura_tenant:scope_query({attribute, ?T}, From)
                ),
                ?assertMatch(
                    #kura_query{wheres = [{title, 7}]},
                    kura_tenant:scope_query({attribute, {title, 7}}, From)
                )
            end},
            {"a virtual field is no column", fun() ->
                Virtual = {attribute, {full_name, 7}},
                ?assertError(
                    {invalid_tenant, Virtual},
                    kura_tenant:scope_query(Virtual, kura_query:from(kura_test_schema))
                )
            end}
        ].

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
