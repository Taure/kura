-module(kura_tenant_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(S, kura_tenant_test_schema).

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
    kura_tenant:clear_tenant(),
    Q = kura_query:prefix(kura_query:from(?S), <<"acme">>),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)).

select_with_prefix_from_process_dict_test() ->
    kura_tenant:put_tenant({prefix, <<"globex">>}),
    Q = kura_query:from(?S),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"globex\".\"posts\"">>)),
    kura_tenant:clear_tenant().

select_without_prefix_test() ->
    kura_tenant:clear_tenant(),
    Q = kura_query:from(?S),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"FROM \"posts\"">>)),
    %% No schema qualifier dot
    ?assertEqual(nomatch, binary:match(SQLBin, <<"\".\"posts\"">>)).

insert_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    {SQL, _} = kura_query_compiler:insert(?S, [title], #{title => <<"Hello">>}),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

update_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    {SQL, _} = kura_query_compiler:update(?S, [title], #{title => <<"Updated">>}, {id, 1}),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

delete_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    {SQL, _} = kura_query_compiler:delete(?S, id, 1),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

delete_all_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    Q = kura_query:from(?S),
    {SQL, _} = kura_query_compiler:delete_all(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

update_all_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    Q = kura_query:from(?S),
    {SQL, _} = kura_query_compiler:update_all(Q, #{title => <<"New">>}),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

insert_all_with_prefix_test() ->
    kura_tenant:put_tenant({prefix, <<"acme">>}),
    {SQL, _} = kura_query_compiler:insert_all(?S, [title], [
        #{title => <<"A">>}, #{title => <<"B">>}
    ]),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"acme\".\"posts\"">>)),
    kura_tenant:clear_tenant().

explicit_prefix_overrides_process_dict_test() ->
    kura_tenant:put_tenant({prefix, <<"process_dict">>}),
    Q = kura_query:prefix(kura_query:from(?S), <<"explicit">>),
    {SQL, _} = kura_query_compiler:to_sql(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertNotEqual(nomatch, binary:match(SQLBin, <<"\"explicit\".\"posts\"">>)),
    ?assertEqual(nomatch, binary:match(SQLBin, <<"process_dict">>)),
    kura_tenant:clear_tenant().
