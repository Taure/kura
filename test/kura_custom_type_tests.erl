-module(kura_custom_type_tests).
-moduledoc "Tests for custom type behaviour ({custom, Module}).".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%% eqWAlizer: proper macros generate term()
-eqwalizer({nowarn_function, prop_money_dollars_roundtrip/0}).
-eqwalizer({nowarn_function, prop_email_lowercase/0}).

%%----------------------------------------------------------------------
%% Unit tests: cast/dump/load through kura_types
%%----------------------------------------------------------------------

money_cast_integer_test() ->
    ?assertEqual({ok, 1500}, kura_types:cast({custom, kura_test_money_type}, 1500)).

money_cast_dollars_test() ->
    ?assertEqual({ok, 1999}, kura_types:cast({custom, kura_test_money_type}, {dollars, 19.99})).

money_cast_invalid_test() ->
    ?assertMatch({error, _}, kura_types:cast({custom, kura_test_money_type}, ~"not money")).

money_dump_test() ->
    ?assertEqual({ok, 500}, kura_types:dump({custom, kura_test_money_type}, 500)).

money_load_test() ->
    ?assertEqual({ok, 500}, kura_types:load({custom, kura_test_money_type}, 500)).

money_undefined_test() ->
    ?assertEqual({ok, undefined}, kura_types:cast({custom, kura_test_money_type}, undefined)),
    ?assertEqual({ok, null}, kura_types:dump({custom, kura_test_money_type}, undefined)),
    ?assertEqual({ok, undefined}, kura_types:load({custom, kura_test_money_type}, null)).

email_cast_valid_test() ->
    ?assertEqual(
        {ok, ~"alice@example.com"},
        kura_types:cast({custom, kura_test_email_type}, ~"Alice@Example.com")
    ).

email_cast_invalid_test() ->
    ?assertMatch({error, _}, kura_types:cast({custom, kura_test_email_type}, ~"not-an-email")).

email_cast_non_binary_test() ->
    ?assertMatch({error, _}, kura_types:cast({custom, kura_test_email_type}, 42)).

pg_type_money_test() ->
    ?assertEqual(~"INTEGER", kura_types:to_pg_type({custom, kura_test_money_type})).

pg_type_email_test() ->
    ?assertEqual(~"VARCHAR(255)", kura_types:to_pg_type({custom, kura_test_email_type})).

format_type_custom_test() ->
    CS = kura_changeset:cast(
        #{price => {custom, kura_test_money_type}},
        #{},
        #{price => ~"bad"},
        [price]
    ),
    ?assertNot(CS#kura_changeset.valid).

%%----------------------------------------------------------------------
%% Property tests
%%----------------------------------------------------------------------

property_test_() ->
    {timeout, 30, [
        {"money cast/dump/load roundtrip", fun() -> run(prop_money_cast_roundtrip()) end},
        {"money dollars roundtrip", fun() -> run(prop_money_dollars_roundtrip()) end},
        {"email always lowercase", fun() -> run(prop_email_lowercase()) end}
    ]}.

run(Prop) ->
    ?assert(proper:quickcheck(Prop, [200, {to_file, user}, noshrink])).

prop_money_cast_roundtrip() ->
    Type = {custom, kura_test_money_type},
    ?FORALL(
        V,
        integer(),
        begin
            {ok, Cast} = kura_types:cast(Type, V),
            {ok, Dumped} = kura_types:dump(Type, Cast),
            {ok, Loaded} = kura_types:load(Type, Dumped),
            Loaded =:= V
        end
    ).

prop_money_dollars_roundtrip() ->
    Type = {custom, kura_test_money_type},
    ?FORALL(
        Cents,
        range(1, 1000000),
        begin
            Dollars = Cents / 100,
            {ok, Cast} = kura_types:cast(Type, {dollars, Dollars}),
            {ok, Dumped} = kura_types:dump(Type, Cast),
            {ok, Loaded} = kura_types:load(Type, Dumped),
            Loaded =:= Cents
        end
    ).

prop_email_lowercase() ->
    Type = {custom, kura_test_email_type},
    ?FORALL(
        Local,
        non_empty(list(range($A, $Z))),
        begin
            Email = list_to_binary(Local ++ "@TEST.COM"),
            {ok, Cast} = kura_types:cast(Type, Email),
            Cast =:= string:lowercase(Email)
        end
    ).

%%----------------------------------------------------------------------
%% Integration tests (DB round-trip)
%%----------------------------------------------------------------------

integration_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"insert with custom types", fun t_insert/0},
            {"get with custom types", fun t_get/0},
            {"update custom fields", fun t_update/0},
            {"cast validation rejects bad custom value", fun t_validation/0},
            {"insert_all with custom types", fun t_insert_all/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS custom_type_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  price INTEGER,"
        "  email VARCHAR(255),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query("TRUNCATE custom_type_items", []),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS custom_type_items CASCADE", []),
    ok.

t_insert() ->
    CS = kura_changeset:cast(
        kura_test_custom_schema,
        #{},
        #{name => ~"Widget", price => {dollars, 19.99}, email => ~"Shop@Example.COM"},
        [name, price, email]
    ),
    ?assert(CS#kura_changeset.valid),
    {ok, Record} = kura_test_repo:insert(CS),
    ?assertEqual(1999, maps:get(price, Record)),
    ?assertEqual(~"shop@example.com", maps:get(email, Record)).

t_get() ->
    CS = kura_changeset:cast(
        kura_test_custom_schema,
        #{},
        #{name => ~"Gadget", price => 500, email => ~"info@gadget.io"},
        [name, price, email]
    ),
    {ok, Inserted} = kura_test_repo:insert(CS),
    Id = maps:get(id, Inserted),
    {ok, Fetched} = kura_test_repo:get(kura_test_custom_schema, Id),
    ?assertEqual(500, maps:get(price, Fetched)),
    ?assertEqual(~"info@gadget.io", maps:get(email, Fetched)).

t_update() ->
    CS = kura_changeset:cast(
        kura_test_custom_schema,
        #{},
        #{name => ~"Thing", price => 100, email => ~"old@test.com"},
        [name, price, email]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    CS2 = kura_changeset:cast(
        kura_test_custom_schema,
        Record,
        #{price => {dollars, 25.50}, email => ~"NEW@TEST.COM"},
        [price, email]
    ),
    {ok, Updated} = kura_test_repo:update(CS2),
    ?assertEqual(2550, maps:get(price, Updated)),
    ?assertEqual(~"new@test.com", maps:get(email, Updated)).

t_validation() ->
    CS = kura_changeset:cast(
        kura_test_custom_schema,
        #{},
        #{name => ~"Bad", email => ~"no-at-sign"},
        [name, email]
    ),
    ?assertNot(CS#kura_changeset.valid),
    ?assertMatch([{email, _} | _], CS#kura_changeset.errors).

t_insert_all() ->
    Entries = [
        #{name => ~"Bulk1", price => 100, email => ~"a@test.com"},
        #{name => ~"Bulk2", price => 200, email => ~"b@test.com"}
    ],
    {ok, 2} = kura_repo_worker:insert_all(kura_test_repo, kura_test_custom_schema, Entries),
    Q = kura_query:where(
        kura_query:from(kura_test_custom_schema),
        {name, like, ~"Bulk%"}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(2, length(Results)).
