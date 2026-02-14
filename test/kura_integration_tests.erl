-module(kura_integration_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

integration_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"insert returns record with id and timestamps", fun t_insert/0},
            {"get fetches by primary key", fun t_get/0},
            {"get returns not_found for missing id", fun t_get_not_found/0},
            {"get_by fetches by field", fun t_get_by/0},
            {"query with where/order_by/limit", fun t_query/0},
            {"count aggregate", fun t_count/0},
            {"update modifies record", fun t_update/0},
            {"delete removes record", fun t_delete/0},
            {"invalid changeset returns error without hitting DB", fun t_validation/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS users", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{
            <<"name">> => Name,
            <<"email">> => Email
        },
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_insert() ->
    {ok, User} = insert_user(<<"Alice">>, <<"alice@example.com">>),
    ?assert(is_integer(maps:get(id, User))),
    ?assertEqual(<<"Alice">>, maps:get(name, User)),
    ?assertEqual(<<"alice@example.com">>, maps:get(email, User)),
    ?assertNotEqual(undefined, maps:get(inserted_at, User)),
    ?assertNotEqual(undefined, maps:get(updated_at, User)).

t_get() ->
    {ok, Inserted} = insert_user(<<"Bob">>, <<"bob@example.com">>),
    Id = maps:get(id, Inserted),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, Id),
    ?assertEqual(<<"Bob">>, maps:get(name, Fetched)),
    ?assertEqual(<<"bob@example.com">>, maps:get(email, Fetched)),
    ?assertEqual(Id, maps:get(id, Fetched)).

t_get_not_found() ->
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_schema, 999999)).

t_get_by() ->
    {ok, _} = insert_user(<<"Carol">>, <<"carol@example.com">>),
    {ok, Found} = kura_test_repo:get_by(kura_test_schema, [{email, <<"carol@example.com">>}]),
    ?assertEqual(<<"Carol">>, maps:get(name, Found)).

t_query() ->
    {ok, _} = insert_user(<<"Dave">>, <<"dave@example.com">>),
    {ok, _} = insert_user(<<"Eve">>, <<"eve@example.com">>),
    {ok, _} = insert_user(<<"Frank">>, <<"frank@example.com">>),

    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"Dave">>, <<"Eve">>, <<"Frank">>]}),
    Q2 = kura_query:order_by(Q1, [{name, asc}]),
    Q3 = kura_query:limit(Q2, 2),
    {ok, Results} = kura_test_repo:all(Q3),

    ?assertEqual(2, length(Results)),
    ?assertEqual(<<"Dave">>, maps:get(name, hd(Results))).

t_count() ->
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:count(Q),
    {ok, [Row]} = kura_test_repo:all(Q1),
    Count = maps:get(count, Row),
    ?assert(Count >= 1).

t_update() ->
    {ok, User} = insert_user(<<"Grace">>, <<"grace@example.com">>),

    CS = kura_changeset:cast(
        kura_test_schema,
        User,
        #{
            <<"name">> => <<"Grace Updated">>
        },
        [name]
    ),
    {ok, Updated} = kura_test_repo:update(CS),

    ?assertEqual(<<"Grace Updated">>, maps:get(name, Updated)),
    ?assertEqual(maps:get(id, User), maps:get(id, Updated)),

    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    ?assertEqual(<<"Grace Updated">>, maps:get(name, Fetched)).

t_delete() ->
    {ok, User} = insert_user(<<"Hank">>, <<"hank@example.com">>),
    Id = maps:get(id, User),

    CS = kura_changeset:cast(kura_test_schema, User, #{}, []),
    {ok, _Deleted} = kura_test_repo:delete(CS),

    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_schema, Id)).

t_validation() ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{
            <<"name">> => <<>>
        },
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:validate_length(CS1, name, [{min, 1}]),

    ?assertNot(CS2#kura_changeset.valid),
    {error, InvalidCS} = kura_test_repo:insert(CS2),
    ?assertNot(InvalidCS#kura_changeset.valid),
    ?assertEqual(insert, InvalidCS#kura_changeset.action).
