-module(kura_generate_id_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

generate_id_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"generate_id callback is used on insert", fun t_insert_uses_generate_id/0},
            {"explicit id takes precedence over generate_id", fun t_explicit_id_wins/0},
            {"each insert gets a unique generated id", fun t_unique_ids/0},
            {"schema without generate_id still works", fun t_schema_without_callback/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS generate_id_items ("
        "  id UUID PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  status VARCHAR(255),"
        "  tags TEXT[],"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS generate_id_items CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_insert_uses_generate_id() ->
    CS = kura_changeset:cast(
        kura_test_generate_id_schema,
        #{},
        #{~"name" => ~"Alice"},
        [name]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    Id = maps:get(id, Record),
    ?assertNotEqual(undefined, Id),
    %% Verify it's a valid UUID v7 (version nibble = 7)
    <<_:14/binary, Version:1/binary, _/binary>> = Id,
    ?assertEqual(~"7", Version).

t_explicit_id_wins() ->
    ExplicitId = ~"00000000-0000-7000-8000-000000000001",
    CS = kura_changeset:cast(
        kura_test_generate_id_schema,
        #{},
        #{~"id" => ExplicitId, ~"name" => ~"Bob"},
        [id, name]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    ?assertEqual(ExplicitId, maps:get(id, Record)).

t_unique_ids() ->
    Insert = fun(Name) ->
        CS = kura_changeset:cast(
            kura_test_generate_id_schema,
            #{},
            #{~"name" => Name},
            [name]
        ),
        {ok, Record} = kura_test_repo:insert(CS),
        maps:get(id, Record)
    end,
    Id1 = Insert(~"One"),
    Id2 = Insert(~"Two"),
    Id3 = Insert(~"Three"),
    ?assertNotEqual(Id1, Id2),
    ?assertNotEqual(Id2, Id3),
    ?assertNotEqual(Id1, Id3).

t_schema_without_callback() ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{~"name" => ~"NoCallback", ~"email" => ~"no@cb.com"},
        [name, email]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    ?assert(is_integer(maps:get(id, Record))).
