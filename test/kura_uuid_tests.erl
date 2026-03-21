-module(kura_uuid_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

uuid_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Insert & Get
            {"insert with UUID primary key", fun t_insert_uuid_pk/0},
            {"get by UUID primary key", fun t_get_by_uuid/0},

            %% Update
            {"update UUID fields on existing record", fun t_update_uuid_fields/0},
            {"update non-UUID fields on UUID-PK record", fun t_update_uuid_pk_record/0},

            %% Multiple UUID fields
            {"insert with multiple UUID fields", fun t_multiple_uuid_fields/0},
            {"UUID combined with jsonb, arrays, strings", fun t_uuid_with_other_types/0},

            %% Nil UUIDs
            {"insert with nil UUID fields", fun t_uuid_nil_fields/0},

            %% Queries
            {"where clause on UUID field", fun t_uuid_where_clause/0},
            {"IN query with UUID list", fun t_uuid_in_query/0},

            %% UUID formats
            {"UUID without dashes gets formatted", fun t_uuid_without_dashes/0},

            %% Associations
            {"UUID foreign key insert and get", fun t_uuid_foreign_key/0},
            {"preload with UUID associations", fun t_uuid_preload/0},

            %% Bulk
            {"insert_all with UUID PK", fun t_uuid_insert_all/0},

            %% update_all
            {"update_all with UUID where", fun t_uuid_update_all/0},

            %% Delete
            {"delete UUID-PK record", fun t_uuid_delete/0},

            %% Reload
            {"reload UUID-PK record", fun t_uuid_reload/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS uuid_items ("
        "  id UUID PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  external_id UUID,"
        "  correlation_id UUID,"
        "  status VARCHAR(255) DEFAULT 'active',"
        "  tags TEXT[],"
        "  metadata JSONB,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS uuid_children ("
        "  id UUID PRIMARY KEY,"
        "  parent_id UUID NOT NULL REFERENCES uuid_items(id),"
        "  label VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS uuid_children CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS uuid_items CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

uuid() ->
    Hex = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    <<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>> = Hex,
    <<A/binary, "-", B/binary, "-", C/binary, "-", D/binary, "-", E/binary>>.

insert_item(Name) ->
    insert_item(Name, #{}).

insert_item(Name, Extra) ->
    Params = maps:merge(#{id => uuid(), name => Name}, Extra),
    Fields = [id, name | maps:keys(Extra)],
    CS = kura_changeset:cast(kura_test_uuid_schema, #{}, Params, Fields),
    {ok, Record} = kura_test_repo:insert(CS),
    Record.

insert_child(ParentId, Label) ->
    CS = kura_changeset:cast(
        kura_test_uuid_child_schema,
        #{},
        #{id => uuid(), parent_id => ParentId, label => Label},
        [id, parent_id, label]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    Record.

%%----------------------------------------------------------------------
%% Insert & Get
%%----------------------------------------------------------------------

t_insert_uuid_pk() ->
    Id = uuid(),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        #{},
        #{id => Id, name => ~"Alice"},
        [id, name]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    ?assertEqual(Id, maps:get(id, Record)),
    ?assertEqual(~"Alice", maps:get(name, Record)),
    ?assertEqual(~"active", maps:get(status, Record)),
    ?assert(maps:is_key(inserted_at, Record)).

t_get_by_uuid() ->
    Item = insert_item(~"Bob"),
    Id = maps:get(id, Item),
    {ok, Fetched} = kura_test_repo:get(kura_test_uuid_schema, Id),
    ?assertEqual(Id, maps:get(id, Fetched)),
    ?assertEqual(~"Bob", maps:get(name, Fetched)).

%%----------------------------------------------------------------------
%% Update
%%----------------------------------------------------------------------

t_update_uuid_fields() ->
    Item = insert_item(~"Carol"),
    NewExtId = uuid(),
    NewCorrId = uuid(),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        Item,
        #{external_id => NewExtId, correlation_id => NewCorrId},
        [external_id, correlation_id]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(NewExtId, maps:get(external_id, Updated)),
    ?assertEqual(NewCorrId, maps:get(correlation_id, Updated)),
    %% Re-fetch to verify persistence
    {ok, Refetched} = kura_test_repo:get(kura_test_uuid_schema, maps:get(id, Item)),
    ?assertEqual(NewExtId, maps:get(external_id, Refetched)),
    ?assertEqual(NewCorrId, maps:get(correlation_id, Refetched)).

t_update_uuid_pk_record() ->
    Item = insert_item(~"Dave"),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        Item,
        #{name => ~"Dave Updated", status => ~"inactive"},
        [name, status]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(maps:get(id, Item), maps:get(id, Updated)),
    ?assertEqual(~"Dave Updated", maps:get(name, Updated)),
    ?assertEqual(~"inactive", maps:get(status, Updated)).

%%----------------------------------------------------------------------
%% Multiple UUID fields
%%----------------------------------------------------------------------

t_multiple_uuid_fields() ->
    ExtId = uuid(),
    CorrId = uuid(),
    Item = insert_item(~"Eve", #{external_id => ExtId, correlation_id => CorrId}),
    ?assertEqual(ExtId, maps:get(external_id, Item)),
    ?assertEqual(CorrId, maps:get(correlation_id, Item)),
    %% All three UUIDs should be distinct
    Id = maps:get(id, Item),
    ?assertNotEqual(Id, ExtId),
    ?assertNotEqual(Id, CorrId),
    ?assertNotEqual(ExtId, CorrId).

t_uuid_with_other_types() ->
    ExtId = uuid(),
    Item = insert_item(~"Frank", #{
        external_id => ExtId,
        tags => [~"admin", ~"active"],
        metadata => #{~"role" => ~"superuser", ~"level" => 5}
    }),
    ?assertEqual(ExtId, maps:get(external_id, Item)),
    ?assertEqual([~"admin", ~"active"], maps:get(tags, Item)),
    ?assertEqual(#{~"role" => ~"superuser", ~"level" => 5}, maps:get(metadata, Item)),
    %% Update the UUID while keeping other fields
    NewExtId = uuid(),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        Item,
        #{external_id => NewExtId, tags => [~"updated"]},
        [external_id, tags]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(NewExtId, maps:get(external_id, Updated)),
    ?assertEqual([~"updated"], maps:get(tags, Updated)).

%%----------------------------------------------------------------------
%% Nil UUIDs
%%----------------------------------------------------------------------

t_uuid_nil_fields() ->
    Item = insert_item(~"Grace"),
    ?assertEqual(undefined, maps:get(external_id, Item)),
    ?assertEqual(undefined, maps:get(correlation_id, Item)),
    %% Update one to a value, leave other nil
    NewExtId = uuid(),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        Item,
        #{external_id => NewExtId},
        [external_id]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(NewExtId, maps:get(external_id, Updated)),
    ?assertEqual(undefined, maps:get(correlation_id, Updated)).

%%----------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------

t_uuid_where_clause() ->
    Item = insert_item(~"Heidi", #{external_id => uuid()}),
    ExtId = maps:get(external_id, Item),
    Q = kura_query:where(kura_query:from(kura_test_uuid_schema), {external_id, ExtId}),
    {ok, [Found]} = kura_test_repo:all(Q),
    ?assertEqual(maps:get(id, Item), maps:get(id, Found)).

t_uuid_in_query() ->
    I1 = insert_item(~"Ivan"),
    I2 = insert_item(~"Judy"),
    _I3 = insert_item(~"Karl"),
    Ids = [maps:get(id, I1), maps:get(id, I2)],
    Q = kura_query:where(kura_query:from(kura_test_uuid_schema), {id, in, Ids}),
    {ok, Results} = kura_test_repo:all(Q),
    FoundIds = [maps:get(id, R) || R <- Results],
    ?assert(lists:member(maps:get(id, I1), FoundIds)),
    ?assert(lists:member(maps:get(id, I2), FoundIds)),
    ?assertEqual(2, length(Results)).

%%----------------------------------------------------------------------
%% UUID formats
%%----------------------------------------------------------------------

t_uuid_without_dashes() ->
    %% UUID without dashes should be auto-formatted by kura_types:cast
    Hex = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    CS = kura_changeset:cast(
        kura_test_uuid_schema,
        #{},
        #{id => Hex, name => ~"Dashless"},
        [id, name]
    ),
    {ok, Record} = kura_test_repo:insert(CS),
    Id = maps:get(id, Record),
    %% Should have dashes
    ?assertEqual(36, byte_size(Id)),
    ?assertEqual($-, binary:at(Id, 8)),
    ?assertEqual($-, binary:at(Id, 13)),
    ?assertEqual($-, binary:at(Id, 18)),
    ?assertEqual($-, binary:at(Id, 23)).

%%----------------------------------------------------------------------
%% Associations
%%----------------------------------------------------------------------

t_uuid_foreign_key() ->
    Parent = insert_item(~"Linda"),
    ParentId = maps:get(id, Parent),
    Child = insert_child(ParentId, ~"child-1"),
    ?assertEqual(ParentId, maps:get(parent_id, Child)),
    %% Fetch child by parent_id
    Q = kura_query:where(
        kura_query:from(kura_test_uuid_child_schema),
        {parent_id, ParentId}
    ),
    {ok, [Found]} = kura_test_repo:all(Q),
    ?assertEqual(maps:get(id, Child), maps:get(id, Found)).

t_uuid_preload() ->
    Parent = insert_item(~"Mike"),
    ParentId = maps:get(id, Parent),
    _C1 = insert_child(ParentId, ~"child-a"),
    _C2 = insert_child(ParentId, ~"child-b"),
    [Loaded] = kura_test_repo:preload(
        kura_test_uuid_schema, [Parent], [children]
    ),
    Children = maps:get(children, Loaded),
    ?assertEqual(2, length(Children)),
    Labels = lists:sort([maps:get(label, C) || C <- Children]),
    ?assertEqual([~"child-a", ~"child-b"], Labels).

%%----------------------------------------------------------------------
%% Bulk
%%----------------------------------------------------------------------

t_uuid_insert_all() ->
    Entries = [
        #{id => uuid(), name => ~"Bulk1"},
        #{id => uuid(), name => ~"Bulk2"},
        #{id => uuid(), name => ~"Bulk3"}
    ],
    {ok, 3} = kura_repo_worker:insert_all(
        kura_test_repo, kura_test_uuid_schema, Entries
    ),
    Q = kura_query:where(
        kura_query:from(kura_test_uuid_schema),
        {name, like, ~"Bulk%"}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(3, length(Results)).

%%----------------------------------------------------------------------
%% update_all
%%----------------------------------------------------------------------

t_uuid_update_all() ->
    I1 = insert_item(~"UpdateAll1", #{status => ~"pending"}),
    I2 = insert_item(~"UpdateAll2", #{status => ~"pending"}),
    _I3 = insert_item(~"UpdateAll3", #{status => ~"done"}),
    Q = kura_query:where(
        kura_query:from(kura_test_uuid_schema),
        {status, ~"pending"}
    ),
    {ok, 2} = kura_repo_worker:update_all(kura_test_repo, Q, #{status => ~"processed"}),
    {ok, R1} = kura_test_repo:get(kura_test_uuid_schema, maps:get(id, I1)),
    {ok, R2} = kura_test_repo:get(kura_test_uuid_schema, maps:get(id, I2)),
    ?assertEqual(~"processed", maps:get(status, R1)),
    ?assertEqual(~"processed", maps:get(status, R2)).

%%----------------------------------------------------------------------
%% Delete
%%----------------------------------------------------------------------

t_uuid_delete() ->
    Item = insert_item(~"ToDelete"),
    Id = maps:get(id, Item),
    CS = kura_changeset:cast(kura_test_uuid_schema, Item, #{}, []),
    {ok, _} = kura_test_repo:delete(CS),
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_uuid_schema, Id)).

%%----------------------------------------------------------------------
%% Reload
%%----------------------------------------------------------------------

t_uuid_reload() ->
    Item = insert_item(~"ReloadMe"),
    Id = maps:get(id, Item),
    %% Update directly via SQL
    {ok, _} = kura_test_repo:query(
        "UPDATE uuid_items SET name = 'Reloaded' WHERE id = $1", [Id]
    ),
    {ok, Reloaded} = kura_repo_worker:reload(kura_test_repo, kura_test_uuid_schema, Item),
    ?assertEqual(~"Reloaded", maps:get(name, Reloaded)),
    ?assertEqual(Id, maps:get(id, Reloaded)).
