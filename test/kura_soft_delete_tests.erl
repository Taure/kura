-module(kura_soft_delete_tests).
-moduledoc "Integration tests for soft delete functionality.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

soft_delete_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Basic soft delete
            {"soft_delete sets deleted_at", fun t_soft_delete/0},
            {"soft-deleted records excluded from all()", fun t_excluded_from_all/0},
            {"soft-deleted records excluded from get()", fun t_excluded_from_get/0},
            {"soft-deleted records excluded from get_by()", fun t_excluded_from_get_by/0},
            {"soft-deleted records excluded from one()", fun t_excluded_from_one/0},
            {"soft-deleted records excluded from exists()", fun t_excluded_from_exists/0},
            {"soft-deleted records excluded from count", fun t_excluded_from_count/0},

            %% with_deleted
            {"with_deleted includes soft-deleted records", fun t_with_deleted/0},
            {"with_deleted in get-style query", fun t_with_deleted_get/0},

            %% only_deleted
            {"only_deleted returns only soft-deleted", fun t_only_deleted/0},

            %% restore
            {"restore clears deleted_at", fun t_restore/0},
            {"restored record visible in normal queries", fun t_restore_visible/0},

            %% edge cases
            {"soft_delete on non-soft-deletable schema", fun t_not_soft_deletable/0},
            {"restore on non-soft-deletable schema", fun t_restore_not_soft_deletable/0},
            {"hard delete still works on soft-deletable schema", fun t_hard_delete/0},
            {"update_all respects soft delete filter", fun t_update_all_filter/0},
            {"delete_all respects soft delete filter", fun t_delete_all_filter/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS soft_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  status VARCHAR(255) DEFAULT 'active',"
        "  deleted_at TIMESTAMPTZ,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query("TRUNCATE soft_items", []),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS soft_items CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_item(Name) ->
    CS = kura_changeset:cast(kura_test_soft_delete_schema, #{}, #{name => Name}, [name]),
    {ok, Record} = kura_test_repo:insert(CS),
    Record.

soft_delete_item(Record) ->
    CS = kura_changeset:cast(kura_test_soft_delete_schema, Record, #{}, []),
    kura_repo_worker:soft_delete(kura_test_repo, CS).

%%----------------------------------------------------------------------
%% Basic soft delete
%%----------------------------------------------------------------------

t_soft_delete() ->
    Item = insert_item(~"Alice"),
    {ok, Deleted} = soft_delete_item(Item),
    ?assertNotEqual(undefined, maps:get(deleted_at, Deleted)),
    ?assertEqual(maps:get(id, Item), maps:get(id, Deleted)).

t_excluded_from_all() ->
    _Live = insert_item(~"Live1"),
    Dead = insert_item(~"Dead1"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:from(kura_test_soft_delete_schema),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(~"Live1", Names)),
    ?assertNot(lists:member(~"Dead1", Names)).

t_excluded_from_get() ->
    Item = insert_item(~"GetDead"),
    Id = maps:get(id, Item),
    {ok, _} = soft_delete_item(Item),
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_soft_delete_schema, Id)).

t_excluded_from_get_by() ->
    Item = insert_item(~"ByDead"),
    {ok, _} = soft_delete_item(Item),
    Result = kura_repo_worker:get_by(kura_test_repo, kura_test_soft_delete_schema, [
        {name, ~"ByDead"}
    ]),
    ?assertEqual({error, not_found}, Result).

t_excluded_from_one() ->
    Item = insert_item(~"OneDead"),
    {ok, _} = soft_delete_item(Item),
    Q = kura_query:where(kura_query:from(kura_test_soft_delete_schema), {name, ~"OneDead"}),
    ?assertEqual({error, not_found}, kura_repo_worker:one(kura_test_repo, Q)).

t_excluded_from_exists() ->
    Item = insert_item(~"ExistsDead"),
    {ok, _} = soft_delete_item(Item),
    Q = kura_query:where(kura_query:from(kura_test_soft_delete_schema), {name, ~"ExistsDead"}),
    ?assertEqual({ok, false}, kura_repo_worker:exists(kura_test_repo, Q)).

t_excluded_from_count() ->
    _Live = insert_item(~"CountLive"),
    Dead = insert_item(~"CountDead"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:where(
        kura_query:from(kura_test_soft_delete_schema),
        {name, like, ~"Count%"}
    ),
    Q1 = kura_query:count(Q),
    {ok, [#{count := Count}]} = kura_test_repo:all(Q1),
    ?assertEqual(1, Count).

%%----------------------------------------------------------------------
%% with_deleted
%%----------------------------------------------------------------------

t_with_deleted() ->
    Live = insert_item(~"WDLive"),
    Dead = insert_item(~"WDDead"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:with_deleted(kura_query:from(kura_test_soft_delete_schema)),
    Q1 = kura_query:where(Q, {name, like, ~"WD%"}),
    {ok, Results} = kura_test_repo:all(Q1),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(~"WDLive", Names)),
    ?assert(lists:member(~"WDDead", Names)),
    _ = Live.

t_with_deleted_get() ->
    Item = insert_item(~"WDGet"),
    Id = maps:get(id, Item),
    {ok, _} = soft_delete_item(Item),
    %% Normal get should not find it
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_soft_delete_schema, Id)),
    %% with_deleted query should find it
    Q = kura_query:with_deleted(
        kura_query:where(kura_query:from(kura_test_soft_delete_schema), {id, Id})
    ),
    {ok, [Found]} = kura_test_repo:all(Q),
    ?assertEqual(Id, maps:get(id, Found)).

%%----------------------------------------------------------------------
%% only_deleted
%%----------------------------------------------------------------------

t_only_deleted() ->
    _Live = insert_item(~"ODLive"),
    Dead = insert_item(~"ODDead"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:only_deleted(kura_query:from(kura_test_soft_delete_schema)),
    Q1 = kura_query:where(Q, {name, like, ~"OD%"}),
    {ok, Results} = kura_test_repo:all(Q1),
    Names = [maps:get(name, R) || R <- Results],
    ?assertNot(lists:member(~"ODLive", Names)),
    ?assert(lists:member(~"ODDead", Names)).

%%----------------------------------------------------------------------
%% restore
%%----------------------------------------------------------------------

t_restore() ->
    Item = insert_item(~"RestoreMe"),
    {ok, Deleted} = soft_delete_item(Item),
    ?assertNotEqual(undefined, maps:get(deleted_at, Deleted)),
    CS = kura_changeset:cast(kura_test_soft_delete_schema, Deleted, #{}, []),
    {ok, Restored} = kura_repo_worker:restore(kura_test_repo, CS),
    ?assertEqual(undefined, maps:get(deleted_at, Restored)).

t_restore_visible() ->
    Item = insert_item(~"RestoreVisible"),
    Id = maps:get(id, Item),
    {ok, Deleted} = soft_delete_item(Item),
    CS = kura_changeset:cast(kura_test_soft_delete_schema, Deleted, #{}, []),
    {ok, _} = kura_repo_worker:restore(kura_test_repo, CS),
    {ok, Found} = kura_test_repo:get(kura_test_soft_delete_schema, Id),
    ?assertEqual(~"RestoreVisible", maps:get(name, Found)).

%%----------------------------------------------------------------------
%% Edge cases
%%----------------------------------------------------------------------

t_not_soft_deletable() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{id => 1, name => ~"X", email => ~"x@x.com"}, #{}, []
    ),
    ?assertEqual({error, not_soft_deletable}, kura_repo_worker:soft_delete(kura_test_repo, CS)).

t_restore_not_soft_deletable() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{id => 1, name => ~"X", email => ~"x@x.com"}, #{}, []
    ),
    ?assertEqual({error, not_soft_deletable}, kura_repo_worker:restore(kura_test_repo, CS)).

t_hard_delete() ->
    Item = insert_item(~"HardDelete"),
    Id = maps:get(id, Item),
    CS = kura_changeset:cast(kura_test_soft_delete_schema, Item, #{}, []),
    {ok, _} = kura_test_repo:delete(CS),
    %% Even with_deleted should not find it
    Q = kura_query:with_deleted(
        kura_query:where(kura_query:from(kura_test_soft_delete_schema), {id, Id})
    ),
    {ok, []} = kura_test_repo:all(Q).

t_update_all_filter() ->
    _Live = insert_item(~"UALive"),
    Dead = insert_item(~"UADead"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:where(
        kura_query:from(kura_test_soft_delete_schema),
        {name, like, ~"UA%"}
    ),
    {ok, Count} = kura_repo_worker:update_all(kura_test_repo, Q, #{status => ~"updated"}),
    ?assertEqual(1, Count).

t_delete_all_filter() ->
    _Live = insert_item(~"DALive"),
    Dead = insert_item(~"DADead"),
    {ok, _} = soft_delete_item(Dead),
    Q = kura_query:where(
        kura_query:from(kura_test_soft_delete_schema),
        {name, like, ~"DA%"}
    ),
    {ok, Count} = kura_repo_worker:delete_all(kura_test_repo, Q),
    ?assertEqual(1, Count).
