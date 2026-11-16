-module(kura_fragment_group_SUITE).
-moduledoc """
The rows a fragment with an `OR` in it matches, in PostgreSQL, beside the
conditions kura adds: the tenant's, the soft-delete filter and the keys of
a conditional preload. Two tenants hold rows the fragment matches, so each
case fails if the fragment's `OR` reaches past those conditions.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    a_select_returns_the_tenants_rows/1,
    update_all_leaves_other_tenants_rows/1,
    delete_all_leaves_other_tenants_rows/1,
    a_soft_deleted_row_is_not_read/1,
    a_conditional_preload_reads_the_owners_rows/1,
    a_line_comment_keeps_the_conditions_after_it/1
]).

-define(REPO, kura_fragment_group_test_repo).
-define(ITEM, kura_test_frag_item_schema).
-define(OWNER, kura_test_frag_owner_schema).
-define(ONE, {attribute, {org_id, 1}}).
-define(APPLE, {fragment, ~"name = ? OR note = ?", [~"apple", ~"apple"]}).

all() ->
    [
        a_select_returns_the_tenants_rows,
        update_all_leaves_other_tenants_rows,
        delete_all_leaves_other_tenants_rows,
        a_soft_deleted_row_is_not_read,
        a_conditional_preload_reads_the_owners_rows,
        a_line_comment_keeps_the_conditions_after_it
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    application:set_env(kura, ensure_database, false),
    application:set_env(kura, ?REPO, #{
        pool => ?REPO,
        dialect => kura_dialect_pg,
        pool_module => kura_pool_minato,
        driver_module => kura_driver_minato,
        database => ~"kura_test",
        hostname => ~"localhost",
        port => 5555,
        username => ~"postgres",
        password => ~"root",
        pool_size => 2,
        require_tenant => false
    }),
    ok = kura_repo_worker:start(?REPO),
    Config.

end_per_suite(_Config) ->
    drop_tables(),
    application:unset_env(kura, ?REPO),
    application:unset_env(kura, ensure_database),
    ok.

%% Tenant 1 and tenant 2 each hold a row named apple and a row the fragment
%% matches by its note; tenant 1 also holds a deleted apple.
init_per_testcase(_TC, Config) ->
    drop_tables(),
    {ok, _} = raw(
        ~"CREATE TABLE frag_owners (id BIGSERIAL PRIMARY KEY, org_id INTEGER NOT NULL, name TEXT)"
    ),
    {ok, _} = raw(
        <<
            "CREATE TABLE frag_items (id BIGSERIAL PRIMARY KEY, org_id INTEGER NOT NULL,"
            " owner_id BIGINT, name TEXT, note TEXT, deleted_at TIMESTAMPTZ)"
        >>
    ),
    {ok, _} = raw(
        ~"INSERT INTO frag_owners (id, org_id, name) VALUES (1, 1, 'one'), (2, 2, 'two')"
    ),
    {ok, _} = raw(
        <<
            "INSERT INTO frag_items (org_id, owner_id, name, note, deleted_at) VALUES"
            " (1, 1, 'apple', 'one apple', NULL),"
            " (1, 1, 'pear', 'one pear', NULL),"
            " (1, 1, 'apple', 'one deleted', now()),"
            " (2, 2, 'apple', 'two apple', NULL),"
            " (2, 2, 'plum', 'apple', NULL)"
        >>
    ),
    kura_query_cache:flush(),
    Config.

end_per_testcase(_TC, _Config) ->
    kura_tenant:clear_tenant(),
    ok.

drop_tables() ->
    {ok, _} = raw(~"DROP TABLE IF EXISTS frag_items"),
    {ok, _} = raw(~"DROP TABLE IF EXISTS frag_owners"),
    ok.

raw(SQL) ->
    kura_repo_worker:query(?REPO, SQL, []).

notes(Rows) ->
    lists:sort([Note || #{note := Note} <- Rows]).

every_note() ->
    {ok, Rows} = raw(~"SELECT note FROM frag_items"),
    notes(Rows).

apples() ->
    kura_query:where(kura_query:from(?ITEM), ?APPLE).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

a_select_returns_the_tenants_rows(_Config) ->
    {ok, Rows} = kura_tenant:with_tenant(?ONE, {kura_repo_worker, all, [?REPO, apples()]}),
    ?assertEqual([~"one apple"], notes(Rows)),
    {ok, Cached} = kura_tenant:with_tenant(?ONE, {kura_repo_worker, all, [?REPO, apples()]}),
    ?assertEqual([~"one apple"], notes(Cached)).

update_all_leaves_other_tenants_rows(_Config) ->
    ?assertEqual(
        {ok, 1},
        kura_tenant:with_tenant(
            ?ONE, {kura_repo_worker, update_all, [?REPO, apples(), #{note => ~"changed"}]}
        )
    ),
    ?assertEqual(
        [~"apple", ~"changed", ~"one deleted", ~"one pear", ~"two apple"], every_note()
    ).

delete_all_leaves_other_tenants_rows(_Config) ->
    ?assertEqual(
        {ok, 1}, kura_tenant:with_tenant(?ONE, {kura_repo_worker, delete_all, [?REPO, apples()]})
    ),
    ?assertEqual([~"apple", ~"one deleted", ~"one pear", ~"two apple"], every_note()).

a_soft_deleted_row_is_not_read(_Config) ->
    {ok, Rows} = kura_repo_worker:all(?REPO, apples()),
    ?assertEqual([~"apple", ~"one apple", ~"two apple"], notes(Rows)).

%% Without the limit the rows of another owner would be read and then left
%% out; with it one of them takes the place of the owner's own row.
a_conditional_preload_reads_the_owners_rows(_Config) ->
    Opts = #{
        where => {fragment, ~"note = ? OR name = ?", [~"one apple", ~"plum"]},
        order_by => [{name, desc}],
        limit => 1
    },
    [Owner] = kura_repo_worker:preload(?REPO, ?OWNER, [#{id => 1, org_id => 1}], [{items, Opts}]),
    ?assertEqual([~"one apple"], notes(maps:get(items, Owner))).

a_line_comment_keeps_the_conditions_after_it(_Config) ->
    Q = kura_query:where(kura_query:from(?ITEM), {fragment, ~"name = ? -- by name", [~"apple"]}),
    {ok, Rows} = kura_tenant:with_tenant(?ONE, {kura_repo_worker, all, [?REPO, Q]}),
    ?assertEqual([~"one apple"], notes(Rows)).
