-module(kura_tenant_cte_SUITE).
-moduledoc """
What PostgreSQL reads where a CTE's body names the CTE itself, or a CTE
listed after it, and that `require_tenant` judges the query by it.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    a_body_naming_its_own_cte_reads_the_table/1,
    a_body_naming_a_later_cte_reads_the_table/1,
    a_body_naming_an_earlier_cte_reads_the_cte/1,
    without_a_tenant_both_are_refused/1,
    under_a_tenant_both_read_only_its_rows/1
]).

-define(REPO, kura_tenant_cte_test_repo).
-define(ITEM, kura_test_cte_scope_item_schema).
-define(NOTE, kura_test_cte_scope_note_schema).
-define(REFUSED, {kura, {tenant_required, ?ITEM}}).

all() ->
    [
        a_body_naming_its_own_cte_reads_the_table,
        a_body_naming_a_later_cte_reads_the_table,
        a_body_naming_an_earlier_cte_reads_the_cte,
        without_a_tenant_both_are_refused,
        under_a_tenant_both_read_only_its_rows
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
        require_tenant => true
    }),
    ok = kura_repo_worker:start(?REPO),
    Config.

end_per_suite(_Config) ->
    _ = [drop(Table) || Table <- tables()],
    application:unset_env(kura, ?REPO),
    application:unset_env(kura, ensure_database),
    ok.

init_per_testcase(_TC, Config) ->
    [
        begin
            drop(Table),
            {ok, _} = kura_repo_worker:query(
                ?REPO,
                [
                    ~"CREATE TABLE ",
                    Table,
                    ~" (id BIGSERIAL PRIMARY KEY, org_id INTEGER NOT NULL, name TEXT)"
                ],
                []
            )
        end
     || Table <- tables()
    ],
    {ok, _} = kura_repo_worker:query(
        ?REPO,
        ~"INSERT INTO cte_scope_items (org_id, name) VALUES (1, 'item 1a'), (1, 'item 1b'), (2, 'item 2a')",
        []
    ),
    {ok, _} = kura_repo_worker:query(
        ?REPO,
        ~"INSERT INTO cte_scope_notes (org_id, name) VALUES (1, 'note 1'), (2, 'note 2')",
        []
    ),
    kura_query_cache:flush(),
    Config.

end_per_testcase(_TC, _Config) ->
    kura_tenant:clear_tenant(),
    ok.

tables() -> [~"cte_scope_items", ~"cte_scope_notes"].

drop(Table) ->
    {ok, _} = kura_repo_worker:query(?REPO, [~"DROP TABLE IF EXISTS ", Table], []).

%% WITH "cte_scope_items" AS (SELECT * FROM "cte_scope_items")
%% SELECT * FROM "cte_scope_items"
own_name() ->
    kura_query:with_cte(
        kura_query:from(~"cte_scope_items"), ~"cte_scope_items", kura_query:from(?ITEM)
    ).

%% WITH "picked" AS (SELECT * FROM "cte_scope_items"),
%%      "cte_scope_items" AS (SELECT * FROM "cte_scope_notes")
%% SELECT * FROM "picked"
later_name() ->
    kura_query:with_cte(
        kura_query:with_cte(kura_query:from(~"picked"), ~"picked", kura_query:from(?ITEM)),
        ~"cte_scope_items",
        kura_query:from(?NOTE)
    ).

%% WITH "cte_scope_items" AS (SELECT * FROM "cte_scope_notes"),
%%      "picked" AS (SELECT * FROM "cte_scope_items")
%% SELECT * FROM "picked"
earlier_name() ->
    kura_query:with_cte(
        kura_query:with_cte(
            kura_query:from(~"picked"), ~"cte_scope_items", kura_query:from(?NOTE)
        ),
        ~"picked",
        kura_query:from(?ITEM)
    ).

names(Query) ->
    {ok, Rows} = kura_repo_worker:all(?REPO, Query),
    lists:sort([Name || #{name := Name} <- Rows]).

every_tenant(Query) ->
    kura_tenant:without_tenant(fun() -> names(Query) end).

under(Org, Query) ->
    kura_tenant:with_tenant({attribute, {org_id, Org}}, fun() -> names(Query) end).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

a_body_naming_its_own_cte_reads_the_table(_Config) ->
    ?assertEqual([~"item 1a", ~"item 1b", ~"item 2a"], every_tenant(own_name())).

a_body_naming_a_later_cte_reads_the_table(_Config) ->
    ?assertEqual([~"item 1a", ~"item 1b", ~"item 2a"], every_tenant(later_name())).

a_body_naming_an_earlier_cte_reads_the_cte(_Config) ->
    ?assertEqual([~"note 1", ~"note 2"], every_tenant(earlier_name())).

without_a_tenant_both_are_refused(_Config) ->
    [
        ?assertError(?REFUSED, kura_repo_worker:all(?REPO, Query))
     || Query <- [own_name(), later_name(), own_name(), later_name()]
    ].

under_a_tenant_both_read_only_its_rows(_Config) ->
    [
        begin
            ?assertEqual([~"item 1a", ~"item 1b"], under(1, Query)),
            ?assertEqual([~"item 2a"], under(2, Query))
        end
     || Query <- [own_name(), later_name()]
    ].
