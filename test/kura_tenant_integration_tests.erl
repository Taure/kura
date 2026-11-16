-module(kura_tenant_integration_tests).
-moduledoc """
Attribute-mode tenancy against a real database. Two tenants hold rows that
an unscoped query would visibly return, so each test fails if any path
reads, counts or writes across the tenant boundary.
""".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(ITEMS, kura_test_tenant_item_schema).
-define(LINKS, kura_test_tenant_shared_link_schema).
-define(ONE, {attribute, {org_id, 1}}).
-define(TWO, {attribute, {org_id, 2}}).

tenant_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        {foreach, fun seed/0, fun(_) -> ok end, [
            {"a union's second branch is scoped", fun union_branches_are_scoped/0},
            {"an exists subquery does not see another tenant", fun exists_is_scoped/0},
            {"a not_exists subquery does not see another tenant", fun not_exists_is_scoped/0},
            {"a CTE read through a subquery is scoped", fun cte_is_scoped/0},
            {"an in subquery is scoped", fun in_subquery_is_scoped/0},
            {"count and aggregates are scoped", fun aggregates_are_scoped/0},
            {"a stream returns only the tenant's rows", fun stream_is_scoped/0},
            {"the cached query is not shared across tenants", fun cache_is_per_tenant/0},
            {"update_all with a subquery touches nothing foreign", fun update_all_subquery/0},
            {"update of a foreign record is not found", fun update_foreign_record/0},
            {"delete of a foreign record is not found", fun delete_foreign_record/0},
            {"a locked update of a foreign record is stale", fun locked_update_foreign_record/0},
            {"a tenant still updates and deletes its own rows", fun own_rows_are_writable/0},
            {"an update cannot move a row to another tenant", fun update_cannot_move_row/0},
            {"update_all cannot move rows to another tenant", fun update_all_cannot_move_rows/0},
            {"insert_all stamps the tenant on every row", fun insert_all_stamps_tenant/0},
            {"insert with options stamps the tenant", fun insert_with_opts_stamps_tenant/0},
            {"an upsert leaves a foreign row alone", fun upsert_skips_foreign_row/0},
            {"an upsert updates the tenant's own row", fun upsert_updates_own_row/0},
            {"a bulk upsert leaves a foreign row alone", fun bulk_upsert_skips_foreign_row/0},
            {"a bulk upsert updates the tenant's own row", fun bulk_upsert_updates_own_row/0},
            {"a nested child update cannot reach a foreign child",
                fun nested_child_update_cannot_reach_a_foreign_child/0},
            {"associations cannot be written onto a foreign record",
                fun assoc_onto_foreign_record/0},
            {"associations can be written onto the tenant's own record",
                fun assoc_onto_own_record/0},
            {"a many_to_many link to another tenant's row is refused",
                fun link_to_foreign_row_is_refused/0},
            {"a many_to_many link to the tenant's own row is written",
                fun link_to_own_row_is_written/0},
            {"a join between two tenant tables is refused", fun join_of_tenant_tables_fails/0},
            {"a schema that leaves out the tenant column its table has is scoped",
                fun undeclared_tenant_column_is_scoped/0},
            {"a table without the tenant column is refused by the database",
                fun missing_tenant_column_is_refused_by_the_database/0},
            {"a schema every tenant shares is read whole", fun shared_schema_is_read_whole/0},
            {"a shared schema beside a tenant's table keeps the tenant's rows",
                fun shared_schema_joined_is_scoped/0},
            {"a preload of a shared schema reads its rows", fun shared_schema_is_preloaded/0},
            {"update_all and delete_all of a shared schema reach every row",
                fun shared_schema_bulk_writes/0},
            {"preloading several associations stays in the tenant",
                fun parallel_preload_is_scoped/0},
            {"preloading several associations stays in the prefix schema",
                fun parallel_preload_keeps_prefix/0},
            {"count, aggregates and streams stay in the prefix schema",
                fun prefix_scopes_count_and_stream/0}
        ]}
    end}.

setup() ->
    application:ensure_all_started(minato),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    drop_tables(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tenant_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  org_id INTEGER NOT NULL,"
        "  name VARCHAR(255) NOT NULL UNIQUE,"
        "  score INTEGER,"
        "  lock_version INTEGER NOT NULL DEFAULT 0"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tenant_item_links ("
        "  item_id BIGINT NOT NULL,"
        "  linked_id BIGINT NOT NULL,"
        "  PRIMARY KEY (item_id, linked_id)"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tenant_notes ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  org_id INTEGER NOT NULL,"
        "  item_id BIGINT NOT NULL,"
        "  body VARCHAR(255)"
        ")",
        []
    ),
    ok.

teardown(_) ->
    _ = kura_tenant:clear_tenant(),
    drop_tables().

drop_tables() ->
    kura_test_repo:query("DROP SCHEMA IF EXISTS tenant_p1 CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tenant_item_links CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tenant_notes CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tenant_items CASCADE", []),
    ok.

seed() ->
    _ = kura_tenant:clear_tenant(),
    kura_query_cache:flush(),
    {ok, _} = kura_test_repo:query(
        "TRUNCATE tenant_items, tenant_notes, tenant_item_links RESTART IDENTITY", []
    ),
    {ok, _} = kura_test_repo:query(
        "INSERT INTO tenant_items (org_id, name, score) VALUES "
        "(1, 'one-a', 1), (1, 'one-b', 2), (2, 'two-a', 10), (2, 'two-only', 20)",
        []
    ),
    ok.

%%----------------------------------------------------------------------
%% Reads
%%----------------------------------------------------------------------

union_branches_are_scoped() ->
    Q = kura_query:union(
        kura_query:where(kura_query:from(?ITEMS), {name, ~"one-a"}),
        kura_query:where(kura_query:from(?ITEMS), {score, '>', 0})
    ),
    {ok, Rows} = as(?ONE, fun() -> kura_test_repo:all(Q) end),
    ?assertEqual([~"one-a", ~"one-b"], names(Rows)).

exists_is_scoped() ->
    Foreign = kura_query:where(kura_query:from(?ITEMS), {name, ~"two-only"}),
    Q = kura_query:where(kura_query:from(?ITEMS), {exists, {subquery, Foreign}}),
    ?assertEqual({ok, []}, as(?ONE, fun() -> kura_test_repo:all(Q) end)).

not_exists_is_scoped() ->
    Foreign = kura_query:where(kura_query:from(?ITEMS), {name, ~"two-only"}),
    Q = kura_query:where(kura_query:from(?ITEMS), {not_exists, {subquery, Foreign}}),
    {ok, Rows} = as(?ONE, fun() -> kura_test_repo:all(Q) end),
    ?assertEqual([~"one-a", ~"one-b"], names(Rows)).

cte_is_scoped() ->
    Cte = kura_query:select(kura_query:where(kura_query:from(?ITEMS), {name, ~"two-only"}), [name]),
    Q0 = kura_query:where(
        kura_query:from(?ITEMS),
        {exists, {subquery, kura_query:from(kura_test_tenant_cte_schema)}}
    ),
    Q = kura_query:with_cte(Q0, ~"tenant_cte", Cte),
    ?assertEqual({ok, []}, as(?ONE, fun() -> kura_test_repo:all(Q) end)).

in_subquery_is_scoped() ->
    Sub = kura_query:select(
        kura_query:where(kura_query:from(?ITEMS), {name, ~"two-only"}), [lock_version]
    ),
    Q = kura_query:where(kura_query:from(?ITEMS), {lock_version, in, {subquery, Sub}}),
    ?assertEqual({ok, []}, as(?ONE, fun() -> kura_test_repo:all(Q) end)).

aggregates_are_scoped() ->
    Q = kura_query:from(?ITEMS),
    as(?ONE, fun() ->
        ?assertEqual({ok, 2}, kura_repo_worker:count(kura_test_repo, Q)),
        ?assertEqual({ok, 3}, kura_repo_worker:aggregate(kura_test_repo, Q, {sum, score})),
        ?assertEqual({ok, 2}, kura_repo_worker:aggregate(kura_test_repo, Q, {max, score}))
    end).

stream_is_scoped() ->
    Self = self(),
    ok = as(?ONE, fun() ->
        kura_stream:stream(kura_test_repo, kura_query:from(?ITEMS), fun(Batch) ->
            Self ! {batch, Batch},
            ok
        end)
    end),
    ?assertEqual([~"one-a", ~"one-b"], names(drain_batches([]))).

cache_is_per_tenant() ->
    Q = kura_query:union(
        kura_query:where(kura_query:from(?ITEMS), {score, '<', 5}),
        kura_query:where(kura_query:from(?ITEMS), {score, '>', 5})
    ),
    {ok, One} = as(?ONE, fun() -> kura_test_repo:all(Q) end),
    {ok, Two} = as(?TWO, fun() -> kura_test_repo:all(Q) end),
    ?assertEqual([~"one-a", ~"one-b"], names(One)),
    ?assertEqual([~"two-a", ~"two-only"], names(Two)).

update_all_subquery() ->
    Foreign = kura_query:where(kura_query:from(?ITEMS), {name, ~"two-only"}),
    Q = kura_query:where(kura_query:from(?ITEMS), {exists, {subquery, Foreign}}),
    ?assertEqual(
        {ok, 0}, as(?ONE, fun() -> kura_test_repo:update_all(Q, #{score => 0}) end)
    ).

%%----------------------------------------------------------------------
%% Writes
%%----------------------------------------------------------------------

update_foreign_record() ->
    Id = id_of(~"two-only"),
    CS = kura_changeset:cast(?ITEMS, #{id => Id}, #{name => ~"taken"}, [name]),
    ?assertMatch({error, #kura_changeset{}}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertEqual({2, ~"two-only"}, raw_row(Id)).

delete_foreign_record() ->
    Id = id_of(~"two-only"),
    CS = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
    ?assertEqual({error, not_found}, as(?ONE, fun() -> kura_test_repo:delete(CS) end)),
    ?assertEqual({2, ~"two-only"}, raw_row(Id)).

locked_update_foreign_record() ->
    Id = id_of(~"two-only"),
    CS0 = kura_changeset:cast(?ITEMS, #{id => Id, lock_version => 0}, #{name => ~"taken"}, [name]),
    CS = kura_changeset:optimistic_lock(CS0, lock_version),
    ?assertEqual({error, stale}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertEqual({2, ~"two-only"}, raw_row(Id)).

own_rows_are_writable() ->
    Id = id_of(~"one-a"),
    as(?ONE, fun() ->
        CS = kura_changeset:cast(?ITEMS, #{id => Id}, #{name => ~"one-renamed"}, [name]),
        ?assertMatch({ok, #{name := ~"one-renamed"}}, kura_test_repo:update(CS)),
        Del = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
        ?assertMatch({ok, _}, kura_test_repo:delete(Del))
    end).

update_cannot_move_row() ->
    Id = id_of(~"one-a"),
    CS = kura_changeset:cast(?ITEMS, #{id => Id}, #{org_id => 2}, [org_id]),
    ?assertMatch({ok, #{org_id := 1}}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertEqual({1, ~"one-a"}, raw_row(Id)).

update_all_cannot_move_rows() ->
    {ok, 2} = as(?ONE, fun() ->
        kura_test_repo:update_all(kura_query:from(?ITEMS), #{org_id => 2})
    end),
    ?assertEqual({1, ~"one-a"}, raw_row(id_of(~"one-a"))).

insert_all_stamps_tenant() ->
    Entries = [
        #{name => ~"bulk-missing", score => 1}, #{name => ~"bulk-foreign", org_id => 2, score => 1}
    ],
    {ok, 2} = as(?ONE, fun() -> kura_test_repo:insert_all(?ITEMS, Entries) end),
    ?assertEqual({1, ~"bulk-missing"}, raw_row(id_of(~"bulk-missing"))),
    ?assertEqual({1, ~"bulk-foreign"}, raw_row(id_of(~"bulk-foreign"))).

insert_with_opts_stamps_tenant() ->
    CS = kura_changeset:cast(?ITEMS, #{}, #{name => ~"opts", org_id => 2}, [name, org_id]),
    {ok, _} = as(?ONE, fun() -> kura_test_repo:insert(CS, #{}) end),
    ?assertEqual({1, ~"opts"}, raw_row(id_of(~"opts"))).

upsert_skips_foreign_row() ->
    Id = id_of(~"two-only"),
    CS = kura_changeset:cast(
        ?ITEMS, #{}, #{name => ~"two-only", org_id => 1, score => 99}, [name, org_id, score]
    ),
    Result = as(?ONE, fun() ->
        kura_test_repo:insert(CS, #{on_conflict => {name, replace_all}})
    end),
    ?assertMatch({ok, Skipped} when not is_map_key(id, Skipped), Result),
    ?assertEqual({2, ~"two-only"}, raw_row(Id)),
    ?assertEqual(20, raw_score(Id)).

upsert_updates_own_row() ->
    Id = id_of(~"one-a"),
    CS = kura_changeset:cast(
        ?ITEMS, #{}, #{name => ~"one-a", org_id => 1, score => 99}, [name, org_id, score]
    ),
    {ok, Row} = as(?ONE, fun() ->
        kura_test_repo:insert(CS, #{on_conflict => {name, {replace, [score]}}})
    end),
    ?assertMatch(#{score := 99}, Row),
    ?assertEqual(99, raw_score(Id)).

bulk_upsert_skips_foreign_row() ->
    Id = id_of(~"two-only"),
    Entries = [#{name => ~"two-only", org_id => 1, score => 99}],
    ?assertEqual(
        {ok, 0},
        as(?ONE, fun() ->
            kura_test_repo:insert_all(?ITEMS, Entries, #{on_conflict => {name, replace_all}})
        end)
    ),
    ?assertEqual({2, ~"two-only"}, raw_row(Id)),
    ?assertEqual(20, raw_score(Id)).

bulk_upsert_updates_own_row() ->
    Id = id_of(~"one-a"),
    Entries = [#{name => ~"one-a", score => 99}],
    ?assertEqual(
        {ok, 1},
        as(?ONE, fun() ->
            kura_test_repo:insert_all(?ITEMS, Entries, #{on_conflict => {name, {replace, [score]}}})
        end)
    ),
    ?assertEqual(99, raw_score(Id)).

nested_child_update_cannot_reach_a_foreign_child() ->
    OwnId = id_of(~"one-a"),
    {ok, _} = raw("INSERT INTO tenant_notes (org_id, item_id, body) VALUES (2, $1, 'theirs')", [
        OwnId
    ]),
    {ok, [#{id := NoteId}]} = raw("SELECT id FROM tenant_notes WHERE body = 'theirs'", []),
    Child0 = kura_changeset:cast(
        kura_test_tenant_note_schema, #{id => NoteId, item_id => OwnId}, #{body => ~"hijacked"}, [
            body
        ]
    ),
    Child = Child0#kura_changeset{action = update},
    CS = kura_changeset:put_assoc(kura_changeset:cast(?ITEMS, #{id => OwnId}, #{}, []), notes, [
        Child
    ]),
    Result =
        try
            as(?ONE, fun() -> kura_test_repo:update(CS) end)
        catch
            error:Reason -> {raised, Reason}
        end,
    ?assertNotMatch({ok, _}, Result),
    ?assertEqual(
        {ok, [#{body => ~"theirs"}]}, raw("SELECT body FROM tenant_notes WHERE id = $1", [NoteId])
    ).

%%----------------------------------------------------------------------
%% Associations and joins
%%----------------------------------------------------------------------

assoc_onto_foreign_record() ->
    Id = id_of(~"two-only"),
    CS0 = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
    CS = kura_changeset:put_assoc(CS0, notes, [#{body => ~"planted"}]),
    ?assertMatch({error, _}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertEqual({ok, []}, raw("SELECT id FROM tenant_notes WHERE item_id = $1", [Id])).

assoc_onto_own_record() ->
    Id = id_of(~"one-a"),
    CS0 = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
    CS = kura_changeset:put_assoc(CS0, notes, [#{body => ~"mine"}]),
    ?assertMatch({ok, _}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertMatch(
        {ok, [#{org_id := 1}]}, raw("SELECT org_id FROM tenant_notes WHERE item_id = $1", [Id])
    ).

link_to_foreign_row_is_refused() ->
    Id = id_of(~"one-a"),
    CS0 = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
    CS = kura_changeset:put_assoc(CS0, links, [#{id => id_of(~"two-only")}]),
    {error, ErrCS} = as(?ONE, fun() -> kura_test_repo:update(CS) end),
    ?assertEqual([{links, ~"does not exist"}], ErrCS#kura_changeset.errors),
    ?assertEqual({ok, []}, raw("SELECT item_id FROM tenant_item_links", [])).

link_to_own_row_is_written() ->
    Id = id_of(~"one-a"),
    Own = id_of(~"one-b"),
    CS0 = kura_changeset:cast(?ITEMS, #{id => Id}, #{}, []),
    CS = kura_changeset:put_assoc(CS0, links, [#{id => Own}]),
    ?assertMatch({ok, _}, as(?ONE, fun() -> kura_test_repo:update(CS) end)),
    ?assertEqual(
        {ok, [#{item_id => Id, linked_id => Own}]},
        raw("SELECT item_id, linked_id FROM tenant_item_links", [])
    ).

join_of_tenant_tables_fails() ->
    Q = kura_query:join(
        kura_query:from(?ITEMS), inner, kura_test_tenant_note_schema, {id, item_id}
    ),
    ?assertMatch({error, _}, as(?ONE, fun() -> kura_test_repo:all(Q) end)).

undeclared_tenant_column_is_scoped() ->
    Partial = kura_query:from(kura_test_tenant_partial_item_schema),
    {ok, Rows} = as(?ONE, fun() -> kura_test_repo:all(Partial) end),
    ?assertEqual([~"one-a", ~"one-b"], names(Rows)),
    {ok, 2} = as(?ONE, fun() -> kura_test_repo:update_all(Partial, #{score => 7}) end),
    ?assertEqual(
        {ok, [#{score => 7}, #{score => 7}, #{score => 10}, #{score => 20}]},
        raw("SELECT score FROM tenant_items ORDER BY score", [])
    ),
    {ok, 2} = as(?ONE, fun() -> kura_test_repo:delete_all(Partial) end),
    ?assertEqual(
        {ok, [#{name => ~"two-a"}, #{name => ~"two-only"}]},
        raw("SELECT name FROM tenant_items ORDER BY name", [])
    ).

missing_tenant_column_is_refused_by_the_database() ->
    {ok, _} = raw("INSERT INTO tenant_item_links (item_id, linked_id) VALUES (1, 2)", []),
    Links = kura_query:from(kura_test_tenant_link_schema),
    Runs = [
        fun() -> kura_test_repo:all(Links) end,
        fun() -> kura_test_repo:update_all(Links, #{linked_id => 3}) end,
        fun() -> kura_test_repo:delete_all(Links) end
    ],
    [
        ?assertMatch({error, {pgsql_error, #{code := ~"42703"}}}, as(?ONE, Run))
     || Run <- Runs
    ],
    ?assertEqual({ok, [#{linked_id => 2}]}, raw("SELECT linked_id FROM tenant_item_links", [])).

%% Links 1-2 and 3-4 are between two rows of one tenant, 2-3 crosses.
seed_links() ->
    {ok, _} = raw(
        "INSERT INTO tenant_item_links (item_id, linked_id) VALUES (1, 2), (2, 3), (3, 4)", []
    ),
    kura_query:from(?LINKS).

linked(Rows) ->
    lists:sort([{Item, Linked} || #{item_id := Item, linked_id := Linked} <- Rows]).

shared_schema_is_read_whole() ->
    Links = seed_links(),
    {ok, Rows} = as(?ONE, fun() -> kura_test_repo:all(Links) end),
    ?assertEqual([{1, 2}, {2, 3}, {3, 4}], linked(Rows)),
    ?assertEqual({ok, 3}, as(?ONE, fun() -> kura_repo_worker:count(kura_test_repo, Links) end)),
    ?assertEqual({ok, true}, as(?TWO, fun() -> kura_test_repo:exists(Links) end)),
    Ids = kura_query:select(kura_query:where(Links, {linked_id, 3}), [item_id]),
    Linking = kura_query:where(kura_query:from(?ITEMS), {id, in, {subquery, Ids}}),
    {ok, OfOne} = as(?ONE, fun() -> kura_test_repo:all(Linking) end),
    {ok, OfTwo} = as(?TWO, fun() -> kura_test_repo:all(Linking) end),
    ?assertEqual({[~"one-b"], []}, {names(OfOne), names(OfTwo)}).

shared_schema_joined_is_scoped() ->
    Links = seed_links(),
    FromShared = kura_query:select(kura_query:join(Links, inner, ?ITEMS, {item_id, id}), [
        item_id, linked_id
    ]),
    FromTenant = kura_query:select(
        kura_query:join(kura_query:from(?ITEMS), inner, ?LINKS, {id, item_id}), [name]
    ),
    {ok, OfOne} = as(?ONE, fun() -> kura_test_repo:all(FromShared) end),
    {ok, OfTwo} = as(?TWO, fun() -> kura_test_repo:all(FromShared) end),
    ?assertEqual([{1, 2}, {2, 3}], linked(OfOne)),
    ?assertEqual([{3, 4}], linked(OfTwo)),
    {ok, Named} = as(?TWO, fun() -> kura_test_repo:all(FromTenant) end),
    ?assertEqual([~"two-a"], names(Named)).

shared_schema_is_preloaded() ->
    _ = seed_links(),
    {ok, Items} = as(?ONE, fun() -> kura_test_repo:all(kura_query:from(?ITEMS)) end),
    Loaded = as(?ONE, fun() -> kura_test_repo:preload(?ITEMS, Items, [shared_links]) end),
    ?assertEqual(
        [{~"one-a", [{1, 2}]}, {~"one-b", [{2, 3}]}],
        lists:sort([{Name, linked(Links)} || #{name := Name, shared_links := Links} <- Loaded])
    ).

shared_schema_bulk_writes() ->
    Links = seed_links(),
    ?assertEqual(
        {ok, 3}, as(?ONE, fun() -> kura_test_repo:update_all(Links, #{linked_id => 9}) end)
    ),
    ?assertEqual(
        {ok, [#{linked_id => 9}, #{linked_id => 9}, #{linked_id => 9}]},
        raw("SELECT linked_id FROM tenant_item_links", [])
    ),
    ?assertEqual({ok, 3}, as(?TWO, fun() -> kura_test_repo:delete_all(Links) end)),
    ?assertEqual({ok, []}, raw("SELECT linked_id FROM tenant_item_links", [])).

parallel_preload_is_scoped() ->
    Foreign = id_of(~"two-only"),
    {ok, _} = raw("INSERT INTO tenant_notes (org_id, item_id, body) VALUES (1, $1, 'planted')", [
        Foreign
    ]),
    Q = kura_query:preload(kura_query:from(kura_test_tenant_note_schema), [item, parent]),
    {ok, [Note]} = as(?ONE, fun() -> kura_test_repo:all(Q) end),
    ?assertEqual(nil, maps:get(item, Note)),
    ?assertEqual(nil, maps:get(parent, Note)).

parallel_preload_keeps_prefix() ->
    {ok, _} = raw("DROP SCHEMA IF EXISTS tenant_p1 CASCADE", []),
    {ok, _} = raw("CREATE SCHEMA tenant_p1", []),
    {ok, _} = raw("CREATE TABLE tenant_p1.tenant_items (LIKE tenant_items INCLUDING ALL)", []),
    {ok, _} = raw("CREATE TABLE tenant_p1.tenant_notes (LIKE tenant_notes INCLUDING ALL)", []),
    PublicId = id_of(~"one-a"),
    {ok, _} = raw(
        "INSERT INTO tenant_p1.tenant_items (id, org_id, name) VALUES ($1, 1, 'p1-item')", [
            PublicId
        ]
    ),
    {ok, _} = raw("INSERT INTO tenant_p1.tenant_notes (org_id, item_id) VALUES (1, $1)", [PublicId]),
    Q = kura_query:preload(kura_query:from(kura_test_tenant_note_schema), [item, parent]),
    {ok, [Note]} = as({prefix, ~"tenant_p1"}, fun() -> kura_test_repo:all(Q) end),
    ?assertMatch(#{name := ~"p1-item"}, maps:get(item, Note)),
    ?assertMatch(#{name := ~"p1-item"}, maps:get(parent, Note)).

prefix_scopes_count_and_stream() ->
    {ok, _} = raw("DROP SCHEMA IF EXISTS tenant_p1 CASCADE", []),
    {ok, _} = raw("CREATE SCHEMA tenant_p1", []),
    {ok, _} = raw("CREATE TABLE tenant_p1.tenant_items (LIKE tenant_items INCLUDING ALL)", []),
    {ok, _} = raw(
        "INSERT INTO tenant_p1.tenant_items (org_id, name, score) VALUES (1, 'p1-only', 7)", []
    ),
    Q = kura_query:from(?ITEMS),
    Self = self(),
    as({prefix, ~"tenant_p1"}, fun() ->
        ?assertEqual({ok, 1}, kura_repo_worker:count(kura_test_repo, Q)),
        ?assertEqual({ok, 7}, kura_repo_worker:aggregate(kura_test_repo, Q, {sum, score})),
        ok = kura_stream:stream(kura_test_repo, Q, fun(Batch) ->
            Self ! {batch, Batch},
            ok
        end)
    end),
    ?assertEqual([~"p1-only"], names(drain_batches([]))).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

as(Tenant, Fun) ->
    kura_tenant:with_tenant(Tenant, Fun).

names(Rows) ->
    lists:sort([maps:get(name, R) || R <- Rows]).

drain_batches(Acc) ->
    receive
        {batch, Batch} -> drain_batches(Acc ++ Batch)
    after 0 -> Acc
    end.

id_of(Name) ->
    {ok, [Row]} = raw("SELECT id FROM tenant_items WHERE name = $1", [Name]),
    maps:get(id, Row).

raw_row(Id) ->
    {ok, [Row]} = raw("SELECT org_id, name FROM tenant_items WHERE id = $1", [Id]),
    {maps:get(org_id, Row), maps:get(name, Row)}.

raw_score(Id) ->
    {ok, [Row]} = raw("SELECT score FROM tenant_items WHERE id = $1", [Id]),
    maps:get(score, Row).

raw(SQL, Params) ->
    kura_test_repo:query(SQL, Params).
