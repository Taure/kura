-module(kura_require_tenant_tests).
-moduledoc "require_tenant: a missing tenant is an error, not an unscoped statement.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([log/2]).

-define(REPO, kura_require_tenant_test_repo).
-define(SCOPED, kura_test_edge_schema).
-define(SHARED, kura_test_shared_schema).
%% Scoped, as ?SCOPED is, and without its qty field.
-define(POSTS, kura_tenant_test_schema).
%% Says it is scoped, and points at a shared schema.
-define(REFS, kura_test_shared_ref_schema).
-define(TENANT, {attribute, {qty, 7}}).
%% A tenant on a field the shared schema has too.
-define(NAMED, {attribute, {name, ~"acme"}}).
-define(REFUSED(Source), {kura, {tenant_required, Source}}).

setting(Setting) ->
    case Setting of
        unset -> application:unset_env(kura, require_tenant);
        _ -> application:set_env(kura, require_tenant, Setting)
    end.

with_setting(Setting, Tests) ->
    {foreach,
        fun() ->
            setting(Setting),
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:init(),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_tenant:clear_tenant(),
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush(),
            setting(unset)
        end,
        Tests}.

scoped() -> kura_query:from(?SCOPED).
shared() -> kura_query:from(?SHARED).
posts() -> kura_query:from(?POSTS).

changeset(Schema, Data, Params) ->
    kura_changeset:cast(Schema, Data, Params, [name]).

statements() ->
    [SQL || {SQL, _Params} <- kura_test_recording_driver:statements()].

%% A refused miss stores nothing, so the entry a hit needs comes from the
%% same query run where it is allowed.
refused_on_a_miss_and_on_a_hit(Q) ->
    ?assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, Q)),
    ?assertEqual(0, kura_query_cache:entry_count()),
    ?assertEqual([], statements()),
    {ok, []} = kura_tenant:without_tenant(fun() -> kura_repo_worker:all(?REPO, Q) end),
    ?assertEqual(1, kura_query_cache:entry_count()),
    ?assertMatch([_], statements()),
    ?assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, Q)),
    ?assertEqual(1, kura_query_cache:entry_count()),
    ?assertEqual([], statements()).

%%----------------------------------------------------------------------
%% Required
%%----------------------------------------------------------------------

writes_and_reads() ->
    [
        {"all", fun() -> kura_repo_worker:all(?REPO, scoped()) end},
        {"get", fun() -> kura_repo_worker:get(?REPO, ?SCOPED, 1) end},
        {"get_by", fun() -> kura_repo_worker:get_by(?REPO, ?SCOPED, [{name, ~"a"}]) end},
        {"reload", fun() -> kura_repo_worker:reload(?REPO, ?SCOPED, #{id => 1}) end},
        {"insert", fun() ->
            kura_repo_worker:insert(?REPO, changeset(?SCOPED, #{}, #{name => ~"a"}))
        end},
        {"update", fun() ->
            kura_repo_worker:update(?REPO, changeset(?SCOPED, #{id => 1}, #{name => ~"b"}))
        end},
        {"delete", fun() ->
            kura_repo_worker:delete(?REPO, changeset(?SCOPED, #{id => 1}, #{}))
        end},
        {"insert_all", fun() -> kura_repo_worker:insert_all(?REPO, ?SCOPED, [#{name => ~"a"}]) end},
        {"update_all", fun() -> kura_repo_worker:update_all(?REPO, scoped(), #{name => ~"b"}) end},
        {"delete_all", fun() -> kura_repo_worker:delete_all(?REPO, scoped()) end}
    ].

required_refuses_a_missing_tenant_test_() ->
    with_setting(true, [
        {Name, fun() ->
            ?assertError(?REFUSED(?SCOPED), Run()),
            ?assertEqual([], statements())
        end}
     || {Name, Run} <- writes_and_reads()
    ]).

required_runs_with_a_tenant_test_() ->
    with_setting(true, [
        {Name, fun() ->
            _ = kura_tenant:with_tenant(?TENANT, Run),
            [SQL] = statements(),
            ?assertMatch({_, _}, binary:match(SQL, ~"\"qty\""))
        end}
     || {Name, Run} <- writes_and_reads()
    ]).

%% A tenant whose field heads a condition would scope to every tenant's
%% rows and still count as a tenant.
a_tenant_that_scopes_nothing_is_no_tenant_test_() ->
    Bad = kura_test_typing:ill_typed({attribute, {'or', [{qty, 7}, {qty, is_not_nil}]}}),
    with_setting(true, [
        {Name, fun() ->
            ?assertError({invalid_tenant, Bad}, kura_tenant:with_tenant(Bad, Run)),
            _ = erlang:put(kura_tenant, Bad),
            ?assertError({invalid_tenant, Bad}, Run()),
            _ = erlang:erase(kura_tenant),
            ?assertError(?REFUSED(?SCOPED), Run()),
            ?assertEqual([], statements()),
            ?assertEqual(0, kura_query_cache:entry_count())
        end}
     || {Name, Run} <- writes_and_reads()
    ]).

required_test_() ->
    with_setting(true, [
        {"a cached statement is not served to a process without a tenant", fun() ->
            setting(false),
            {SQL, []} = kura_query_compiler:to_sql_cached(?REPO, scoped()),
            ?assertEqual({SQL, []}, kura_query_compiler:to_sql_cached(?REPO, scoped())),
            ?assertEqual(1, kura_query_cache:entry_count()),
            setting(true),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql_cached(?REPO, scoped()))
        end},
        {"a write that names no table is not refused", fun() ->
            ?assertEqual(ok, kura_tenant:require(?REPO, undefined))
        end},
        {"a spawned process has no tenant", fun() ->
            kura_tenant:put_tenant(?TENANT),
            {Pid, Ref} = spawn_monitor(fun() -> kura_repo_worker:all(?REPO, scoped()) end),
            receive
                {'DOWN', Ref, process, Pid, Reason} ->
                    ?assertMatch({?REFUSED(?SCOPED), _Stack}, Reason)
            end
        end},
        {"a preload inside without_tenant runs unscoped in its own processes", fun() ->
            Posts = [#{id => 1, title => ~"a", author_id => 2}],
            Loaded = kura_tenant:without_tenant(fun() ->
                kura_repo_worker:preload(?REPO, kura_test_post_schema, Posts, [author, comments])
            end),
            ?assertMatch([#{id := 1, author := nil, comments := []}], Loaded)
        end},
        {"a table that is not a schema is refused", fun() ->
            ?assertError(
                ?REFUSED(some_table), kura_query_compiler:to_sql(?REPO, kura_query:from(some_table))
            )
        end},
        {"a schema every tenant shares needs none", fun() ->
            {ok, []} = kura_repo_worker:all(?REPO, shared()),
            {ok, _} = kura_repo_worker:insert(?REPO, changeset(?SHARED, #{}, #{name => ~"a"})),
            {ok, 0} = kura_repo_worker:delete_all(?REPO, shared()),
            ?assertEqual(3, length(statements()))
        end},
        {"a shared schema does not cover what the query reads beside it", fun() ->
            Sub = kura_query:select(scoped(), [id]),
            Queries = [
                kura_query:where(shared(), {id, in, {subquery, Sub}}),
                kura_query:where(shared(), {'or', [{id, '=', 1}, {exists, {subquery, Sub}}]}),
                kura_query:where(shared(), {'not', {not_exists, {subquery, Sub}}}),
                (shared())#kura_query{havings = [{id, in, {subquery, Sub}}]},
                kura_query:union(shared(), Sub),
                kura_query:with_cte(shared(), ~"scoped_rows", Sub)
            ],
            [
                ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q))
             || Q <- Queries
            ],
            ?assertEqual([], statements())
        end},
        {"a source that names a CTE is judged by the CTE's body", fun() ->
            Q = kura_query:with_cte(kura_query:from(shared_rows), ~"shared_rows", shared()),
            ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, Q)),
            Scoped = kura_query:with_cte(kura_query:from(scoped_rows), ~"scoped_rows", scoped()),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Scoped))
        end},
        {"a schema whose table a CTE is named after reads the CTE", fun() ->
            Q = kura_query:with_cte(scoped(), ~"edge_items", shared()),
            ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, Q))
        end},
        {"a CTE's body that names the CTE reads the table, on a miss and on a hit", fun() ->
            Q = kura_query:with_cte(kura_query:from(~"edge_items"), ~"edge_items", scoped()),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q)),
            refused_on_a_miss_and_on_a_hit(Q)
        end},
        {"a CTE's body reads the table a later CTE is named after, miss and hit", fun() ->
            Q = kura_query:with_cte(
                kura_query:with_cte(kura_query:from(~"rows"), ~"rows", scoped()),
                ~"edge_items",
                shared()
            ),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q)),
            refused_on_a_miss_and_on_a_hit(Q)
        end},
        {"a CTE's body reads the CTE listed before it", fun() ->
            Q = kura_query:with_cte(
                kura_query:with_cte(kura_query:from(~"rows"), ~"edge_items", shared()),
                ~"rows",
                scoped()
            ),
            ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, Q))
        end},
        {"a CTE named after a schema's module does not stand in for its table", fun() ->
            Q = kura_query:with_cte(scoped(), atom_to_binary(?SCOPED), shared()),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q))
        end},
        {"a prefix reads the table, whatever a CTE is named", fun() ->
            Q = kura_query:with_cte(scoped(), ~"edge_items", shared()),
            ?assertError(
                ?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, kura_query:prefix(Q, ~"p"))
            ),
            Joined = kura_query:join(
                kura_query:with_cte(shared(), ~"edge_items", shared()),
                inner,
                ~"edge_items",
                {id, id}
            ),
            ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, Joined)),
            ?assertError(
                ?REFUSED(~"edge_items"),
                kura_query_compiler:to_sql(?REPO, kura_query:prefix(Joined, ~"p"))
            )
        end}
    ]).

%%----------------------------------------------------------------------
%% A tenant field the schema does not declare
%%----------------------------------------------------------------------

undeclared() -> {attribute, {no_such_field, ~"tenant-9f3"}}.

named(Names) ->
    [Run || {Name, _} = Run <- writes_and_reads(), lists:member(Name, Names)].

compiled_queries() -> named(["all", "get", "get_by", "reload", "update_all", "delete_all"]).
keyed_writes() -> named(["update", "delete"]).
inserts() -> named(["insert", "insert_all"]).

an_undeclared_tenant_field_is_left_to_the_database_test_() ->
    [
        with_setting(Setting, [
            {Name, fun() ->
                _ = kura_tenant:with_tenant(undeclared(), Run),
                _ = kura_tenant:with_tenant(undeclared(), Run),
                Statements = kura_test_recording_driver:statements(),
                ?assertMatch([_, _], Statements),
                [
                    begin
                        ?assertMatch({_, _}, binary:match(SQL, ~"\"no_such_field\" = $")),
                        ?assertEqual(~"tenant-9f3", lists:last(Params))
                    end
                 || {SQL, Params} <- Statements
                ]
            end}
         || {Name, Run} <- compiled_queries() ++ keyed_writes()
        ])
     || Setting <- [unset, false]
    ].

an_undeclared_tenant_field_scopes_the_statement_test_() ->
    Tenant = undeclared(),
    with_setting(unset, [
        {"all", fun() ->
            {ok, []} = kura_tenant:with_tenant(Tenant, {kura_repo_worker, all, [?REPO, scoped()]}),
            ?assertEqual(
                [{~"SELECT * FROM \"edge_items\" WHERE \"no_such_field\" = $1", [~"tenant-9f3"]}],
                kura_test_recording_driver:statements()
            )
        end},
        {"update_all", fun() ->
            {ok, 0} = kura_tenant:with_tenant(
                Tenant, {kura_repo_worker, update_all, [?REPO, scoped(), #{name => ~"b"}]}
            ),
            ?assertEqual(
                [
                    {
                        ~"UPDATE \"edge_items\" SET \"name\" = $1 WHERE \"no_such_field\" = $2",
                        [~"b", ~"tenant-9f3"]
                    }
                ],
                kura_test_recording_driver:statements()
            )
        end},
        {"delete_all", fun() ->
            {ok, 0} = kura_tenant:with_tenant(
                Tenant, {kura_repo_worker, delete_all, [?REPO, scoped()]}
            ),
            ?assertEqual(
                [{~"DELETE FROM \"edge_items\" WHERE \"no_such_field\" = $1", [~"tenant-9f3"]}],
                kura_test_recording_driver:statements()
            )
        end}
    ]).

%% The row is written without the tenant: a field the schema does not have
%% is not a column kura can write.
an_insert_leaves_out_an_undeclared_tenant_field_test_() ->
    [
        with_setting(Setting, [
            {Name, fun() ->
                _ = kura_tenant:with_tenant(undeclared(), Run),
                [SQL] = statements(),
                ?assertEqual(nomatch, binary:match(SQL, ~"no_such_field"))
            end}
         || {Name, Run} <- inserts()
        ])
     || Setting <- [unset, false, true]
    ].

an_undeclared_tenant_field_is_refused_where_a_tenant_is_required_test_() ->
    Tenant = undeclared(),
    Refused = fun(Run) ->
        ?assertError({invalid_tenant, Tenant}, kura_tenant:with_tenant(Tenant, Run)),
        ?assertError({invalid_tenant, Tenant}, kura_tenant:with_tenant(Tenant, Run)),
        ?assertEqual([], statements())
    end,
    [
        with_setting(true, [{Name, fun() -> Refused(Run) end} || {Name, Run} <- compiled_queries()]),
        with_repo_setting(unset, #{require_tenant => true}, [
            {Name, fun() -> Refused(Run) end}
         || {Name, Run} <- compiled_queries()
        ]),
        with_repo_setting(true, #{require_tenant => false}, [
            {"a repo that says false is not refused", fun() ->
                {ok, []} = kura_tenant:with_tenant(
                    Tenant, {kura_repo_worker, all, [?REPO, scoped()]}
                ),
                ?assertMatch([_], statements())
            end}
        ]),
        with_setting(true, [
            {"a branch of a union", fun() ->
                Q = kura_query:union(scoped(), posts()),
                ?assertError(
                    {invalid_tenant, ?TENANT},
                    kura_tenant:with_tenant(?TENANT, {kura_repo_worker, all, [?REPO, Q]})
                ),
                ?assertEqual([], statements())
            end},
            {"a field the schema declares is not", fun() ->
                {ok, []} = kura_tenant:with_tenant(
                    ?TENANT, {kura_repo_worker, all, [?REPO, scoped()]}
                ),
                ?assertMatch([_], statements())
            end},
            {"a join or a CTE can bring the column in", fun() ->
                Queries = [
                    joined(scoped(), ?SHARED),
                    kura_query:with_cte(scoped(), ~"rows", kura_query:from(~"other"))
                ],
                [
                    ?assertMatch(
                        {ok, []},
                        kura_tenant:with_tenant(undeclared(), {kura_repo_worker, all, [?REPO, Q]})
                    )
                 || Q <- Queries
                ],
                ?assertMatch([_, _], statements())
            end},
            {"no warning is logged for what is refused", fun() ->
                ?assertEqual(
                    [],
                    field_warnings_by(fun() ->
                        ?assertError(
                            {invalid_tenant, Tenant},
                            kura_tenant:with_tenant(
                                Tenant, {kura_repo_worker, all, [?REPO, scoped()]}
                            )
                        )
                    end)
                )
            end}
        ])
    ].

an_undeclared_tenant_field_warns_once_for_each_schema_test_() ->
    All = fun(Tenant, Q) ->
        kura_tenant:with_tenant(Tenant, {kura_repo_worker, all, [?REPO, Q]})
    end,
    [
        with_setting(Setting, [
            {"repeated statements warn once, naming the schema and the field", fun() ->
                [Report] = field_warnings_by(fun() ->
                    [
                        _ = kura_tenant:with_tenant(undeclared(), Run)
                     || {_, Run} <- compiled_queries()
                    ],
                    [
                        _ = kura_tenant:with_tenant(undeclared(), Run)
                     || {_, Run} <- compiled_queries()
                    ],
                    All({attribute, {no_such_field, 8}}, scoped()),
                    All({attribute, {another_field, 8}}, scoped())
                end),
                ?assertMatch(#{schema := ?SCOPED, field := no_such_field}, Report),
                ?assertEqual([field, fix, msg, schema], lists:sort(maps:keys(Report))),
                Text = iolist_to_binary(io_lib:format("~0p", [Report])),
                ?assertEqual(nomatch, binary:match(Text, ~"9f3"))
            end},
            {"a statement compiled without the cache warns too", fun() ->
                ?assertMatch(
                    [#{schema := ?SCOPED, field := no_such_field}],
                    field_warnings_by(fun() ->
                        kura_tenant:put_tenant(undeclared()),
                        _ = kura_query_compiler:to_sql(?REPO, scoped()),
                        _ = kura_query_compiler:to_sql(?REPO, scoped())
                    end)
                )
            end},
            {"each schema has its own warning", fun() ->
                ?assertMatch(
                    [
                        #{schema := ?POSTS, field := qty},
                        #{schema := ?SCOPED, field := no_such_field}
                    ],
                    field_warnings_by(fun() ->
                        All(?TENANT, posts()),
                        All(?TENANT, posts()),
                        All(undeclared(), scoped()),
                        All(?TENANT, posts())
                    end)
                )
            end},
            {"a branch of a union is named", fun() ->
                ?assertMatch(
                    [#{schema := ?POSTS, field := qty}],
                    field_warnings_by(fun() ->
                        All(?TENANT, kura_query:union(scoped(), posts()))
                    end)
                )
            end},
            {"a field the schema declares warns of nothing", fun() ->
                ?assertEqual([], field_warnings_by(fun() -> All(?TENANT, scoped()) end))
            end},
            {"a table that is no schema warns of nothing", fun() ->
                ?assertEqual(
                    [],
                    field_warnings_by(fun() ->
                        All(undeclared(), kura_query:from(~"edge_items")),
                        All(undeclared(), kura_query:from(edge_items))
                    end)
                )
            end}
        ])
     || Setting <- [unset, false]
    ].

forget_field_warnings() ->
    [
        ok = kura_boot_warning:forget({undeclared_tenant_field, S})
     || S <- [?SCOPED, ?SHARED, ?POSTS]
    ],
    ok.

field_warnings_by(Run) ->
    forget_field_warnings(),
    _ = logger:remove_handler(?MODULE),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        _ = Run(),
        field_warnings()
    after
        logger:remove_handler(?MODULE),
        forget_field_warnings()
    end.

field_warnings() ->
    receive
        {logged, #{schema := _} = Report} -> [Report | field_warnings()];
        {logged, _} -> field_warnings()
    after 100 -> []
    end.

%%----------------------------------------------------------------------
%% A schema every tenant shares, read under a tenant
%%----------------------------------------------------------------------

sql(Run) ->
    _ = kura_tenant:with_tenant(?TENANT, Run),
    _ = kura_tenant:with_tenant(?TENANT, Run),
    [Statement, Statement] = kura_test_recording_driver:statements(),
    Statement.

a_shared_schema_is_not_scoped_test_() ->
    [
        with_setting(Setting, [
            {"all", fun() ->
                ?assertEqual(
                    {~"SELECT * FROM \"shared_items\"", []},
                    sql({kura_repo_worker, all, [?REPO, shared()]})
                )
            end},
            {"get, get_by and reload", fun() ->
                ById = {~"SELECT * FROM \"shared_items\" WHERE \"id\" = $1", [1]},
                ?assertEqual(ById, sql({kura_repo_worker, get, [?REPO, ?SHARED, 1]})),
                ?assertEqual(
                    ById, sql({kura_repo_worker, reload, [?REPO, ?SHARED, #{id => 1}]})
                ),
                ?assertEqual(
                    {~"SELECT * FROM \"shared_items\" WHERE \"name\" = $1", [~"a"]},
                    sql({kura_repo_worker, get_by, [?REPO, ?SHARED, [{name, ~"a"}]]})
                )
            end},
            {"exists and an aggregate", fun() ->
                {Exists, [1]} = sql({kura_repo_worker, exists, [?REPO, shared()]}),
                {Count, []} = sql({kura_repo_worker, aggregate, [?REPO, shared(), count]}),
                [?assertEqual(nomatch, binary:match(SQL, ~"qty")) || SQL <- [Exists, Count]]
            end},
            {"update_all", fun() ->
                ?assertEqual(
                    {~"UPDATE \"shared_items\" SET \"name\" = $1", [~"b"]},
                    sql({kura_repo_worker, update_all, [?REPO, shared(), #{name => ~"b"}]})
                )
            end},
            {"delete_all", fun() ->
                ?assertEqual(
                    {~"DELETE FROM \"shared_items\"", []},
                    sql({kura_repo_worker, delete_all, [?REPO, shared()]})
                )
            end},
            {"a preload of a shared schema", fun() ->
                Refs = [#{id => 1, shared_id => 5, qty => 7}],
                ?assertEqual(
                    {~"SELECT * FROM \"shared_items\" WHERE \"id\" IN ($1)", [5]},
                    sql({kura_repo_worker, preload, [?REPO, ?REFS, Refs, [shared]]})
                )
            end},
            {"a join of shared schemas", fun() ->
                {SQL, []} = sql({kura_repo_worker, all, [?REPO, joined(shared(), ?SHARED)]}),
                ?assertEqual(nomatch, binary:match(SQL, ~"qty"))
            end},
            {"a shared branch of a union, a shared subquery and a shared CTE body", fun() ->
                Ids = kura_query:select(shared(), [id]),
                ?assertEqual(
                    {
                        <<
                            "SELECT * FROM \"edge_items\" WHERE \"qty\" = $1"
                            " UNION SELECT * FROM \"shared_items\""
                        >>,
                        [7]
                    },
                    sql({kura_repo_worker, all, [?REPO, kura_query:union(scoped(), shared())]})
                ),
                ?assertEqual(
                    {
                        <<
                            "SELECT * FROM \"edge_items\" WHERE \"id\" IN"
                            " (SELECT \"id\" FROM \"shared_items\") AND \"qty\" = $1"
                        >>,
                        [7]
                    },
                    sql(
                        {kura_repo_worker, all, [
                            ?REPO, kura_query:where(scoped(), {id, in, {subquery, Ids}})
                        ]}
                    )
                ),
                ?assertEqual(
                    {
                        <<
                            "WITH \"picked\" AS (SELECT \"id\" FROM \"shared_items\")"
                            " SELECT * FROM \"edge_items\" WHERE \"qty\" = $1"
                        >>,
                        [7]
                    },
                    sql(
                        {kura_repo_worker, all, [
                            ?REPO, kura_query:with_cte(scoped(), ~"picked", Ids)
                        ]}
                    )
                )
            end},
            {"a field of its own that the tenant names is not compared", fun() ->
                {ok, []} = kura_tenant:with_tenant(
                    ?NAMED, {kura_repo_worker, all, [?REPO, shared()]}
                ),
                ?assertEqual(
                    [{~"SELECT * FROM \"shared_items\"", []}],
                    kura_test_recording_driver:statements()
                )
            end},
            {"it is neither warned of nor refused for the field it lacks", fun() ->
                ?assertEqual(
                    [],
                    field_warnings_by(fun() ->
                        {ok, []} = kura_tenant:with_tenant(
                            ?TENANT, {kura_repo_worker, all, [?REPO, shared()]}
                        ),
                        _ = kura_tenant:with_tenant(
                            ?TENANT, {kura_query_compiler, to_sql, [?REPO, shared()]}
                        )
                    end)
                )
            end}
        ])
     || Setting <- [unset, false, true]
    ].

%% Only queries leave the tenant out. A write of one record by its key is
%% matched on the tenant as any other; run it in without_tenant/1.
a_changeset_write_of_a_shared_schema_is_matched_on_the_tenant_test_() ->
    with_setting(true, [
        {"update", fun() ->
            _ = kura_tenant:with_tenant(
                ?TENANT,
                {kura_repo_worker, update, [
                    ?REPO, changeset(?SHARED, #{id => 1, name => ~"a"}, #{name => ~"b"})
                ]}
            ),
            ?assertMatch(
                [
                    {
                        <<"UPDATE \"shared_items\" SET \"name\" = $1 WHERE \"id\" = $2 AND \"qty\" = $3",
                            _/binary>>,
                        [~"b", 1, 7]
                    }
                ],
                kura_test_recording_driver:statements()
            )
        end},
        {"delete", fun() ->
            _ = kura_tenant:with_tenant(
                ?TENANT, {kura_repo_worker, delete, [?REPO, changeset(?SHARED, #{id => 1}, #{})]}
            ),
            ?assertMatch(
                [
                    {
                        <<"DELETE FROM \"shared_items\" WHERE \"id\" = $1 AND \"qty\" = $2",
                            _/binary>>,
                        [1, 7]
                    }
                ],
                kura_test_recording_driver:statements()
            )
        end},
        {"neither inside without_tenant/1", fun() ->
            Delete = {kura_repo_worker, delete, [?REPO, changeset(?SHARED, #{id => 1}, #{})]},
            _ = kura_tenant:with_tenant(?TENANT, {kura_tenant, without_tenant, [Delete]}),
            ?assertMatch(
                [{<<"DELETE FROM \"shared_items\" WHERE \"id\" = $1", Rest/binary>>, [1]}] when
                    binary_part(Rest, 0, min(4, byte_size(Rest))) =/= ~" AND",
                kura_test_recording_driver:statements()
            )
        end}
    ]).

%% The condition names the tenant table's column, wherever that table
%% stands in the join.
a_shared_schema_beside_a_tenants_table_keeps_the_condition_test_() ->
    Condition = ~" WHERE \"qty\" = $1",
    Scoped = fun(Q) ->
        {SQL, [7]} = sql({kura_repo_worker, all, [?REPO, Q]}),
        ?assertEqual(Condition, binary:part(SQL, byte_size(SQL), -byte_size(Condition)))
    end,
    with_setting(true, [
        {"a tenant's table joined to a shared schema", fun() ->
            Scoped(joined(scoped(), ?SHARED))
        end},
        {"a shared schema joined to a tenant's table", fun() ->
            Scoped(joined(shared(), ?SCOPED))
        end},
        {"a shared schema joined to a shared schema and to a tenant's table", fun() ->
            Scoped(kura_query:join(joined(shared(), ?SHARED), left, ?SCOPED, {id, id}))
        end},
        {"a shared schema joined to a table that is no schema", fun() ->
            Scoped(joined(shared(), ~"audit_rows"))
        end}
    ]).

only_a_schema_that_says_false_is_shared_test_() ->
    Scoped = fun(Q) ->
        {SQL, Params} = sql({kura_repo_worker, all, [?REPO, Q]}),
        ?assertMatch({_, _}, binary:match(SQL, ~"\"qty\" = $")),
        ?assertEqual(7, lists:last(Params))
    end,
    with_setting(true, [
        {"a schema without the callback", fun() -> Scoped(scoped()) end},
        {"a schema whose callback says true", fun() -> Scoped(kura_query:from(?REFS)) end},
        {"a table named by a binary", fun() -> Scoped(kura_query:from(~"shared_items")) end},
        {"a table named by an atom", fun() -> Scoped(kura_query:from(shared_items)) end},
        {"a CTE named as the shared schema's table is", fun() ->
            Scoped(
                kura_query:with_cte(kura_query:from(~"shared_items"), ~"shared_items", scoped())
            )
        end}
    ]).

%%----------------------------------------------------------------------
%% Joins
%%----------------------------------------------------------------------

joined(From, Joined) ->
    kura_query:select(kura_query:join(From, inner, Joined, {id, id}), [id]).

joined_test_() ->
    Join = joined(shared(), ?SCOPED),
    with_setting(true, [
        {"a shared schema joined to a scoped one is refused, however it is run", fun() ->
            Runs = [
                fun() -> kura_repo_worker:all(?REPO, Join) end,
                fun() -> kura_repo_worker:one(?REPO, Join) end,
                fun() -> kura_repo_worker:exists(?REPO, Join) end,
                fun() -> kura_query_compiler:to_sql(?REPO, Join) end,
                fun() -> kura_query_compiler:to_sql_cached(?REPO, Join) end,
                fun() -> kura_query_compiler:to_sql_from(?REPO, Join, 1) end
            ],
            [?assertError(?REFUSED(?SCOPED), Run()) || Run <- Runs],
            ?assertEqual([], statements())
        end},
        {"every join is read, not only the first", fun() ->
            Q = kura_query:join(joined(shared(), ?SHARED), left, ?SCOPED, {id, id}),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q))
        end},
        {"a joined table that is not a schema is refused", fun() ->
            ?assertError(
                ?REFUSED(~"audit_rows"),
                kura_query_compiler:to_sql(?REPO, joined(shared(), ~"audit_rows"))
            )
        end},
        {"a join inside a nested query is refused", fun() ->
            Queries = [
                kura_query:where(shared(), {id, in, {subquery, Join}}),
                kura_query:where(shared(), {'not', {exists, {subquery, Join}}}),
                (shared())#kura_query{havings = [{not_exists, {subquery, Join}}]},
                kura_query:union_all(shared(), Join),
                kura_query:with_cte(shared(), ~"joined_rows", Join),
                kura_query:where(
                    shared(),
                    {id, in, {subquery, kura_query:with_cte(shared(), ~"joined_rows", Join)}}
                )
            ],
            [
                ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Q))
             || Q <- Queries
            ],
            ?assertEqual([], statements())
        end},
        {"a cached join is not served to a process without a tenant", fun() ->
            {SQL, []} = kura_tenant:without_tenant(fun() ->
                kura_query_compiler:to_sql_cached(?REPO, Join)
            end),
            ?assertEqual(
                {SQL, []},
                kura_tenant:without_tenant(fun() ->
                    kura_query_compiler:to_sql_cached(?REPO, Join)
                end)
            ),
            ?assertEqual(1, kura_query_cache:entry_count()),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql_cached(?REPO, Join))
        end},
        {"shared schemas joined to each other need no tenant", fun() ->
            {ok, []} = kura_repo_worker:all(?REPO, joined(shared(), ?SHARED)),
            ?assertMatch([_], statements())
        end},
        {"a join that names a CTE is judged by the CTE's body", fun() ->
            Shared = kura_query:with_cte(joined(shared(), ~"rows"), ~"rows", shared()),
            ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, Shared)),
            Scoped = kura_query:with_cte(joined(shared(), ~"rows"), ~"rows", scoped()),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, Scoped))
        end},
        {"a join runs with a tenant, and inside without_tenant", fun() ->
            _ = kura_tenant:with_tenant(?TENANT, fun() -> kura_repo_worker:all(?REPO, Join) end),
            _ = kura_tenant:without_tenant(fun() -> kura_repo_worker:all(?REPO, Join) end),
            [Scoped, Unscoped] = statements(),
            ?assertMatch({_, _}, binary:match(Scoped, ~"\"qty\"")),
            ?assertEqual(nomatch, binary:match(Unscoped, ~"\"qty\""))
        end}
    ]).

joined_not_required_test_() ->
    [
        with_setting(Setting, [
            {"a join runs unscoped", fun() ->
                {ok, []} = kura_repo_worker:all(?REPO, joined(shared(), ?SCOPED)),
                ?assertMatch([_], statements())
            end}
        ])
     || Setting <- [unset, false]
    ].

%%----------------------------------------------------------------------
%% without_tenant/1
%%----------------------------------------------------------------------

without_tenant_test_() ->
    with_setting(true, [
        {"runs unscoped and returns the result", fun() ->
            ?assertEqual(
                {ok, []},
                kura_tenant:without_tenant(fun() -> kura_repo_worker:all(?REPO, scoped()) end)
            ),
            ?assertEqual([~"SELECT * FROM \"edge_items\""], statements())
        end},
        {"covers writes", fun() ->
            _ = kura_tenant:without_tenant(fun() ->
                kura_repo_worker:insert(?REPO, changeset(?SCOPED, #{}, #{name => ~"a"}))
            end),
            ?assertEqual(
                [~"INSERT INTO \"edge_items\" (\"name\") VALUES ($1) RETURNING *"], statements()
            )
        end},
        {"drops the process's tenant inside and restores it after", fun() ->
            kura_tenant:put_tenant(?TENANT),
            ?assertEqual(undefined, kura_tenant:without_tenant(fun kura_tenant:get_tenant/0)),
            ?assertEqual(?TENANT, kura_tenant:get_tenant())
        end},
        {"the requirement is back afterwards", fun() ->
            _ = kura_tenant:without_tenant(fun() -> ok end),
            ?assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, scoped()))
        end},
        {"and after a crash inside", fun() ->
            ?assertError(boom, kura_tenant:without_tenant(fun() -> error(boom) end)),
            ?assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, scoped()))
        end},
        {"nests", fun() ->
            kura_tenant:without_tenant(fun() ->
                _ = kura_tenant:without_tenant(fun() -> ok end),
                {ok, []} = kura_repo_worker:all(?REPO, scoped())
            end),
            ?assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, scoped()))
        end},
        {"a tenant set inside scopes as usual", fun() ->
            kura_tenant:without_tenant(fun() ->
                kura_tenant:with_tenant(?TENANT, fun() -> kura_repo_worker:all(?REPO, scoped()) end),
                kura_repo_worker:all(?REPO, scoped())
            end),
            ?assertEqual(
                [
                    ~"SELECT * FROM \"edge_items\" WHERE \"qty\" = $1",
                    ~"SELECT * FROM \"edge_items\""
                ],
                statements()
            )
        end}
    ]).

%%----------------------------------------------------------------------
%% Not required
%%----------------------------------------------------------------------

not_required_runs_unscoped_test_() ->
    [
        with_setting(Setting, [
            {Name, fun() ->
                _ = Run(),
                ?assertMatch([_], statements())
            end}
         || {Name, Run} <- writes_and_reads()
        ])
     || Setting <- [unset, false]
    ].

invalid_setting_test_() ->
    with_setting(strict, [
        ?_assertError(
            {kura, {invalid_setting, require_tenant, strict}},
            kura_repo_worker:all(?REPO, scoped())
        )
    ]).

%%----------------------------------------------------------------------
%% Per repo
%%----------------------------------------------------------------------

with_repo_setting(Env, Repo, Tests) ->
    {foreach,
        fun() ->
            setting(Env),
            kura_test_recording_driver:configure(?REPO, Repo),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_tenant:clear_tenant(),
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush(),
            setting(unset)
        end,
        Tests}.

refused() ->
    [
        ?_assertError(?REFUSED(?SCOPED), kura_repo_worker:all(?REPO, scoped())),
        ?_assertError(
            ?REFUSED(?SCOPED),
            kura_repo_worker:insert(?REPO, changeset(?SCOPED, #{}, #{name => ~"a"}))
        ),
        ?_assertError(
            ?REFUSED(?SCOPED), kura_repo_worker:insert_all(?REPO, ?SCOPED, [#{name => ~"a"}])
        ),
        ?_assertError(
            ?REFUSED(?SCOPED),
            kura_repo_worker:delete(?REPO, changeset(?SCOPED, #{id => 1}, #{}))
        )
    ].

runs() ->
    [
        ?_assertEqual({ok, []}, kura_repo_worker:all(?REPO, scoped())),
        ?_assertMatch(
            {ok, _}, kura_repo_worker:insert(?REPO, changeset(?SCOPED, #{}, #{name => ~"a"}))
        )
    ].

per_repo_test_() ->
    [
        {"a repo requires a tenant on its own",
            with_repo_setting(unset, #{require_tenant => true}, refused())},
        {"a repo's own setting wins over the application environment's", [
            with_repo_setting(true, #{require_tenant => false}, runs()),
            with_repo_setting(false, #{require_tenant => true}, refused())
        ]},
        {"the application environment stands for a repo that sets none", [
            with_repo_setting(true, #{}, refused()),
            with_repo_setting(false, #{}, runs())
        ]},
        {"a repo's setting that is not a boolean is named",
            with_repo_setting(true, #{require_tenant => ~"yes"}, [
                ?_assertError(
                    {kura, {invalid_setting, require_tenant, ~"yes"}},
                    kura_repo_worker:all(?REPO, scoped())
                )
            ])},
        {"an empty insert_all writes nothing and needs no tenant",
            with_repo_setting(unset, #{require_tenant => true}, [
                ?_assertEqual({ok, 0}, kura_repo_worker:insert_all(?REPO, ?SCOPED, []))
            ])}
    ].

%% A repo whose pool kura started keeps the setting it was started with,
%% the application environment's included.
started_repo_test_() ->
    Config = #{dialect => kura_dialect_pg, pool_module => kura_pool_minato},
    Started = fun(Env, Repo) ->
        setting(Env),
        kura_repo_settings:started(?REPO, maps:merge(Config, Repo))
    end,
    {foreach, fun() -> ok end,
        fun(_) ->
            kura_repo_settings:forget(?REPO),
            kura_query_cache:flush(),
            setting(unset)
        end,
        [
            {"its own setting", fun() ->
                Started(unset, #{require_tenant => true}),
                ?assertMatch(#{require_tenant := true}, kura_repo:settings(?REPO)),
                ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, scoped()))
            end},
            {"the application environment's, as it was at the start", fun() ->
                Started(true, #{}),
                setting(false),
                ?assertMatch(#{require_tenant := true}, kura_repo:settings(?REPO)),
                ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql(?REPO, scoped())),
                Started(false, #{}),
                ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, scoped()))
            end},
            {"neither: nothing is required, whatever is set afterwards", fun() ->
                Started(unset, #{}),
                setting(true),
                ?assertNot(is_map_key(require_tenant, kura_repo:settings(?REPO))),
                ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, scoped()))
            end},
            {"its own setting wins", fun() ->
                Started(true, #{require_tenant => false}),
                ?assertMatch({_, []}, kura_query_compiler:to_sql(?REPO, scoped()))
            end}
        ]}.

%%----------------------------------------------------------------------
%% Warning
%%----------------------------------------------------------------------

warning_test_() ->
    Query = fun() -> kura_repo_worker:all(?REPO, shared()) end,
    Write = fun() ->
        kura_repo_worker:insert(?REPO, changeset(?SHARED, #{}, #{name => ~"a"}))
    end,
    [
        {"unset: the first query under a tenant warns once, naming the repo", fun() ->
            ?assertMatch(
                [#{repo := ?REPO}],
                warnings_from(unset, #{}, fun() ->
                    kura_tenant:put_tenant(?NAMED),
                    Query(),
                    Query(),
                    kura_tenant:with_tenant({prefix, ~"t1"}, Query)
                end)
            )
        end},
        {"unset: so does the first write", fun() ->
            ?assertMatch(
                [#{repo := ?REPO}],
                warnings_from(unset, #{}, fun() ->
                    kura_tenant:with_tenant({prefix, ~"t1"}, Write),
                    kura_tenant:with_tenant({prefix, ~"t1"}, Write)
                end)
            )
        end},
        {"unset: so does a statement compiled without the cache", fun() ->
            ?assertMatch(
                [#{repo := ?REPO}],
                warnings_from(unset, #{}, fun() ->
                    kura_tenant:put_tenant(?NAMED),
                    kura_query_compiler:to_sql(?REPO, shared())
                end)
            )
        end},
        {"unset: so does a statement served from the cache", fun() ->
            ?assertMatch(
                [#{repo := ?REPO}],
                warnings_from(unset, #{}, fun() ->
                    kura_query_cache:init(),
                    kura_query_cache:flush(),
                    kura_tenant:put_tenant(?NAMED),
                    Compiled = kura_query_compiler:to_sql_cached(?REPO, shared()),
                    ?assertEqual(1, kura_query_cache:entry_count()),
                    ?assertMatch([_], warnings()),
                    kura_boot_warning:forget(require_tenant),
                    ?assertEqual(Compiled, kura_query_compiler:to_sql_cached(?REPO, shared())),
                    ?assertEqual(1, kura_query_cache:entry_count())
                end)
            )
        end},
        {"unset and no tenant ever set: no warning", fun() ->
            ?assertEqual(
                [],
                warnings_from(unset, #{}, fun() ->
                    kura_tenant:put_tenant(undefined),
                    Query(),
                    Write()
                end)
            )
        end},
        {"setting a tenant runs nothing, so warns of nothing", fun() ->
            ?assertEqual(
                [], warnings_from(unset, #{}, fun() -> kura_tenant:put_tenant(?NAMED) end)
            )
        end}
    ] ++
        [
            {lists:flatten(io_lib:format("~p in ~p: no warning", [Setting, Where])), fun() ->
                {Env, Repo} =
                    case Where of
                        env -> {Setting, #{}};
                        repo -> {unset, #{require_tenant => Setting}}
                    end,
                ?assertEqual(
                    [],
                    warnings_from(Env, Repo, fun() ->
                        kura_tenant:with_tenant(?NAMED, Query),
                        kura_tenant:with_tenant(?NAMED, Write)
                    end)
                )
            end}
         || Setting <- [true, false], Where <- [env, repo]
        ].

warnings_from(Setting, Repo, Run) ->
    setting(Setting),
    kura_test_recording_driver:configure(?REPO, Repo),
    kura_boot_warning:forget(require_tenant),
    _ = logger:remove_handler(?MODULE),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        _ = Run(),
        warnings()
    after
        logger:remove_handler(?MODULE),
        kura_tenant:clear_tenant(),
        kura_test_recording_driver:unconfigure(),
        kura_query_cache:flush(),
        setting(unset),
        kura_boot_warning:forget(require_tenant)
    end.

warnings() ->
    receive
        {logged, #{fix := <<"set require_tenant", _/binary>>} = Report} ->
            [Report | warnings()];
        {logged, _} ->
            warnings()
    after 100 -> []
    end.

log(#{msg := {report, Report}}, #{config := Pid}) ->
    Pid ! {logged, Report},
    ok;
log(_Event, _Config) ->
    ok.
