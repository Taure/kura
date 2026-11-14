-module(kura_require_tenant_tests).
-moduledoc "require_tenant: a missing tenant is an error, not an unscoped statement.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([log/2]).

-define(REPO, kura_require_tenant_test_repo).
-define(SCOPED, kura_test_edge_schema).
-define(SHARED, kura_test_shared_schema).
-define(TENANT, {attribute, {qty, 7}}).
-define(REFUSED(Source), {kura, {tenant_required, Source}}).

setting(Setting) ->
    case Setting of
        unset -> application:unset_env(kura, require_tenant);
        _ -> application:set_env(kura, require_tenant, Setting)
    end,
    kura_tenant:reload_settings().

with_setting(Setting, Tests) ->
    {foreach,
        fun() ->
            setting(Setting),
            kura_test_recording_driver:configure(?REPO),
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

changeset(Schema, Data, Params) ->
    kura_changeset:cast(Schema, Data, Params, [name]).

statements() ->
    [SQL || {SQL, _Params} <- kura_test_recording_driver:statements()].

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

required_test_() ->
    with_setting(true, [
        {"a cached statement is not served to a process without a tenant", fun() ->
            setting(false),
            {SQL, []} = kura_query_compiler:to_sql_cached(?REPO, scoped()),
            ?assertEqual({SQL, []}, kura_query_compiler:to_sql_cached(?REPO, scoped())),
            setting(true),
            ?assertError(?REFUSED(?SCOPED), kura_query_compiler:to_sql_cached(?REPO, scoped()))
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
%% Warning
%%----------------------------------------------------------------------

warning_test_() ->
    [
        {"unset: the first real tenant warns once", fun() ->
            ?assertMatch(
                [_],
                warnings_from(unset, fun() ->
                    kura_tenant:put_tenant(undefined),
                    kura_tenant:put_tenant(?TENANT),
                    kura_tenant:with_tenant({prefix, ~"t1"}, fun() -> ok end)
                end)
            )
        end},
        {"unset and no tenant ever set: no warning", fun() ->
            ?assertEqual([], warnings_from(unset, fun() -> kura_tenant:put_tenant(undefined) end))
        end}
    ] ++
        [
            {lists:flatten(io_lib:format("~p: no warning", [Setting])), fun() ->
                ?assertEqual(
                    [], warnings_from(Setting, fun() -> kura_tenant:put_tenant(?TENANT) end)
                )
            end}
         || Setting <- [true, false]
        ].

warnings_from(Setting, Run) ->
    setting(Setting),
    kura_boot_warning:forget(require_tenant),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        _ = Run(),
        warnings()
    after
        logger:remove_handler(?MODULE),
        kura_tenant:clear_tenant(),
        setting(unset),
        kura_boot_warning:forget(require_tenant)
    end.

warnings() ->
    receive
        {logged, #{fix := <<"set {kura, [{require_tenant", _/binary>>} = Report} ->
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
