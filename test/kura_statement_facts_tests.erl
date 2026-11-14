-module(kura_statement_facts_tests).
-moduledoc """
What the compiler works out once for a query shape: a cache hit runs under
the same tenant requirement, redaction and telemetry source as a miss, and
does not walk the query to get there.
""".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([forward/4]).

-define(REPO, kura_statement_facts_test_repo).
-define(SECRET, kura_test_secret_schema).
-define(SCOPED, kura_test_edge_schema).
-define(SHARED, kura_test_shared_schema).
-define(R, ~"[redacted]").
-define(WALKS, [
    {kura_query, sources, 1},
    {kura_query, nested, 1},
    {kura_redact, mode, 1},
    {kura_tenant, scoped_sources, 1},
    {kura_db, extract_source, 1}
]).

facts_test_() ->
    {foreach,
        fun() ->
            {ok, _} = application:ensure_all_started(telemetry),
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:init(),
            kura_query_cache:flush()
        end,
        fun(_) ->
            application:unset_env(kura, require_tenant),
            kura_tenant:clear_tenant(),
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush()
        end,
        [
            {"the facts of a shape", fun t_facts/0},
            {"a hit hands back the facts the miss stored", fun t_hit_has_the_facts/0},
            {"a hit walks nothing", fun t_hit_walks_nothing/0},
            {"a hit is redacted as the miss was", fun t_hit_is_redacted/0},
            {"a hit is refused a missing tenant as the miss was", fun t_hit_requires_a_tenant/0},
            {"a query that cannot be split has its facts too", fun t_malformed/0},
            {"telemetry names the table kura compiled for", fun t_source/0},
            {"raw SQL is read for its table", fun t_raw_source/0}
        ]}.

t_facts() ->
    Plain = kura_query:from(?SHARED),
    ?assertEqual(
        #{shown => plain, source => ~"shared_items", tenant_scoped => []},
        kura_query_compiler:facts(Plain)
    ),
    ?assertEqual(
        #{shown => redacted, source => ~"accounts", tenant_scoped => [?SECRET]},
        kura_query_compiler:facts(kura_query:from(?SECRET))
    ),
    ?assertEqual(
        #{shown => hidden, source => ~"accounts", tenant_scoped => [?SECRET, ?SCOPED]},
        kura_query_compiler:facts(
            kura_query:where(
                kura_query:join(kura_query:from(?SECRET), inner, ?SCOPED, {id, id}),
                {token, '=', ~"t"}
            )
        )
    ),
    ?assertEqual(
        #{shown => plain, source => ~"audit_rows", tenant_scoped => [~"audit_rows"]},
        kura_query_compiler:facts(kura_query:from(~"audit_rows"))
    ),
    ?assertEqual(
        #{shown => plain, source => undefined, tenant_scoped => []},
        kura_query_compiler:facts(#kura_query{})
    ).

t_hit_has_the_facts() ->
    Q = fun(Token) -> kura_query:where(kura_query:from(?SECRET), {token, '=', Token}) end,
    {SQL, [~"a"], Facts} = kura_query_compiler:statement(?REPO, Q(~"a")),
    ?assertEqual(kura_query_compiler:facts(Q(~"a")), Facts),
    ?assertEqual({SQL, [~"b"], Facts}, kura_query_compiler:statement(?REPO, Q(~"b"))),
    ?assertEqual({SQL, [~"c"]}, kura_query_compiler:to_sql_cached(?REPO, Q(~"c"))).

t_hit_walks_nothing() ->
    application:set_env(kura, require_tenant, true),
    Q = fun(Id) ->
        kura_query:where(
            kura_query:select(
                kura_query:join(kura_query:from(?SHARED), inner, ?SHARED, {id, id}), [name]
            ),
            {'or', [{name, '=', Id}, {exists, {subquery, kura_query:from(?SHARED)}}]}
        )
    end,
    Tenant = {prefix, ~"t1"},
    {ok, []} = kura_repo_worker:all(?REPO, Q(1)),
    {ok, []} = kura_tenant:with_tenant(Tenant, fun() -> kura_repo_worker:all(?REPO, Q(2)) end),
    ?assertEqual(2, kura_query_cache:entry_count()),
    ?assertMatch({_, [1], _}, kura_query_compiler:statement(?REPO, Q(1))),
    Counted = [MFA || MFA <- ?WALKS, erlang:trace_pattern(MFA, true, [call_count]) =:= 1],
    Calls = fun() ->
        [Count || MFA <- ?WALKS, {call_count, Count} <- [erlang:trace_info(MFA, call_count)]]
    end,
    try
        ?assertEqual(?WALKS, Counted),
        {ok, []} = kura_repo_worker:all(?REPO, Q(3)),
        {ok, []} = kura_tenant:with_tenant(Tenant, fun() -> kura_repo_worker:all(?REPO, Q(4)) end),
        ?assertEqual([0, 0, 0, 0, 0], Calls()),
        {ok, []} = kura_repo_worker:all(?REPO, kura_query:limit(Q(5), 1)),
        [Sources, Nested | Others] = Calls(),
        ?assert(Sources > 0 andalso Nested > 0),
        ?assertEqual([1, 2, 0], Others)
    after
        [erlang:trace_pattern(MFA, false, [call_count]) || MFA <- ?WALKS]
    end.

t_hit_is_redacted() ->
    Filtered = fun(Token) -> kura_query:where(kura_query:from(?SECRET), {token, '=', Token}) end,
    Other = fun(Email) -> kura_query:where(kura_query:from(?SECRET), {email, '=', Email}) end,
    Shown = [
        shown(fun() -> kura_repo_worker:all(?REPO, Q) end)
     || Q <- [Filtered(~"t1"), Filtered(~"t2"), Other(~"a@b.c"), Other(~"d@e.f")]
    ],
    ?assertEqual(
        [
            {[?R], [~"t1"]},
            {[?R], [~"t2"]},
            {[~"a@b.c"], [~"a@b.c"]},
            {[~"d@e.f"], [~"d@e.f"]}
        ],
        Shown
    ).

t_hit_requires_a_tenant() ->
    Q = fun(Id) ->
        kura_query:where(
            kura_query:join(kura_query:from(?SHARED), inner, ?SCOPED, {id, id}), {id, '=', Id}
        )
    end,
    {ok, []} = kura_tenant:without_tenant(fun() -> kura_repo_worker:all(?REPO, Q(1)) end),
    {ok, []} = kura_repo_worker:all(?REPO, Q(2)),
    application:set_env(kura, require_tenant, true),
    ?assertError({kura, {tenant_required, ?SCOPED}}, kura_repo_worker:all(?REPO, Q(3))),
    {ok, []} = kura_tenant:without_tenant(fun() -> kura_repo_worker:all(?REPO, Q(4)) end),
    ?assertEqual(3, length(kura_test_recording_driver:statements())).

t_malformed() ->
    Q = (kura_query:from(?SECRET))#kura_query{wheres = ill_typed(not_a_list)},
    ?assertEqual(
        #{shown => redacted, source => ~"accounts", tenant_scoped => [?SECRET]},
        kura_query_compiler:facts(Q)
    ).

t_source() ->
    Sources = fun(Run) ->
        {_Result, Events} = observed(Run),
        [maps:get(source, Event) || Event <- Events]
    end,
    CS = kura_changeset:cast(?SHARED, #{}, #{name => ~"a"}, [name]),
    Stored = kura_changeset:cast(?SHARED, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name]),
    Recent = kura_query:with_cte(kura_query:from(recent), ~"recent", kura_query:from(?SCOPED)),
    ?assertEqual(
        [
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"shared_items"],
            [~"recent"],
            [~"audit_rows"]
        ],
        [
            Sources(Run)
         || Run <- [
                fun() -> kura_repo_worker:all(?REPO, kura_query:from(?SHARED)) end,
                fun() -> kura_repo_worker:all(?REPO, kura_query:from(?SHARED)) end,
                fun() -> kura_repo_worker:insert(?REPO, CS) end,
                fun() -> kura_repo_worker:update(?REPO, Stored) end,
                fun() -> kura_repo_worker:delete(?REPO, Stored) end,
                fun() -> kura_repo_worker:insert_all(?REPO, ?SHARED, [#{name => ~"a"}]) end,
                fun() ->
                    kura_repo_worker:update_all(?REPO, kura_query:from(?SHARED), #{name => ~"b"})
                end,
                fun() -> kura_repo_worker:delete_all(?REPO, kura_query:from(?SHARED)) end,
                fun() -> kura_repo_worker:all(?REPO, Recent) end,
                fun() -> kura_repo_worker:all(?REPO, kura_query:from(~"audit_rows")) end
            ]
        ]
    ).

t_raw_source() ->
    {_Result, [Event]} = observed(fun() ->
        kura_repo_worker:query(?REPO, ~"SELECT 1 FROM \"elsewhere\".\"things\"", [])
    end),
    ?assertEqual(~"things", maps:get(source, Event)),
    ?assertMatch(
        #{source := ~"users"},
        kura_db:build_telemetry_metadata(?REPO, ~"SELECT * FROM \"users\"", [], #{rows => []})
    ),
    ?assertMatch(
        #{source := ~"named"},
        kura_db:build_telemetry_metadata(
            ?REPO, ~"SELECT * FROM \"users\"", [], #{rows => []}, ~"named"
        )
    ),
    ?assertMatch(
        #{source := undefined},
        kura_db:build_telemetry_metadata(
            ?REPO, ~"SELECT * FROM \"users\"", [], #{rows => []}, undefined
        )
    ).

observed(Run) ->
    Handler = {?MODULE, make_ref()},
    ok = telemetry:attach(Handler, [kura, repo, query], fun ?MODULE:forward/4, #{pid => self()}),
    try
        Result = Run(),
        {Result, events()}
    after
        telemetry:detach(Handler)
    end.

shown(Run) ->
    {_Result, [Event]} = observed(Run),
    [{_SQL, Given}] = kura_test_recording_driver:statements(),
    {maps:get(params, Event), Given}.

events() ->
    receive
        {telemetry, Metadata} -> [Metadata | events()]
    after 0 -> []
    end.

forward(_Event, _Measurements, Metadata, #{pid := Pid}) ->
    Pid ! {telemetry, Metadata},
    ok.

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.
