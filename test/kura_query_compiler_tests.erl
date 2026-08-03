-module(kura_query_compiler_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%% Verifies that kura_query_compiler delegates to a dialect that the
%% per-repo config can swap. The fake dialect below records its calls
%% so we can assert the facade reached it.

-behaviour(kura_dialect).

-export([
    to_sql/1,
    to_sql_from/2,
    insert/3,
    insert/4,
    update/4,
    delete/2,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4
]).

-define(REPO, my_test_repo).

set_repo_dialect(Dialect) ->
    application:set_env(kura, repos, #{?REPO => #{dialect => Dialect}}).

unset_repo() ->
    application:unset_env(kura, repos).

%%----------------------------------------------------------------------
%% No dialect configured: must error with the repo name.
%%----------------------------------------------------------------------

dialect_unconfigured_errors_test() ->
    application:unset_env(kura, dialect),
    application:set_env(kura, repos, #{?REPO => #{}}),
    try
        ?assertError({no_dialect_configured, ?REPO}, kura_query_compiler:dialect(?REPO))
    after
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% Dialect override is honored by the facade
%%----------------------------------------------------------------------

facade_delegates_to_configured_dialect_test() ->
    set_repo_dialect(?MODULE),
    try
        Q = #kura_query{from = my_schema},
        Result = kura_query_compiler:to_sql(?REPO, Q),
        ?assertEqual({~"FAKE_SQL", [fake_param]}, Result)
    after
        unset_repo()
    end.

facade_delegates_insert_to_configured_dialect_test() ->
    set_repo_dialect(?MODULE),
    try
        Result = kura_query_compiler:insert(?REPO, my_schema, [name], #{name => <<"x">>}),
        ?assertEqual({~"FAKE_INSERT", []}, Result)
    after
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% to_sql_cached: cache miss + hit (key prefixed with RepoMod)
%%----------------------------------------------------------------------

to_sql_cached_miss_compiles_and_stores_test() ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    try
        Q = #kura_query{from = cache_miss_schema},
        Result = kura_query_compiler:to_sql_cached(?REPO, Q),
        ?assertEqual(kura_query_compiler:to_sql(?REPO, Q), Result),
        Key = {?REPO, kura_tenant:get_tenant(), Q},
        ?assertEqual({ok, Result}, kura_query_cache:get(Key))
    after
        unset_repo()
    end.

to_sql_cached_hit_returns_stored_result_test() ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    try
        Q = #kura_query{from = cache_hit_schema},
        Stored = {~"PRE_BAKED_SQL", [pre_baked_param]},
        Key = {?REPO, kura_tenant:get_tenant(), Q},
        kura_query_cache:put(Key, Stored),
        ?assertEqual(Stored, kura_query_compiler:to_sql_cached(?REPO, Q))
    after
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% Regression: two distinct queries that share an `erlang:phash2/1' value
%% must not share a cache entry. A cached entry carries bound parameters
%% as well as SQL, so keying on that hash returned another query's rows -
%% in the wild, rows of a different table entirely.
%%
%% The colliding pairs are searched for at run time rather than hardcoded:
%% the test then stays meaningful if a future OTP changes phash2/1, and it
%% documents how dense collisions are in a 27-bit keyspace.
%%----------------------------------------------------------------------

colliding_queries_across_schemas_do_not_share_cache_entries_test() ->
    Build = fun(N) ->
        [
            #kura_query{from = schema_a, wheres = [{id, N}]},
            #kura_query{from = schema_b, wheres = [{id, N}]}
        ]
    end,
    DifferentTable = fun(A, B) -> A#kura_query.from =/= B#kura_query.from end,
    {QA, QB} = find_collision(Build, DifferentTable),
    assert_cached_independently(QA, QB).

colliding_params_within_a_schema_do_not_share_cache_entries_test() ->
    Build = fun(N) -> [#kura_query{from = schema_a, wheres = [{id, N}]}] end,
    {QA, QB} = find_collision(Build, fun(_, _) -> true end),
    assert_cached_independently(QA, QB).

%% The severity driver: a collision between two tenants' queries is a
%% cross-tenant read, not merely a wrong table.
colliding_tenants_do_not_share_cache_entries_test() ->
    Build = fun(N) ->
        [
            #kura_query{
                from = players,
                prefix = <<"t_", (integer_to_binary(N))/binary>>,
                wheres = [{id, 1}]
            }
        ]
    end,
    {QA, QB} = find_collision(Build, fun(_, _) -> true end),
    ?assertNotEqual(QA#kura_query.prefix, QB#kura_query.prefix),
    assert_cached_independently(QA, QB).

%% A nested CTE keeps prefix = undefined and resolves the tenant from
%% kura_tenant at emit time, so the ambient tenant has to be part of the
%% cache key or one tenant is served the other's compiled SQL.
ambient_tenant_is_part_of_the_cache_key_test() ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    kura_query_cache:flush(),
    Q = kura_query:with_cte(
        kura_query:from(outer_schema),
        ~"scoped",
        kura_query:from(inner_schema)
    ),
    try
        kura_tenant:put_tenant({prefix, ~"tenant_a"}),
        SqlA = kura_query_compiler:to_sql_cached(?REPO, Q),
        kura_tenant:put_tenant({prefix, ~"tenant_b"}),
        SqlB = kura_query_compiler:to_sql_cached(?REPO, Q),
        ?assertNotEqual(SqlA, SqlB),
        ?assertEqual(kura_query_compiler:to_sql(?REPO, Q), SqlB)
    after
        kura_tenant:clear_tenant(),
        kura_query_cache:flush(),
        unset_repo()
    end.

%% Walk N upwards building candidate queries until two that Accept approves
%% hash alike. A rejected collision keeps the incumbent and scanning goes on.
find_collision(Build, Accept) ->
    find_collision(Build, Accept, #{}, 0).

find_collision(_Build, _Accept, _Seen, N) when N > 200000 ->
    erlang:error(no_phash2_collision_found);
find_collision(Build, Accept, Seen, N) ->
    case scan_candidates(Build(N), Accept, Seen) of
        {collision, Pair} -> Pair;
        {ok, Seen1} -> find_collision(Build, Accept, Seen1, N + 1)
    end.

scan_candidates([], _Accept, Seen) ->
    {ok, Seen};
scan_candidates([Q | Rest], Accept, Seen) ->
    Hash = erlang:phash2(Q),
    case maps:find(Hash, Seen) of
        {ok, Other} ->
            case Accept(Other, Q) of
                true -> {collision, {Other, Q}};
                false -> scan_candidates(Rest, Accept, Seen)
            end;
        error ->
            scan_candidates(Rest, Accept, Seen#{Hash => Q})
    end.

assert_cached_independently(QA, QB) ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    kura_query_cache:flush(),
    try
        ?assertEqual(erlang:phash2(QA), erlang:phash2(QB)),
        ExpectedA = kura_query_compiler:to_sql(?REPO, QA),
        ExpectedB = kura_query_compiler:to_sql(?REPO, QB),
        ?assertNotEqual(ExpectedA, ExpectedB),
        %% Prime the cache with A, then B must still compile to its own SQL
        %% and its own parameters, not A's.
        ?assertEqual(ExpectedA, kura_query_compiler:to_sql_cached(?REPO, QA)),
        ?assertEqual(ExpectedB, kura_query_compiler:to_sql_cached(?REPO, QB)),
        ?assertEqual(ExpectedA, kura_query_compiler:to_sql_cached(?REPO, QA))
    after
        kura_query_cache:flush(),
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% The cache is bounded: parameterised queries intern one entry per
%% distinct value set, so an unbounded table is a memory leak. Both limits
%% are in words, because entry *size* is caller-influenced - one `in' list
%% is stored in the key and again in the value.
%%----------------------------------------------------------------------

cache_is_bounded_by_max_memory_test() ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    kura_query_cache:flush(),
    application:set_env(kura, query_cache_max_memory, 2000),
    try
        Q = fun(N) -> #kura_query{from = bounded_schema, wheres = [{id, N}]} end,
        _ = [kura_query_compiler:to_sql_cached(?REPO, Q(N)) || N <- lists:seq(1, 500)],
        ?assert(ets:info(kura_query_cache, memory) < 2000 * 2),
        %% Eviction must never change what a query compiles to. This is what
        %% stops a future LRU from evicting by anything other than whole key.
        Evicted = Q(1),
        ?assertEqual(
            kura_query_compiler:to_sql(?REPO, Evicted),
            kura_query_compiler:to_sql_cached(?REPO, Evicted)
        )
    after
        application:unset_env(kura, query_cache_max_memory),
        kura_query_cache:flush(),
        unset_repo()
    end.

oversized_entries_are_never_interned_test() ->
    set_repo_dialect(kura_dialect_pg),
    kura_query_cache:init(),
    kura_query_cache:flush(),
    application:set_env(kura, query_cache_max_entry_size, 64),
    try
        Big = #kura_query{
            from = bounded_schema,
            wheres = [{id, in, lists:seq(1, 5000)}]
        },
        Expected = kura_query_compiler:to_sql(?REPO, Big),
        ?assertEqual(Expected, kura_query_compiler:to_sql_cached(?REPO, Big)),
        ?assertEqual(0, ets:info(kura_query_cache, size)),
        %% Still correct on every subsequent call, just uncached.
        ?assertEqual(Expected, kura_query_compiler:to_sql_cached(?REPO, Big))
    after
        application:unset_env(kura, query_cache_max_entry_size),
        kura_query_cache:flush(),
        unset_repo()
    end.

facade_delegates_all_callbacks_to_dialect_test_() ->
    set_repo_dialect(?MODULE),
    Q = #kura_query{from = my_schema},
    Cleanup = fun() -> unset_repo() end,
    {setup, fun() -> ok end, fun(_) -> Cleanup() end, [
        ?_assertEqual(
            {~"FAKE_SQL", [fake_param], 2},
            kura_query_compiler:to_sql_from(?REPO, Q, 1)
        ),
        ?_assertEqual(
            {~"FAKE_INSERT", []},
            kura_query_compiler:insert(?REPO, my_schema, [name], #{name => <<"x">>}, #{})
        ),
        ?_assertEqual(
            {~"FAKE_UPDATE", []},
            kura_query_compiler:update(?REPO, my_schema, [name], #{name => <<"x">>}, [{id, 1}])
        ),
        ?_assertEqual(
            {~"FAKE_DELETE", []},
            kura_query_compiler:delete(?REPO, my_schema, [{id, 1}])
        ),
        ?_assertEqual(
            {~"FAKE_UPDATE_ALL", []},
            kura_query_compiler:update_all(?REPO, Q, #{name => <<"x">>})
        ),
        ?_assertEqual(
            {~"FAKE_DELETE_ALL", []},
            kura_query_compiler:delete_all(?REPO, Q)
        ),
        ?_assertEqual(
            {~"FAKE_INSERT_ALL", []},
            kura_query_compiler:insert_all(?REPO, my_schema, [name], [#{name => <<"x">>}])
        ),
        ?_assertEqual(
            {~"FAKE_INSERT_ALL", []},
            kura_query_compiler:insert_all(
                ?REPO, my_schema, [name], [#{name => <<"x">>}], #{returning => true}
            )
        )
    ]}.

%%----------------------------------------------------------------------
%% Two repos with different dialects don't share cache entries.
%%----------------------------------------------------------------------

two_repos_different_dialects_test() ->
    application:set_env(kura, repos, #{
        repo_a => #{dialect => ?MODULE},
        repo_b => #{dialect => kura_dialect_pg}
    }),
    kura_query_cache:init(),
    try
        Q = #kura_query{from = my_schema},
        ResA = kura_query_compiler:to_sql(repo_a, Q),
        ResB = kura_query_compiler:to_sql(repo_b, Q),
        ?assertEqual({~"FAKE_SQL", [fake_param]}, ResA),
        ?assertNotEqual(ResA, ResB)
    after
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% PG dialect still produces real SQL through the facade
%%----------------------------------------------------------------------

pg_dialect_through_facade_produces_select_test() ->
    set_repo_dialect(kura_dialect_pg),
    try
        Q = #kura_query{from = my_schema},
        {SQL, _Params} = kura_query_compiler:to_sql(?REPO, Q),
        SQLBin = iolist_to_binary(SQL),
        ?assertMatch(<<"SELECT", _/binary>>, SQLBin)
    after
        unset_repo()
    end.

pg_dialect_directly_produces_same_result_test() ->
    set_repo_dialect(kura_dialect_pg),
    try
        Q = #kura_query{from = my_schema},
        Direct = kura_dialect_pg:to_sql(Q),
        Facade = kura_query_compiler:to_sql(?REPO, Q),
        ?assertEqual(Direct, Facade)
    after
        unset_repo()
    end.

%%----------------------------------------------------------------------
%% Fake dialect impl
%%----------------------------------------------------------------------

to_sql(_Q) -> {~"FAKE_SQL", [fake_param]}.
to_sql_from(_Q, C) -> {~"FAKE_SQL", [fake_param], C + 1}.
insert(_S, _F, _D) -> {~"FAKE_INSERT", []}.
insert(_S, _F, _D, _O) -> {~"FAKE_INSERT", []}.
update(_S, _F, _C, _KeyClauses) -> {~"FAKE_UPDATE", []}.
delete(_S, _KeyClauses) -> {~"FAKE_DELETE", []}.
update_all(_Q, _M) -> {~"FAKE_UPDATE_ALL", []}.
delete_all(_Q) -> {~"FAKE_DELETE_ALL", []}.
insert_all(_S, _F, _R) -> {~"FAKE_INSERT_ALL", []}.
insert_all(_S, _F, _R, _O) -> {~"FAKE_INSERT_ALL", []}.
