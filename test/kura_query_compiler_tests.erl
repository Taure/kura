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
        Key = {?REPO, erlang:phash2(Q)},
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
        Key = {?REPO, erlang:phash2(Q)},
        kura_query_cache:put(Key, Stored),
        ?assertEqual(Stored, kura_query_compiler:to_sql_cached(?REPO, Q))
    after
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
