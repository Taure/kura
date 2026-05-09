-module(kura_query_compiler_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%% Verifies that kura_query_compiler delegates to a dialect that the
%% application env can swap. The fake dialect below records its calls
%% so we can assert the facade reached it.

-behaviour(kura_dialect).

-export([
    to_sql/1,
    to_sql_from/2,
    insert/3,
    insert/4,
    update/4,
    delete/3,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4
]).

%%----------------------------------------------------------------------
%% Default dialect
%%----------------------------------------------------------------------

default_dialect_is_pg_test() ->
    %% No env override - facade should resolve to kura_dialect_pg.
    application:unset_env(kura, dialect),
    ?assertEqual(kura_dialect_pg, kura_query_compiler:dialect()).

%%----------------------------------------------------------------------
%% Dialect override is honored by the facade
%%----------------------------------------------------------------------

facade_delegates_to_configured_dialect_test() ->
    application:set_env(kura, dialect, ?MODULE),
    try
        Q = #kura_query{from = my_schema},
        Result = kura_query_compiler:to_sql(Q),
        ?assertEqual({~"FAKE_SQL", [fake_param]}, Result)
    after
        application:unset_env(kura, dialect)
    end.

facade_delegates_insert_to_configured_dialect_test() ->
    application:set_env(kura, dialect, ?MODULE),
    try
        Result = kura_query_compiler:insert(my_schema, [name], #{name => <<"x">>}),
        ?assertEqual({~"FAKE_INSERT", []}, Result)
    after
        application:unset_env(kura, dialect)
    end.

%%----------------------------------------------------------------------
%% to_sql_cached: cache miss + hit
%%----------------------------------------------------------------------

to_sql_cached_miss_compiles_and_stores_test() ->
    application:unset_env(kura, dialect),
    kura_query_cache:init(),
    Q = #kura_query{from = cache_miss_schema},
    Result = kura_query_compiler:to_sql_cached(Q),
    %% Verify it compiled (matches what to_sql/1 would return).
    ?assertEqual(kura_query_compiler:to_sql(Q), Result),
    %% Verify it was actually stored.
    Key = erlang:phash2(Q),
    ?assertEqual({ok, Result}, kura_query_cache:get(Key)).

to_sql_cached_hit_returns_stored_result_test() ->
    application:unset_env(kura, dialect),
    kura_query_cache:init(),
    Q = #kura_query{from = cache_hit_schema},
    Stored = {~"PRE_BAKED_SQL", [pre_baked_param]},
    Key = erlang:phash2(Q),
    kura_query_cache:put(Key, Stored),
    %% to_sql_cached should return the stored value, not recompile.
    ?assertEqual(Stored, kura_query_compiler:to_sql_cached(Q)).

facade_delegates_all_callbacks_to_dialect_test_() ->
    application:set_env(kura, dialect, ?MODULE),
    Q = #kura_query{from = my_schema},
    Cleanup = fun() -> application:unset_env(kura, dialect) end,
    {setup, fun() -> ok end, fun(_) -> Cleanup() end, [
        ?_assertEqual(
            {~"FAKE_SQL", [fake_param], 2},
            kura_query_compiler:to_sql_from(Q, 1)
        ),
        ?_assertEqual(
            {~"FAKE_INSERT", []},
            kura_query_compiler:insert(my_schema, [name], #{name => <<"x">>}, #{})
        ),
        ?_assertEqual(
            {~"FAKE_UPDATE", []},
            kura_query_compiler:update(my_schema, [name], #{name => <<"x">>}, {id, 1})
        ),
        ?_assertEqual(
            {~"FAKE_DELETE", []},
            kura_query_compiler:delete(my_schema, id, 1)
        ),
        ?_assertEqual(
            {~"FAKE_UPDATE_ALL", []},
            kura_query_compiler:update_all(Q, #{name => <<"x">>})
        ),
        ?_assertEqual(
            {~"FAKE_DELETE_ALL", []},
            kura_query_compiler:delete_all(Q)
        ),
        ?_assertEqual(
            {~"FAKE_INSERT_ALL", []},
            kura_query_compiler:insert_all(my_schema, [name], [#{name => <<"x">>}])
        ),
        ?_assertEqual(
            {~"FAKE_INSERT_ALL", []},
            kura_query_compiler:insert_all(
                my_schema, [name], [#{name => <<"x">>}], #{returning => true}
            )
        )
    ]}.

%%----------------------------------------------------------------------
%% PG dialect still produces real SQL through the facade
%%----------------------------------------------------------------------

pg_dialect_through_facade_produces_select_test() ->
    application:unset_env(kura, dialect),
    Q = #kura_query{from = my_schema},
    {SQL, _Params} = kura_query_compiler:to_sql(Q),
    SQLBin = iolist_to_binary(SQL),
    ?assertMatch(<<"SELECT", _/binary>>, SQLBin).

pg_dialect_directly_produces_same_result_test() ->
    Q = #kura_query{from = my_schema},
    Direct = kura_dialect_pg:to_sql(Q),
    application:unset_env(kura, dialect),
    Facade = kura_query_compiler:to_sql(Q),
    ?assertEqual(Direct, Facade).

%%----------------------------------------------------------------------
%% Fake dialect impl
%%----------------------------------------------------------------------

to_sql(_Q) -> {~"FAKE_SQL", [fake_param]}.
to_sql_from(_Q, C) -> {~"FAKE_SQL", [fake_param], C + 1}.
insert(_S, _F, _D) -> {~"FAKE_INSERT", []}.
insert(_S, _F, _D, _O) -> {~"FAKE_INSERT", []}.
update(_S, _F, _C, _PK) -> {~"FAKE_UPDATE", []}.
delete(_S, _PK, _V) -> {~"FAKE_DELETE", []}.
update_all(_Q, _M) -> {~"FAKE_UPDATE_ALL", []}.
delete_all(_Q) -> {~"FAKE_DELETE_ALL", []}.
insert_all(_S, _F, _R) -> {~"FAKE_INSERT_ALL", []}.
insert_all(_S, _F, _R, _O) -> {~"FAKE_INSERT_ALL", []}.
