-module(kura_query_compiler).
-moduledoc """
Public entry point for compiling `kura_query` ASTs into SQL.

Resolves the dialect per repo: the configured `dialect` module lives in
the repo's config map (set automatically when the repo declares a
`{backend, ...}`). Different repos in the same app may use different
dialects.

This module is internal. Use `kura_repo_worker` for executing queries.
""".

-include("kura.hrl").

-export([
    to_sql/2,
    to_sql_cached/2,
    to_sql_from/3,
    insert/4,
    insert/5,
    update/5,
    delete/3,
    update_all/3,
    delete_all/2,
    insert_all/4,
    insert_all/5,
    dialect/1
]).

-doc """
Return the dialect module configured for `RepoMod`.

Resolution order:

1. `dialect` key in the repo's config map (multi-repo or v2.4 flat env).
2. Global `application:get_env(kura, dialect)` for legacy single-dialect setups.
""".
-spec dialect(module()) -> module().
dialect(RepoMod) ->
    Cfg = kura_repo:config(RepoMod),
    case maps:get(dialect, Cfg, undefined) of
        M when is_atom(M), M =/= undefined ->
            M;
        _ ->
            case application:get_env(kura, dialect) of
                {ok, GD} when is_atom(GD) -> GD;
                _ -> error({no_dialect_configured, RepoMod})
            end
    end.

-doc "Compile a query record into `{SQL, Params}` for `RepoMod`'s dialect.".
-spec to_sql(module(), #kura_query{}) -> {iodata(), [term()]}.
to_sql(RepoMod, Query) ->
    (dialect(RepoMod)):to_sql(Query).

-doc """
Cached `to_sql/2`. The cache key includes `RepoMod` so two repos with
different dialects don't share an entry.
""".
-spec to_sql_cached(module(), #kura_query{}) -> {iodata(), [term()]}.
to_sql_cached(RepoMod, Query) ->
    Key = {RepoMod, erlang:phash2(Query)},
    case kura_query_cache:get(Key) of
        {ok, Result} ->
            Result;
        miss ->
            Result = to_sql(RepoMod, Query),
            kura_query_cache:put(Key, Result),
            Result
    end.

-spec to_sql_from(module(), #kura_query{}, pos_integer()) ->
    {iodata(), [term()], pos_integer()}.
to_sql_from(RepoMod, Query, StartCounter) ->
    (dialect(RepoMod)):to_sql_from(Query, StartCounter).

-spec insert(module(), atom() | module(), [atom()], map()) -> {iodata(), [term()]}.
insert(RepoMod, SchemaOrTable, Fields, Data) ->
    (dialect(RepoMod)):insert(SchemaOrTable, Fields, Data).

-spec insert(module(), atom() | module(), [atom()], map(), map()) -> {iodata(), [term()]}.
insert(RepoMod, SchemaOrTable, Fields, Data, Opts) ->
    (dialect(RepoMod)):insert(SchemaOrTable, Fields, Data, Opts).

-spec update(module(), atom() | module(), [atom()], map(), [{atom(), term()}]) ->
    {iodata(), [term()]}.
update(RepoMod, SchemaOrTable, Fields, Changes, KeyClauses) ->
    (dialect(RepoMod)):update(SchemaOrTable, Fields, Changes, KeyClauses).

-spec delete(module(), atom() | module(), [{atom(), term()}]) -> {iodata(), [term()]}.
delete(RepoMod, SchemaOrTable, KeyClauses) ->
    (dialect(RepoMod)):delete(SchemaOrTable, KeyClauses).

-spec update_all(module(), #kura_query{}, map()) -> {iodata(), [term()]}.
update_all(RepoMod, Query, SetMap) ->
    (dialect(RepoMod)):update_all(Query, SetMap).

-spec delete_all(module(), #kura_query{}) -> {iodata(), [term()]}.
delete_all(RepoMod, Query) ->
    (dialect(RepoMod)):delete_all(Query).

-spec insert_all(module(), atom() | module(), [atom()], [map()]) -> {iodata(), [term()]}.
insert_all(RepoMod, SchemaOrTable, Fields, Rows) ->
    (dialect(RepoMod)):insert_all(SchemaOrTable, Fields, Rows).

-spec insert_all(module(), atom() | module(), [atom()], [map()], map()) ->
    {iodata(), [term()]}.
insert_all(RepoMod, SchemaOrTable, Fields, Rows, Opts) ->
    (dialect(RepoMod)):insert_all(SchemaOrTable, Fields, Rows, Opts).
