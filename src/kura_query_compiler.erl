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

The key is the query's shape, with bound values replaced by markers
(see `kura_query_shape`), so every call that differs only in its values
shares one entry. The entry holds the SQL and a parameter template; the
values are bound into the template on each call. A shape the dialect
cannot compile, or whose template fails
`kura_query_shape:valid_template/2`, is remembered as `uncacheable` and
compiled directly from then on, so a genuine compile error still raises
from the real query.

The key holds the whole shape term, never a hash of it. `erlang:phash2/1`
returns only 27 bits, so a long-lived cache accumulates collisions, and a
collision here does not degrade a lookup - it returns another shape's SQL
and parameter template, so the caller silently reads rows from a
different table.

The key also carries the ambient tenant. `kura_repo_worker` normalises the
tenant into the top-level query record before compiling, but a nested CTE
or combination query keeps `prefix = undefined` and resolves it from
`kura_tenant` at emit time, so the compiled SQL is a function of ambient
state the query term does not describe. Without the tenant in the key, one
tenant's cached entry is served to the next - a cross-tenant read.

The dialect is deliberately *not* in the key: it is resolved from boot-time
repo config. Swapping a repo's dialect at runtime requires an explicit
`kura_query_cache:flush/0`.
""".
-spec to_sql_cached(module(), #kura_query{}) -> {iodata(), [term()]}.
to_sql_cached(RepoMod, Query) ->
    {Shape, Values} = kura_query_shape:split(Query),
    Key = {RepoMod, kura_tenant:get_tenant(), Shape},
    case kura_query_cache:get(Key) of
        {ok, {SQL, Template}} ->
            {SQL, kura_query_shape:bind(Template, Values)};
        {ok, uncacheable} ->
            to_sql(RepoMod, Query);
        miss ->
            case compile_shape(RepoMod, Shape, tuple_size(Values)) of
                {ok, {SQL, Template} = Entry} ->
                    kura_query_cache:put(Key, Entry),
                    {SQL, kura_query_shape:bind(Template, Values)};
                uncacheable ->
                    kura_query_cache:put(Key, uncacheable),
                    to_sql(RepoMod, Query)
            end
    end.

%% A dialect may reject a marker where its record type promises an integer
%% (`limit`, `offset`), so a crash here means the shape is uncacheable, not
%% that the query is wrong.
-spec compile_shape(module(), #kura_query{}, non_neg_integer()) ->
    {ok, {binary(), [term()]}} | uncacheable.
compile_shape(RepoMod, Shape, Arity) ->
    try to_sql(RepoMod, Shape) of
        {SQL, Template} ->
            case kura_query_shape:valid_template(Template, Arity) of
                true -> {ok, {iolist_to_binary(SQL), Template}};
                false -> uncacheable
            end
    catch
        _:_ -> uncacheable
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
