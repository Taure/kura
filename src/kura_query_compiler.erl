-module(kura_query_compiler).
-moduledoc """
Public entry point for compiling `kura_query` ASTs into SQL.

Delegates to the dialect module configured via
`application:set_env(kura, dialect, Module)`. Install a backend package
(e.g. `kura_postgres`, `kura_sqlite`) which provides a dialect.

This module is internal. Use `kura_repo_worker` for executing queries.
""".

-include("kura.hrl").

-export([
    to_sql/1,
    to_sql_cached/1,
    to_sql_from/2,
    insert/3,
    insert/4,
    update/4,
    delete/3,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4,
    dialect/0
]).

-doc "Return the dialect module configured for the running kura app.".
-spec dialect() -> module().
dialect() ->
    case application:get_env(kura, dialect) of
        {ok, M} when is_atom(M) -> M;
        _ -> error(no_dialect_configured)
    end.

-doc "Compile a query record into `{SQL, Params}`.".
-spec to_sql(#kura_query{}) -> {iodata(), [term()]}.
to_sql(Query) ->
    (dialect()):to_sql(Query).

-doc """
Cached version of `to_sql/1`. Caches `{SQL, Params}` keyed by the
query record hash. Identical queries (same structure and values) hit
the cache, avoiding recompilation.
""".
-spec to_sql_cached(#kura_query{}) -> {iodata(), [term()]}.
to_sql_cached(Query) ->
    Key = erlang:phash2(Query),
    case kura_query_cache:get(Key) of
        {ok, Result} ->
            Result;
        miss ->
            Result = to_sql(Query),
            kura_query_cache:put(Key, Result),
            Result
    end.

-doc "Compile a query starting parameter numbering from `StartCounter`. Returns `{SQL, Params, NextCounter}`.".
-spec to_sql_from(#kura_query{}, pos_integer()) -> {iodata(), [term()], pos_integer()}.
to_sql_from(Query, StartCounter) ->
    (dialect()):to_sql_from(Query, StartCounter).

-spec insert(atom() | module(), [atom()], map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data) ->
    (dialect()):insert(SchemaOrTable, Fields, Data).

-spec insert(atom() | module(), [atom()], map(), map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data, Opts) ->
    (dialect()):insert(SchemaOrTable, Fields, Data, Opts).

-spec update(atom() | module(), [atom()], map(), {atom(), term()}) -> {iodata(), [term()]}.
update(SchemaOrTable, Fields, Changes, PK) ->
    (dialect()):update(SchemaOrTable, Fields, Changes, PK).

-spec delete(atom() | module(), atom(), term()) -> {iodata(), [term()]}.
delete(SchemaOrTable, PKField, PKValue) ->
    (dialect()):delete(SchemaOrTable, PKField, PKValue).

-spec update_all(#kura_query{}, map()) -> {iodata(), [term()]}.
update_all(Query, SetMap) ->
    (dialect()):update_all(Query, SetMap).

-spec delete_all(#kura_query{}) -> {iodata(), [term()]}.
delete_all(Query) ->
    (dialect()):delete_all(Query).

-spec insert_all(atom() | module(), [atom()], [map()]) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows) ->
    (dialect()):insert_all(SchemaOrTable, Fields, Rows).

-spec insert_all(atom() | module(), [atom()], [map()], map()) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows, Opts) ->
    (dialect()):insert_all(SchemaOrTable, Fields, Rows, Opts).
