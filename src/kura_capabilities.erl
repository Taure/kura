-module(kura_capabilities).
-moduledoc """
Capability flags for kura backends and pool implementations.

A module that participates in kura's backend story (today: the pool
implementation; in 2.0 likely a dedicated backend aggregator) declares
the capability set it supports by implementing the optional
`capabilities/0` callback. Consumers ask kura whether the running
backend supports the features they need.

This makes "we don't support that on this backend" a boot error
instead of a 3am pager. It is also the discipline that keeps the
future multi-backend story honest: a non-SQL backend (Mnesia, Riak)
will declare a small capability set, SQL-needing consumers will
refuse to start, and there will be no runtime surprises.

## Standard capabilities

The capability atom set is open — backends may declare custom flags
and consumers may match against them. The standard atoms kura itself
cares about:

- `returning` — `INSERT/UPDATE/DELETE ... RETURNING`
- `jsonb` — `JSONB` column type with operators (the SQLite backend
  declares `json` instead, for its text-based JSON1 support)
- `arrays` — array column types
- `advisory_locks` — `pg_advisory_lock` family
- `listen_notify` — `LISTEN/NOTIFY` pubsub
- `select_for_update_skip_locked` — `SELECT ... FOR UPDATE SKIP LOCKED`
- `partial_indexes` — `CREATE INDEX ... WHERE`
- `concurrent_indexes` — `CREATE INDEX CONCURRENTLY`; without it the
  migrator compiles `concurrently => true` to a plain `CREATE INDEX` for
  a non-Postgres dialect and refuses it for Postgres
- `window_functions` — `OVER (PARTITION BY ... ORDER BY ...)`
- `full_text_search` — the `{Field, matches, Query}` where-condition
- `vector` — pgvector `VECTOR(N)` columns (`{vector, N}` type)
- `transactions` — `BEGIN/COMMIT/ROLLBACK`
- `savepoints` — `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`
- `prepared_statements` — server-side prepared statements

A module that does not implement `capabilities/0` is treated as having
the empty capability set. Anything it does support is opaque to kura.

## Queries

A query is checked against the repo's pool module when it is compiled, so
an operation the backend does not declare raises
`{kura, {unsupported_capability, Capability, PoolModule}}` before any SQL
is built, instead of failing in the database:

| In the query | Capability |
| --- | --- |
| `{Field, matches, Query}` | `full_text_search` |
| a `{over, Fn, Spec}` select expression | `window_functions` |
| a `{distance, Field, Metric, Vector}` | `vector` |
| `kura_query:lock(Q, {Strength, skip_locked})` | `select_for_update_skip_locked` |

CTEs, combinations and subqueries are checked too. A pool module without
`capabilities/0` is not checked: it has made no statement either way.

## Example

A consumer's supervisor refuses to start if PG-only features are
missing:

```erlang
init(_) ->
    case kura_capabilities:require(kura_pool_minato, [returning, listen_notify]) of
        ok -> {ok, ...};
        {error, _} = Err -> Err
    end.
```

A backend declares its set:

```erlang
-module(my_pool).
-behaviour(kura_pool).

capabilities() ->
    [returning, jsonb, arrays, advisory_locks, listen_notify,
     select_for_update_skip_locked, partial_indexes, window_functions,
     full_text_search, transactions, savepoints, prepared_statements].
```
""".

-include("kura.hrl").

-export([supported/1, has/2, require/2, check_query/2]).

-define(QUERY_CAPABILITIES, [
    full_text_search, window_functions, vector, select_for_update_skip_locked
]).

-export_type([capability/0, capability_set/0]).

-doc "An open atom set. Backends may define custom capability atoms.".
-type capability() :: atom().

-doc "A list of capabilities a backend supports or a consumer requires.".
-type capability_set() :: [capability()].

-doc """
Optional callback. Return the set of capabilities this module supports.

Modules that do not implement this callback are treated as supporting
nothing.
""".
-callback capabilities() -> capability_set().

-optional_callbacks([capabilities/0]).

-doc """
Return the capability set declared by `Module`.

Returns `[]` if the module does not export `capabilities/0`.
""".
-spec supported(module()) -> capability_set().
supported(Module) ->
    case erlang:function_exported(Module, capabilities, 0) of
        true -> Module:capabilities();
        false -> []
    end.

-doc "Return `true` if `Module` declares `Cap`.".
-spec has(module(), capability()) -> boolean().
has(Module, Cap) ->
    lists:member(Cap, supported(Module)).

-doc """
Return `ok` if `Module` declares every capability in `Required`.

Otherwise return `{error, {missing_capabilities, Missing}}` listing
the capabilities that the module did not declare. Intended to be
called from a supervisor init so the application refuses to start
on a backend that lacks required features.
""".
-spec require(module(), capability_set()) ->
    ok | {error, {missing_capabilities, capability_set()}}.
require(Module, Required) ->
    Have = supported(Module),
    Missing = [C || C <- Required, not lists:member(C, Have)],
    case Missing of
        [] -> ok;
        _ -> {error, {missing_capabilities, Missing}}
    end.

-doc """
Check that `PoolMod` declares every capability `Query` needs. See the
moduledoc for what needs which. Raises
`{kura, {unsupported_capability, Capability, PoolMod}}` for the first one
missing. A module without `capabilities/0` passes.

The query is only read when the module lacks one of the capabilities a
query can need.
""".
-spec check_query(module(), #kura_query{}) -> ok.
check_query(PoolMod, Query) ->
    case undeclared_query_capabilities(PoolMod) of
        [] ->
            ok;
        Undeclared ->
            case [Cap || Cap <- query_needs(Query), lists:member(Cap, Undeclared)] of
                [] -> ok;
                [Cap | _] -> erlang:error({kura, {unsupported_capability, Cap, PoolMod}})
            end
    end.

undeclared_query_capabilities(PoolMod) ->
    case declares_capabilities(PoolMod) of
        true -> ?QUERY_CAPABILITIES -- PoolMod:capabilities();
        false -> []
    end.

%% function_exported/3 is false for a module not yet loaded.
declares_capabilities(PoolMod) ->
    erlang:function_exported(PoolMod, capabilities, 0) orelse
        (not erlang:module_loaded(PoolMod) andalso
            code:ensure_loaded(PoolMod) =:= {module, PoolMod} andalso
            erlang:function_exported(PoolMod, capabilities, 0)).

-spec query_needs(#kura_query{}) -> capability_set().
query_needs(#kura_query{} = Q) ->
    lists:append([
        conditions_need(Q#kura_query.wheres),
        conditions_need(Q#kura_query.havings),
        select_needs(Q#kura_query.select),
        orders_need(Q#kura_query.order_bys),
        lock_needs(Q#kura_query.lock),
        bodies_need(Q#kura_query.ctes),
        bodies_need(Q#kura_query.combinations)
    ]).

conditions_need([Condition | Rest]) -> condition_needs(Condition) ++ conditions_need(Rest);
conditions_need(_) -> [].

condition_needs({Junction, Conditions}) when Junction =:= 'and'; Junction =:= 'or' ->
    conditions_need(Conditions);
condition_needs({'not', Condition}) ->
    condition_needs(Condition);
condition_needs({{distance, _, _, _}, _Op, _Threshold}) ->
    [vector];
condition_needs({_Field, matches, _Query}) ->
    [full_text_search];
condition_needs({_Field, in, {subquery, #kura_query{} = Q}}) ->
    query_needs(Q);
condition_needs({Exists, {subquery, #kura_query{} = Q}}) when
    Exists =:= exists; Exists =:= not_exists
->
    query_needs(Q);
condition_needs(_Condition) ->
    [].

select_needs({exprs, Exprs}) -> exprs_need(Exprs);
select_needs(_Fields) -> [].

exprs_need([{_Alias, {over, _Fn, _Spec}} | Rest]) -> [window_functions | exprs_need(Rest)];
exprs_need([{_Alias, {distance, _, _, _}} | Rest]) -> [vector | exprs_need(Rest)];
exprs_need([_ | Rest]) -> exprs_need(Rest);
exprs_need(_) -> [].

orders_need([{{distance, _, _, _}, _Dir} | Rest]) -> [vector | orders_need(Rest)];
orders_need([_ | Rest]) -> orders_need(Rest);
orders_need(_) -> [].

lock_needs({_Strength, skip_locked}) ->
    [select_for_update_skip_locked];
lock_needs(Lock) when is_binary(Lock) ->
    case kura_query:lock_clause(Lock) of
        {_Strength, skip_locked} -> [select_for_update_skip_locked];
        _ -> []
    end;
lock_needs(_Lock) ->
    [].

bodies_need([{_NameOrType, #kura_query{} = Q} | Rest]) -> query_needs(Q) ++ bodies_need(Rest);
bodies_need([_ | Rest]) -> bodies_need(Rest);
bodies_need(_) -> [].
