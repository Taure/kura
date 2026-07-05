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
- `window_functions` — `OVER (PARTITION BY ... ORDER BY ...)`
- `full_text_search` — the `{Field, matches, Query}` where-condition
- `vector` — pgvector `VECTOR(N)` columns (`{vector, N}` type)
- `transactions` — `BEGIN/COMMIT/ROLLBACK`
- `savepoints` — `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`
- `prepared_statements` — server-side prepared statements

A module that does not implement `capabilities/0` is treated as having
the empty capability set. Anything it does support is opaque to kura.

## Example

A consumer's supervisor refuses to start if PG-only features are
missing:

```erlang
init(_) ->
    case kura_capabilities:require(kura_pool_pgo, [returning, listen_notify]) of
        ok -> {ok, ...};
        {error, _} = Err -> Err
    end.
```

A backend declares its set:

```erlang
-module(kura_pool_pgo).
-behaviour(kura_pool).

capabilities() ->
    [returning, jsonb, arrays, advisory_locks, listen_notify,
     select_for_update_skip_locked, partial_indexes, window_functions,
     full_text_search, transactions, savepoints, prepared_statements].
```
""".

-export([supported/1, has/2, require/2]).

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
