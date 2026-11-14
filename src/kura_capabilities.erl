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

The capability atom set is open: backends may declare custom flags
and consumers may match against them. The standard atoms kura itself
cares about:

- `returning`: `INSERT/UPDATE/DELETE ... RETURNING`
- `jsonb`: `JSONB` column type with operators (the SQLite backend
  declares `json` instead, for its text-based JSON1 support)
- `arrays`: array column types
- `advisory_locks`: `pg_advisory_lock` family
- `listen_notify`: `LISTEN/NOTIFY` pubsub
- `select_for_update_skip_locked`: `SELECT ... FOR UPDATE SKIP LOCKED`
- `row_locks`: the row locks of `kura_query:lock/2`: `FOR UPDATE`,
  `FOR NO KEY UPDATE`, `FOR SHARE` and `FOR KEY SHARE`. A dialect may take
  a stronger lock than the one asked for, never a weaker one, so a backend
  with only `FOR UPDATE` and `FOR SHARE` declares it and takes `FOR UPDATE`
  for `for_no_key_update` and `FOR SHARE` for `for_key_share`
- `distinct_on`: `SELECT DISTINCT ON (...)`, from `kura_query:distinct/2`
- `cursors`: SQL cursors, `DECLARE ... CURSOR` and `FETCH`, which
  `kura_stream` writes. A backend that reads a result in steps some other
  way does not declare it
- `partial_indexes`: `CREATE INDEX ... WHERE`
- `concurrent_indexes`: `CREATE INDEX CONCURRENTLY`; without it the
  migrator compiles `concurrently => true` to a plain `CREATE INDEX` for
  a non-Postgres dialect and refuses it for Postgres
- `window_functions`: `OVER (PARTITION BY ... ORDER BY ...)`
- `full_text_search`: the `{Field, matches, Query}` where-condition
- `vector`: pgvector `VECTOR(N)` columns (`{vector, N}` type)
- `transactions`: `BEGIN/COMMIT/ROLLBACK`
- `savepoints`: `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`
- `prepared_statements`: server-side prepared statements

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
| `kura_query:lock(Q, Lock)`, any lock | `row_locks` |
| `kura_query:distinct(Q, Fields)` | `distinct_on` |

CTEs, combinations and subqueries are checked too. `kura_stream:stream/4`
raises the same error for `cursors`.

## Capability model

kura 2.30 checked nothing, so a list written for it says what a backend
has and not what it lacks: it may leave out something the backend runs. A
backend therefore says which revision of the checks its list was written
against, with the optional `capability_model/0` callback, and only then is
it checked:

| Model | Checked |
| --- | --- |
| none: no `capability_model/0` | nothing |
| 1 | `full_text_search`, `window_functions`, `vector`, `select_for_update_skip_locked`, `row_locks`, `distinct_on`, `cursors` |

A model number never changes its meaning. When kura starts checking another
capability, that is a new model, and a backend on an earlier one is not
checked for it. A value that is not a model of this kura is refused when
the repo's pool is started, with
`{error, {invalid_capability_model, PoolModule, Value}}`, and raises
`{kura, {invalid_capability_model, PoolModule, Value}}` from a check.

kura 3.0 makes the latest model mandatory: a pool module without
`capability_model/0`, or on an earlier model, is checked as if it declared
the latest.

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
     select_for_update_skip_locked, row_locks, distinct_on, cursors,
     partial_indexes, window_functions, full_text_search, transactions,
     savepoints, prepared_statements].

capability_model() -> 1.
```
""".

-include("kura.hrl").

-export([supported/1, has/2, require/2, check/2, check_query/2, model/1, validate/1]).

-define(MODEL_1, [
    full_text_search,
    window_functions,
    vector,
    select_for_update_skip_locked,
    row_locks,
    distinct_on,
    cursors
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

-doc """
Optional callback. Return the revision of the capability checks the list
from `capabilities/0` was written against: `1`. See the moduledoc. A
module without it is not checked.
""".
-callback capability_model() -> pos_integer().

-optional_callbacks([capabilities/0, capability_model/0]).

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
missing. A module without `capability_model/0` passes.

The query is only read when the module lacks one of the capabilities its
model checks.
""".
-spec check_query(module(), #kura_query{}) -> ok.
check_query(PoolMod, Query) ->
    case undeclared(PoolMod) of
        [] ->
            ok;
        Undeclared ->
            case [Cap || Cap <- query_needs(Query), lists:member(Cap, Undeclared)] of
                [] -> ok;
                [Cap | _] -> erlang:error({kura, {unsupported_capability, Cap, PoolMod}})
            end
    end.

-doc """
Check one capability as `check_query/2` checks those a query needs: raises
`{kura, {unsupported_capability, Capability, PoolMod}}` when `PoolMod`
declares a capability model, that model checks `Capability`, and its list
leaves it out. `has/2` and `require/2` answer for any capability, checked
or not.
""".
-spec check(module(), capability()) -> ok.
check(PoolMod, Capability) ->
    case lists:member(Capability, undeclared(PoolMod)) of
        true -> erlang:error({kura, {unsupported_capability, Capability, PoolMod}});
        false -> ok
    end.

-spec undeclared(module()) -> capability_set().
undeclared(PoolMod) ->
    case model(PoolMod) of
        unchecked -> [];
        1 -> ?MODEL_1 -- supported(PoolMod)
    end.

-doc """
The capability model `PoolMod` declares, or `unchecked` when it has no
`capability_model/0`. Raises
`{kura, {invalid_capability_model, PoolMod, Value}}` for a value that is
not a model of this kura.
""".
-spec model(module()) -> unchecked | 1.
model(PoolMod) ->
    case declared_model(PoolMod) of
        {ok, Model} -> Model;
        {error, Invalid} -> erlang:error({kura, Invalid})
    end.

-doc false.
-spec validate(module()) -> ok | {error, {invalid_capability_model, module(), term()}}.
validate(PoolMod) ->
    case declared_model(PoolMod) of
        {ok, _Model} -> ok;
        {error, _} = Invalid -> Invalid
    end.

-spec declared_model(module()) ->
    {ok, unchecked | 1} | {error, {invalid_capability_model, module(), term()}}.
declared_model(PoolMod) ->
    case declares_model(PoolMod) of
        true ->
            case PoolMod:capability_model() of
                1 -> {ok, 1};
                Value -> {error, {invalid_capability_model, PoolMod, Value}}
            end;
        false ->
            {ok, unchecked}
    end.

%% function_exported/3 is false for a module not yet loaded.
declares_model(PoolMod) ->
    erlang:function_exported(PoolMod, capability_model, 0) orelse
        (not erlang:module_loaded(PoolMod) andalso
            code:ensure_loaded(PoolMod) =:= {module, PoolMod} andalso
            erlang:function_exported(PoolMod, capability_model, 0)).

-spec query_needs(#kura_query{}) -> capability_set().
query_needs(#kura_query{} = Q) ->
    lists:append([
        conditions_need(Q#kura_query.wheres),
        conditions_need(Q#kura_query.havings),
        select_needs(Q#kura_query.select),
        orders_need(Q#kura_query.order_bys),
        lock_needs(Q#kura_query.lock),
        distinct_needs(Q#kura_query.distinct)
        | [query_needs(Nested) || Nested <- kura_query:nested(Q)]
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

lock_needs(undefined) ->
    [];
lock_needs(Lock) ->
    case skips_locked(kura_query:lock_clause(Lock)) of
        true -> [select_for_update_skip_locked, row_locks];
        false -> [row_locks]
    end.

skips_locked([_ | _] = Clauses) -> lists:any(fun skips_locked/1, Clauses);
skips_locked({_Strength, skip_locked}) -> true;
skips_locked({_Strength, #{wait := skip_locked}}) -> true;
skips_locked(_Clause) -> false.

distinct_needs(Fields) when is_list(Fields) -> [distinct_on];
distinct_needs(_Distinct) -> [].
