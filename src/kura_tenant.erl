-module(kura_tenant).
-moduledoc ~"""
Process dictionary-based multitenancy. Stores the current tenant strategy
(schema prefix or attribute filter) in the process dictionary for use by
Kura repo operations.

Under the attribute strategy every query kura compiles is passed through
`scope_query/2`, which adds the tenant condition to the query and to every
query nested in it. Under the prefix strategy the dialect qualifies each
table with the schema when it emits SQL.

## Scope and lifetime

`with_tenant/2` sets the tenant for one piece of work and puts the process
back as it was when the work ends, however it ends. `put_tenant/1` sets it
for the rest of the process's life. Use `with_tenant/2` in any process that
outlives a request, such as a pool worker or a job runner: a tenant left
there by `put_tenant/1` is the tenant of whatever that process does next.

A process kura starts for a parallel preload takes on the caller's scope
and ends with the preload. No other process inherits a tenant, and kura
keeps none on a pooled connection: a prefix tenant qualifies each table in
the statement and never changes the connection's `search_path`.

## Requiring a tenant

The tenant lives in the process dictionary, so a process that never set
one has none: a spawned process does not inherit its parent's, and a
query run there is not scoped at all. Set

```erlang
{kura, [{require_tenant, true}]}
```

and a query or a changeset write with no tenant in the process raises
`{kura, {tenant_required, Source}}` instead of running unscoped, `Source`
being the schema or table it reads or writes. A query is judged by
everything it reads (see `kura_query:sources/1`): what it is from and what
it joins, in the query itself and in every CTE, combination and subquery
inside it. Two things lift the requirement:

- a schema shared by every tenant exports `tenant_scoped() -> false`;
- a deliberate cross-tenant path, such as an admin report or a
  maintenance job, runs inside `without_tenant/1`.

The setting is read once, on first use. Call `reload_settings/0` after
changing it at runtime. Left unset, a missing tenant is not an error, and
the first `put_tenant/1` of a real tenant logs one warning per boot.
`false` says the same without the warning. The default becomes `true` in
kura 3.0, where a single-tenant application sets it to `false`.

Raw SQL through a repo's `query/2` is never scoped and never checked.
""".

-include("kura.hrl").

-export([
    put_tenant/1,
    get_tenant/0,
    clear_tenant/0,
    with_tenant/2,
    without_tenant/1,
    scope/0,
    adopt/1,
    scope_query/2,
    tenant_for/1,
    require/1,
    reload_settings/0
]).

-define(REQUIRE, {?MODULE, require_tenant}).
-define(UNSCOPED, kura_tenant_unscoped).

-type strategy() :: {prefix, binary()} | {attribute, {atom(), term()}}.

-doc "What a scope runs: a fun, or a module, function and arguments.".
-type scoped(Result) :: fun(() -> Result) | {module(), atom(), [term()]}.

-doc "A process's tenant and whether it is inside `without_tenant/1`.".
-opaque scope() :: {undefined | strategy(), boolean()}.

-export_type([strategy/0, scoped/1, scope/0]).

-doc """
Set the current tenant strategy in the process dictionary.

The tenant stays for the life of the process, or until `clear_tenant/0` or
another `put_tenant/1`. That suits a process that serves one request and
ends. A process that outlives the work it was given, such as a pool worker,
a job runner or a `gen_server`, would carry the tenant into the next piece
of work: there, set the tenant with `with_tenant/2`, which ends with the
work.

Raises `{invalid_tenant, Tenant}` on anything that is not a strategy, rather
than storing something that would scope nothing. The attribute value cannot be
`is_nil` or `is_not_nil`, which a condition reads as a test rather than a value.
`undefined` is accepted and leaves the process without a tenant.
""".
-spec put_tenant(undefined | strategy()) -> undefined | strategy().
put_tenant(Tenant) ->
    ok = warn_unless_decided(validate_tenant(Tenant)),
    validate_tenant(erlang:put(kura_tenant, Tenant)).

warn_unless_decided(undefined) ->
    ok;
warn_unless_decided(_Tenant) ->
    case required() of
        unset ->
            kura_boot_warning:once(require_tenant, #{
                msg =>
                    ~"kura: tenants are in use but require_tenant is not set, so a query in a process without a tenant runs unscoped",
                fix =>
                    ~"set {kura, [{require_tenant, true}]}; that is the default in 3.0"
            });
        _ ->
            ok
    end.

-doc "Get the current tenant strategy from the process dictionary.".
-spec get_tenant() -> undefined | strategy().
get_tenant() ->
    validate_tenant(erlang:get(kura_tenant)).

-doc "Remove the current tenant strategy from the process dictionary.".
-spec clear_tenant() -> undefined | strategy().
clear_tenant() ->
    validate_tenant(erlang:erase(kura_tenant)).

-spec validate_tenant(term()) -> undefined | strategy().
validate_tenant(undefined) ->
    undefined;
validate_tenant({prefix, Prefix}) when is_binary(Prefix) ->
    {prefix, Prefix};
validate_tenant({attribute, {Field, Value}}) when
    is_atom(Field), Value =/= is_nil, Value =/= is_not_nil
->
    {attribute, {Field, Value}};
validate_tenant(Other) ->
    error({invalid_tenant, Other}).

-doc """
Run `Scoped` under `Tenant` and put the process back as it was, whether
`Scoped` returns, raises, throws or exits: the tenant it had before, or
none. A tenant set inside `Scoped` with `put_tenant/1` ends with it too.

`Scoped` is a fun or `{Module, Function, Args}`. An invalid `Tenant`
raises before anything changes.
""".
-spec with_tenant(undefined | strategy(), scoped(Result)) -> Result.
with_tenant(Tenant, Scoped) ->
    Valid = validate_tenant(Tenant),
    ok = warn_unless_decided(Valid),
    Previous = erlang:put(kura_tenant, Valid),
    try
        run(Scoped)
    after
        restore(kura_tenant, Previous)
    end.

-doc """
Run `Scoped` with no tenant, on purpose: queries and writes inside it are
not scoped, and `require_tenant` does not refuse them. The process is put
back as it was afterwards, as with `with_tenant/2`. A `with_tenant/2`
inside `Scoped` scopes as usual.

Use it for the few paths that must cross tenants. It is the only way to
run unscoped once `require_tenant` is set.
""".
-spec without_tenant(scoped(Result)) -> Result.
without_tenant(Scoped) ->
    Previous = erlang:erase(kura_tenant),
    Unscoped = erlang:put(?UNSCOPED, true),
    try
        run(Scoped)
    after
        restore(?UNSCOPED, Unscoped),
        restore(kura_tenant, Previous)
    end.

-spec run(scoped(Result)) -> Result.
run(Fun) when is_function(Fun, 0) -> Fun();
run({Module, Function, Args}) -> erlang:apply(Module, Function, Args).

%% The value is put back as it was read, valid or not: a scope neither
%% repairs nor adds to what the process held before it.
-spec restore(atom(), term()) -> ok.
restore(Key, undefined) ->
    _ = erlang:erase(Key),
    ok;
restore(Key, Previous) ->
    _ = erlang:put(Key, Previous),
    ok.

-doc """
The calling process's scope, for `adopt/1` in a process that does part of
its work, as kura's parallel preloads do. A process that is not handed the
scope has no tenant.
""".
-spec scope() -> scope().
scope() ->
    {get_tenant(), erlang:get(?UNSCOPED) =:= true}.

-doc """
Take on a scope read with `scope/0` in another process, for the rest of
this process's life.
""".
-spec adopt(scope()) -> ok.
adopt({Tenant, Unscoped}) ->
    restore(kura_tenant, validate_tenant(Tenant)),
    restore(?UNSCOPED, flag(Unscoped)).

-spec flag(boolean()) -> true | undefined.
flag(true) -> true;
flag(false) -> undefined.

-doc """
Re-read `require_tenant` from the application environment. The setting is
otherwise read once, on first use.
""".
-spec reload_settings() -> ok.
reload_settings() ->
    _ = persistent_term:erase(?REQUIRE),
    ok.

-spec required() -> boolean() | unset.
required() ->
    case persistent_term:get(?REQUIRE, unread) of
        unread ->
            Setting = setting(),
            persistent_term:put(?REQUIRE, Setting),
            Setting;
        Setting ->
            Setting
    end.

setting() ->
    case application:get_env(kura, require_tenant) of
        undefined -> unset;
        {ok, Required} when is_boolean(Required) -> Required;
        {ok, Other} -> error({kura, {invalid_require_tenant, Other}})
    end.

-doc """
The tenant to compile `Query` under: the process's tenant, or `undefined`
when it has none and none is required. Raises
`{kura, {tenant_required, Source}}` when `require_tenant` is set, the
process has no tenant and is not inside `without_tenant/1`, and the query
reads a tenant-scoped `Source`: as its `from` or through a join, in itself,
in one of its CTEs or combinations, or in a subquery in its conditions.
""".
-spec tenant_for(#kura_query{}) -> undefined | strategy().
tenant_for(Query) ->
    case get_tenant() of
        undefined ->
            ok = unless_required(Query),
            undefined;
        Tenant ->
            Tenant
    end.

-doc """
Check that a write to `Source` may run under the process's tenant. Raises
as `tenant_for/1` does.
""".
-spec require(atom() | binary()) -> ok.
require(Source) ->
    case get_tenant() of
        undefined -> unless_required(Source);
        _Tenant -> ok
    end.

unless_required(QueryOrSource) ->
    case required() =:= true andalso erlang:get(?UNSCOPED) =/= true of
        true -> check_scoped(QueryOrSource);
        false -> ok
    end.

%% A source that names one of the query's CTEs is not a table: the CTE's
%% own body is checked instead. A preload runs as a query of its own and is
%% checked there.
check_scoped(#kura_query{} = Query) ->
    check_sources([
        Source
     || {Role, Source} <- kura_query:sources(Query), Role =:= from orelse Role =:= join
    ]);
check_scoped(undefined) ->
    ok;
check_scoped(Source) ->
    check_sources([Source]).

check_sources([]) ->
    ok;
check_sources([Source | Rest]) ->
    case tenant_scoped(Source) of
        true -> error({kura, {tenant_required, Source}});
        false -> check_sources(Rest)
    end.

tenant_scoped(Source) when is_atom(Source) ->
    case kura_schema:is_schema(Source) andalso erlang:function_exported(Source, tenant_scoped, 0) of
        true -> Source:tenant_scoped() =/= false;
        false -> true
    end;
tenant_scoped(_Table) ->
    true.

-doc """
Confine `Query` to the tenant `Strategy` names.

Under `{attribute, {Field, Value}}` the condition `{Field, Value}` is added
to the query, to each CTE and combination, and to every `in` and `exists`
subquery in its `WHERE` and `HAVING` conditions, however deeply nested. The
condition names `Field` unqualified. A top-level query, CTE or union branch over
a table without it fails. An `in` or `exists` subquery over such a table does
not: the database resolves `Field` to the outer query's column, and the inner
table is not scoped. Any other strategy leaves `Query` as it is.

The condition goes through the guard of `kura_query:where/2`, so a `Field`
that is encrypted in a schema of the query raises instead of binding the
tenant value, also when it is deterministic: writes refuse an encrypted
tenant field, so reads do too.
""".
-spec scope_query(undefined | strategy(), #kura_query{}) -> #kura_query{}.
scope_query({attribute, {Field, Value}}, Query) when is_atom(Field) ->
    scoped(Query, {Field, Value});
scope_query(_Strategy, Query) ->
    Query.

-spec scoped(#kura_query{}, {atom(), term()}) -> #kura_query{}.
scoped(Query, Condition) ->
    kura_query:where_unrewritten(
        Query#kura_query{
            wheres = conditions(Query#kura_query.wheres, Condition),
            havings = conditions(Query#kura_query.havings, Condition),
            ctes = [{Name, scoped(Cte, Condition)} || {Name, Cte} <- Query#kura_query.ctes],
            combinations = [
                {Kind, scoped(Combined, Condition)}
             || {Kind, Combined} <- Query#kura_query.combinations
            ]
        },
        Condition
    ).

-spec conditions([term()], {atom(), term()}) -> [term()].
conditions(Conditions, Condition) ->
    [scoped_condition(C, Condition) || C <- Conditions].

-spec scoped_condition(term(), {atom(), term()}) -> term().
scoped_condition({Op, Conditions}, Condition) when
    (Op =:= 'and' orelse Op =:= 'or'), is_list(Conditions)
->
    {Op, conditions(Conditions, Condition)};
scoped_condition({'not', Inner}, Condition) ->
    {'not', scoped_condition(Inner, Condition)};
scoped_condition({Field, in, {subquery, #kura_query{} = Sub}}, Condition) ->
    {Field, in, {subquery, scoped(Sub, Condition)}};
scoped_condition({Exists, {subquery, #kura_query{} = Sub}}, Condition) when
    Exists =:= exists; Exists =:= not_exists
->
    {Exists, {subquery, scoped(Sub, Condition)}};
scoped_condition(Other, _Condition) ->
    Other.
