-module(kura_tenant).
-moduledoc ~"""
Process dictionary-based multitenancy. Stores the current tenant strategy
(schema prefix or attribute filter) in the process dictionary for use by
Kura repo operations.

Under the attribute strategy every query kura compiles is passed through
`scope_query/2`, which adds the tenant condition to the query and to every
query nested in it. Under the prefix strategy the dialect qualifies each
table with the schema when it emits SQL.

## Requiring a tenant

The tenant lives in the process dictionary, so a process that never set
one has none: a spawned process does not inherit its parent's, and a
query run there is not scoped at all. Set

```erlang
{kura, [{require_tenant, true}]}
```

and a query or a changeset write with no tenant in the process raises
`{kura, {tenant_required, Source}}` instead of running unscoped, `Source`
being the schema or table it reads or writes. Two things lift the
requirement:

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
    scope_query/2,
    tenant_for/1,
    require/1,
    reload_settings/0
]).

-define(REQUIRE, {?MODULE, require_tenant}).
-define(UNSCOPED, kura_tenant_unscoped).

-type strategy() :: {prefix, binary()} | {attribute, {atom(), term()}}.
-export_type([strategy/0]).

-doc """
Set the current tenant strategy in the process dictionary.

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

-doc "Execute a function with a temporary tenant, restoring the previous tenant after.".
-spec with_tenant(strategy(), fun(() -> T)) -> T.
with_tenant(Tenant, Fun) ->
    Old = put_tenant(Tenant),
    try
        Fun()
    after
        case Old of
            undefined -> clear_tenant();
            _ -> put_tenant(Old)
        end
    end.

-doc """
Run `Fun` with no tenant, on purpose: queries and writes inside it are not
scoped, and `require_tenant` does not refuse them. The tenant the process
had is restored afterwards. A `with_tenant/2` inside `Fun` scopes as usual.

Use it for the few paths that must cross tenants. It is the only way to
run unscoped once `require_tenant` is set.
""".
-spec without_tenant(fun(() -> T)) -> T.
without_tenant(Fun) ->
    Tenant = clear_tenant(),
    Unscoped = erlang:put(?UNSCOPED, true),
    try
        Fun()
    after
        _ =
            case Unscoped of
                true -> ok;
                _ -> erlang:erase(?UNSCOPED)
            end,
        case Tenant of
            undefined -> clear_tenant();
            _ -> put_tenant(Tenant)
        end
    end.

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
process has no tenant and is not inside `without_tenant/1`, and the query,
one of its CTEs or combinations, or a subquery in its conditions reads a
tenant-scoped `Source`.
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
        true -> check_scoped(QueryOrSource, []);
        false -> ok
    end.

%% A source that names one of the query's CTEs is not a table: the CTE's
%% own body is checked instead.
check_scoped(#kura_query{from = From, ctes = Ctes} = Query, OuterNames) ->
    Names = [Name || {Name, _Body} <- Ctes] ++ OuterNames,
    ok = check_source(From, Names),
    ok = check_bodies(Ctes, Names),
    ok = check_bodies(Query#kura_query.combinations, Names),
    ok = check_conditions(Query#kura_query.wheres, Names),
    check_conditions(Query#kura_query.havings, Names);
check_scoped(Source, Names) ->
    check_source(Source, Names).

check_source(undefined, _Names) ->
    ok;
check_source(Source, Names) ->
    case names_cte(Source, Names) orelse not tenant_scoped(Source) of
        true -> ok;
        false -> error({kura, {tenant_required, Source}})
    end.

names_cte(Source, Names) when is_binary(Source) -> lists:member(Source, Names);
names_cte(Source, Names) when is_atom(Source) -> lists:member(atom_to_binary(Source), Names);
names_cte(_Source, _Names) -> false.

tenant_scoped(Source) when is_atom(Source) ->
    case kura_schema:is_schema(Source) andalso erlang:function_exported(Source, tenant_scoped, 0) of
        true -> Source:tenant_scoped() =/= false;
        false -> true
    end;
tenant_scoped(_Table) ->
    true.

check_bodies([{_NameOrKind, #kura_query{} = Body} | Rest], Names) ->
    ok = check_scoped(Body, Names),
    check_bodies(Rest, Names);
check_bodies([_ | Rest], Names) ->
    check_bodies(Rest, Names);
check_bodies(_, _Names) ->
    ok.

check_conditions([Condition | Rest], Names) ->
    ok = check_condition(Condition, Names),
    check_conditions(Rest, Names);
check_conditions(_, _Names) ->
    ok.

check_condition({Op, Conditions}, Names) when Op =:= 'and'; Op =:= 'or' ->
    check_conditions(Conditions, Names);
check_condition({'not', Inner}, Names) ->
    check_condition(Inner, Names);
check_condition({_Field, in, {subquery, #kura_query{} = Sub}}, Names) ->
    check_scoped(Sub, Names);
check_condition({Exists, {subquery, #kura_query{} = Sub}}, Names) when
    Exists =:= exists; Exists =:= not_exists
->
    check_scoped(Sub, Names);
check_condition(_Other, _Names) ->
    ok.

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
