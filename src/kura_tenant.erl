-module(kura_tenant).
-moduledoc ~"""
Process dictionary-based multitenancy. Stores the current tenant strategy
(schema prefix or attribute filter) in the process dictionary for use by
Kura repo operations.

Under the attribute strategy every query kura compiles is passed through
`scope_query/2`, which adds the tenant condition to the query and to every
query nested in it. Under the prefix strategy the dialect qualifies each
table with the schema when it emits SQL.
""".

-include("kura.hrl").

-export([
    put_tenant/1,
    get_tenant/0,
    clear_tenant/0,
    with_tenant/2,
    scope_query/2
]).

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
    _ = validate_tenant(Tenant),
    validate_tenant(erlang:put(kura_tenant, Tenant)).

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
-spec with_tenant(strategy(), fun(() -> T)) -> T when T :: term().
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
Confine `Query` to the tenant `Strategy` names.

Under `{attribute, {Field, Value}}` the condition `{Field, Value}` is added
to the query, to each CTE and combination, and to every `in` and `exists`
subquery in its `WHERE` and `HAVING` conditions, however deeply nested. The
condition names `Field` unqualified. A top-level query, CTE or union branch over
a table without it fails. An `in` or `exists` subquery over such a table does
not: the database resolves `Field` to the outer query's column, and the inner
table is not scoped. Any other strategy leaves `Query` as it is.
""".
-spec scope_query(undefined | strategy(), #kura_query{}) -> #kura_query{}.
scope_query({attribute, {Field, Value}}, Query) when is_atom(Field) ->
    scoped(Query, {Field, Value});
scope_query(_Strategy, Query) ->
    Query.

-spec scoped(#kura_query{}, {atom(), term()}) -> #kura_query{}.
scoped(Query, Condition) ->
    Query#kura_query{
        wheres = conditions(Query#kura_query.wheres, Condition) ++ [Condition],
        havings = conditions(Query#kura_query.havings, Condition),
        ctes = [{Name, scoped(Cte, Condition)} || {Name, Cte} <- Query#kura_query.ctes],
        combinations = [
            {Kind, scoped(Combined, Condition)}
         || {Kind, Combined} <- Query#kura_query.combinations
        ]
    }.

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
