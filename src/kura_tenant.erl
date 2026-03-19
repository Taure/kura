-module(kura_tenant).
-moduledoc ~"""
Process dictionary-based multitenancy. Stores the current tenant strategy
(schema prefix or attribute filter) in the process dictionary for use by
Kura repo operations.
""".

-export([
    put_tenant/1,
    get_tenant/0,
    clear_tenant/0,
    with_tenant/2
]).

-type strategy() :: {prefix, binary()} | {attribute, {atom(), term()}}.

-doc "Set the current tenant strategy in the process dictionary.".
-spec put_tenant(strategy()) -> undefined | strategy().
put_tenant(Tenant) ->
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
validate_tenant(undefined) -> undefined;
validate_tenant({prefix, P}) when is_binary(P) -> {prefix, P};
validate_tenant({attribute, {F, _V}} = A) when is_atom(F) -> A;
validate_tenant(_) -> undefined.

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
