-module(kura_prefix).
-moduledoc """
Process-scoped schema prefix for multi-tenancy.

Set a prefix once per request (e.g. in a Nova pre_request plugin),
and all Kura operations in that process automatically use it.

```erlang
%% In your tenant plugin:
kura_prefix:put(<<"tenant_abc">>).

%% All queries now target tenant_abc.players, tenant_abc.wallets, etc.
{ok, Players} = my_repo:all(kura_query:from(player)).

%% Clear when done (optional — process exit clears it automatically):
kura_prefix:delete().
```

Explicit `kura_query:prefix/2` always takes precedence over the
process prefix. If both are set, the query-level prefix wins.
""".

-export([put/1, get/0, delete/0]).

-define(KEY, kura_schema_prefix).

-doc "Set the schema prefix for the current process.".
-spec put(binary()) -> ok.
put(Prefix) when is_binary(Prefix) ->
    erlang:put(?KEY, Prefix),
    ok.

-doc "Get the schema prefix for the current process, or `undefined` if not set.".
-spec get() -> binary() | undefined.
get() ->
    case erlang:get(?KEY) of
        V when is_binary(V) -> V;
        _ -> undefined
    end.

-doc "Remove the schema prefix for the current process.".
-spec delete() -> ok.
delete() ->
    erlang:erase(?KEY),
    ok.
