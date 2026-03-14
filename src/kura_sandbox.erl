-module(kura_sandbox).
-moduledoc """
Test sandbox that wraps each test in a rolled-back transaction.

Supports three modes:

**Single-process** (default) — the test process owns the connection:

```erlang
init_per_testcase(_TestCase, Config) ->
    kura_sandbox:checkout(my_repo),
    Config.

end_per_testcase(_TestCase, _Config) ->
    kura_sandbox:checkin(my_repo),
    ok.
```

**Allow** — let spawned processes share the sandbox connection:

```erlang
kura_sandbox:checkout(my_repo),
kura_sandbox:allow(my_repo, self(), WorkerPid).
```

**Shared** — all processes use a single sandbox connection (simplest
for tests that spawn many processes):

```erlang
kura_sandbox:checkout(my_repo, #{shared => true}).
```

All database operations within the sandbox are rolled back on checkin.
No test data persists.
""".

-include("kura.hrl").

-export([
    start/0,
    checkout/1,
    checkout/2,
    checkin/1,
    allow/3,
    get_conn/1
]).

-define(TABLE, kura_sandbox_registry).

-doc "Create the sandbox ETS table. Call once before tests run.".
-spec start() -> ok.
start() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ?TABLE = ets:new(?TABLE, [named_table, public, set]),
            ok;
        _Tid ->
            ok
    end.

-doc "Check out a sandbox connection with default options.".
-spec checkout(module()) -> ok.
checkout(RepoMod) ->
    checkout(RepoMod, #{}).

-doc """
Check out a sandbox connection.

Options:
- `shared` — when `true`, all processes use this connection (default: `false`)
""".
-spec checkout(module(), map()) -> ok.
checkout(RepoMod, Opts) ->
    ensure_started(),
    Pool = get_pool(RepoMod),
    {ok, Ref, Conn} = pgo:checkout(Pool),
    Self = self(),

    case maps:get(shared, Opts, false) of
        true ->
            ets:insert(?TABLE, {{Pool, shared}, {Ref, Conn, Self}});
        false ->
            ets:insert(?TABLE, {{Pool, Self}, {Ref, Conn}})
    end,

    %% Set process dict so pgo:query in this process uses the connection
    erlang:put(pgo_transaction_connection, Conn),
    pgo:query(<<"BEGIN">>, [], #{pool => Pool}),
    ok.

-doc "Roll back the sandbox transaction and return the connection to the pool.".
-spec checkin(module()) -> ok.
checkin(RepoMod) ->
    Pool = get_pool(RepoMod),
    Self = self(),

    {Ref, Conn} =
        case ets:lookup(?TABLE, {Pool, Self}) of
            [{_, {R, C}}] ->
                ets:delete(?TABLE, {Pool, Self}),
                {R, C};
            [] ->
                case ets:lookup(?TABLE, {Pool, shared}) of
                    [{_, {R, C, Self}}] ->
                        ets:delete(?TABLE, {Pool, shared}),
                        {R, C};
                    _ ->
                        {undefined, undefined}
                end
        end,

    case Conn of
        undefined ->
            ok;
        _ ->
            pgo:query(<<"ROLLBACK">>, [], #{pool => Pool}),
            erlang:erase(pgo_transaction_connection),
            pgo:checkin(Ref, Conn),
            %% Clean up any allowances for this owner
            ets:match_delete(?TABLE, {{Pool, '_', allowed}, Self}),
            ok
    end.

-doc """
Allow `ChildPid` to use `OwnerPid`'s sandbox connection.

The child process must call this before making any database queries.
""".
-spec allow(module(), pid(), pid()) -> ok.
allow(RepoMod, OwnerPid, ChildPid) ->
    Pool = get_pool(RepoMod),
    ets:insert(?TABLE, {{Pool, ChildPid, allowed}, OwnerPid}),
    ok.

-doc """
Look up a sandbox connection for the current process and pool.

Returns `{ok, Conn}` if a sandbox connection exists (owned, allowed,
or shared), `not_found` otherwise. Called from `kura_repo_worker`.
""".
-spec get_conn(atom()) -> {ok, term()} | not_found.
get_conn(Pool) ->
    case ets:whereis(?TABLE) of
        undefined -> not_found;
        _Tid -> lookup_conn(Pool)
    end.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

lookup_conn(Pool) ->
    Self = self(),
    %% 1. Direct owner
    case ets:lookup(?TABLE, {Pool, Self}) of
        [{_, {_Ref, Conn}}] ->
            {ok, Conn};
        [] ->
            %% 2. Allowed by an owner
            case ets:lookup(?TABLE, {Pool, Self, allowed}) of
                [{_, OwnerPid}] ->
                    case ets:lookup(?TABLE, {Pool, OwnerPid}) of
                        [{_, {_Ref, Conn}}] -> {ok, Conn};
                        [] -> not_found
                    end;
                [] ->
                    %% 3. Shared mode
                    case ets:lookup(?TABLE, {Pool, shared}) of
                        [{_, {_Ref, Conn, _Owner}}] -> {ok, Conn};
                        [] -> not_found
                    end
            end
    end.

ensure_started() ->
    case ets:whereis(?TABLE) of
        undefined -> start();
        _ -> ok
    end.

get_pool(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    maps:get(pool, Config, RepoMod).
