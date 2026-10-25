-module(kura_sandbox).
-moduledoc """
Test sandbox that wraps each test in a rolled-back transaction.

Supports three modes:

**Single-process** (default) - the test process owns the connection:

```erlang
init_per_testcase(_TestCase, Config) ->
    kura_sandbox:checkout(my_repo),
    Config.

end_per_testcase(_TestCase, _Config) ->
    kura_sandbox:checkin(my_repo),
    ok.
```

**Allow** - let spawned processes share the sandbox connection:

```erlang
kura_sandbox:checkout(my_repo),
kura_sandbox:allow(my_repo, self(), WorkerPid).
```

**Shared** - all processes use a single sandbox connection (simplest
for tests that spawn many processes):

```erlang
kura_sandbox:checkout(my_repo, #{shared => true}).
```

All database operations within the sandbox are rolled back on checkin.
No test data persists.

A `kura_repo_worker:transaction/2` inside the sandbox runs under a
`SAVEPOINT` on the sandbox connection, as a nested transaction does
outside it: a raise undoes only that transaction's work, and a statement
that fails inside it leaves the sandbox usable. Processes that share one
sandbox connection (`allow/3` or `shared`) share its savepoints too, so
their transactions must run one after another: a transaction opened
while another process has one open on the same connection raises
`{kura_sandbox, overlapping_transactions}`.
""".

-export([
    start/0,
    checkout/1,
    checkout/2,
    checkin/1,
    allow/3,
    get_conn/1,
    enter_transaction/1,
    leave_transaction/1
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
- `shared` - when `true`, all processes use this connection (default: `false`)
""".
-spec checkout(module(), map()) -> ok.
checkout(RepoMod, Opts) ->
    ensure_started(),
    Pool = get_pool(RepoMod),
    PoolMod = kura_db:get_pool_module(RepoMod),
    DriverMod = kura_db:get_driver_module(RepoMod),
    {ok, Conn, Token} = PoolMod:checkout(Pool, #{}),
    Self = self(),

    case maps:get(shared, Opts, false) of
        true ->
            ets:insert(?TABLE, {{Pool, shared}, {PoolMod, Token, Conn, Self}});
        false ->
            ets:insert(?TABLE, {{Pool, Self}, {PoolMod, Token, Conn}})
    end,

    _ = DriverMod:query_on(Conn, ~"BEGIN", [], #{}),
    ok.

-doc "Roll back the sandbox transaction and return the connection to the pool.".
-spec checkin(module()) -> ok.
checkin(RepoMod) ->
    Pool = get_pool(RepoMod),
    DriverMod = kura_db:get_driver_module(RepoMod),
    Self = self(),

    Lookup =
        case ets:lookup(?TABLE, {Pool, Self}) of
            [{_, {Pm, Tk, Cn}}] ->
                ets:delete(?TABLE, {Pool, Self}),
                {Pm, Tk, Cn};
            [] ->
                case ets:lookup(?TABLE, {Pool, shared}) of
                    [{_, {Pm, Tk, Cn, Self}}] ->
                        ets:delete(?TABLE, {Pool, shared}),
                        {Pm, Tk, Cn};
                    _ ->
                        undefined
                end
        end,

    case Lookup of
        undefined ->
            ok;
        {PoolMod, Token, Conn} ->
            _ = DriverMod:query_on(Conn, ~"ROLLBACK", [], #{}),
            ok = PoolMod:checkin(Pool, Token),
            ets:match_delete(?TABLE, {{Pool, '_', allowed}, Self}),
            ets:delete(?TABLE, {transaction, Conn}),
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

-doc """
Note that this process opens a transaction on sandbox connection `Conn`.

Called by `kura_db:transaction/2`. A process may nest its own
transactions. Raises `{kura_sandbox, overlapping_transactions}` when
another live process has one open on `Conn`, since their savepoints
would interleave.
""".
-spec enter_transaction(kura_pool:conn()) -> ok.
enter_transaction(Conn) ->
    Self = self(),
    Key = {transaction, Conn},
    case ets:insert_new(?TABLE, {Key, Self, 1}) of
        true ->
            ok;
        false ->
            case ets:lookup(?TABLE, Key) of
                [{_, Self, _Depth}] ->
                    _ = ets:update_counter(?TABLE, Key, {3, 1}),
                    ok;
                [{_, Holder, _Depth} = Held] when is_pid(Holder) ->
                    case is_process_alive(Holder) of
                        true ->
                            error({kura_sandbox, overlapping_transactions});
                        false ->
                            _ = ets:delete_object(?TABLE, Held),
                            enter_transaction(Conn)
                    end;
                [] ->
                    enter_transaction(Conn)
            end
    end.

-doc "Note that this process closed a transaction it opened on `Conn`.".
-spec leave_transaction(kura_pool:conn()) -> ok.
leave_transaction(Conn) ->
    Self = self(),
    Key = {transaction, Conn},
    case ets:lookup(?TABLE, Key) of
        [{_, Self, 1} = Held] ->
            _ = ets:delete_object(?TABLE, Held),
            ok;
        [{_, Self, _Depth}] ->
            _ = ets:update_counter(?TABLE, Key, {3, -1}),
            ok;
        _NotHeld ->
            ok
    end.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

lookup_conn(Pool) ->
    Self = self(),
    %% 1. Direct owner
    case ets:lookup(?TABLE, {Pool, Self}) of
        [{_, {_PoolMod, _Token, Conn}}] ->
            {ok, Conn};
        [] ->
            %% 2. Allowed by an owner
            case ets:lookup(?TABLE, {Pool, Self, allowed}) of
                [{_, OwnerPid}] ->
                    case ets:lookup(?TABLE, {Pool, OwnerPid}) of
                        [{_, {_PoolMod, _Token, Conn}}] -> {ok, Conn};
                        [] -> not_found
                    end;
                [] ->
                    %% 3. Shared mode
                    case ets:lookup(?TABLE, {Pool, shared}) of
                        [{_, {_PoolMod, _Token, Conn, _Owner}}] -> {ok, Conn};
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
