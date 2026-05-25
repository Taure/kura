-module(kura_pool).
-moduledoc """
Pluggable connection-pool behaviour.

Each implementation manages a set of connections to a backend (Postgres,
MySQL, SQLite, an HTTP client, etc.) and exposes a uniform
checkout/checkin/give_away surface to `kura_db` and friends.

Different drivers want different pool shapes:

- Network-protocol drivers like Postgres benefit from caller-side I/O on
  the socket, so the pool just hands out a connection record and stays
  out of the way (`kura_pool_pgo`, the canonical PG impl).
- gen_server-style drivers like MySQL or HTTP clients want a worker
  process the caller calls into; a gen_server-call-style pool fits.

This behaviour is the contract; implementations pick the shape that
matches their backend.

## Token vs connection

`checkout/2` returns both a `conn()` and a `token()`. The `conn()` is
what callers actually use to run work (e.g. a `pgo_pool:conn()` record
that `pgo_handler:extended_query/4` accepts). The `token()` is the
opaque value the caller hands back to `checkin/2`. Implementations are
free to make them the same value if they like.

`give_away/3` transfers ownership of an in-flight checkout to another
process. The new owner becomes responsible for calling `checkin/2`.
Used by sandbox test patterns: a setup process checks out a connection,
transfers it to the test process for the duration of the test, and the
teardown checks it back in.

## Example

```erlang
{ok, _Pid} = kura_pool_pgo:start_pool(my_pool, #{
    host => "localhost",
    database => "my_app",
    user => "postgres",
    password => "secret",
    pool_size => 10
}),

kura_pool:with_conn(kura_pool_pgo, my_pool, fun(Conn) ->
    pgo_handler:extended_query(Conn, ~"SELECT 1", [])
end).
```
""".

-export([with_conn/3, with_conn/4]).

-export_type([name/0, opts/0, checkout_opts/0, conn/0, token/0]).

-doc "Pool registration name. Implementation chooses the registry semantics.".
-type name() :: atom().

-doc "Implementation-specific pool options. Pass through to `start_pool/2`.".
-type opts() :: map().

-doc "Implementation-specific options for a single checkout (e.g. `#{timeout => 5000}`).".
-type checkout_opts() :: map().

-doc "Opaque per-impl connection handle the caller uses to run work.".
-type conn() :: dynamic().

-doc "Opaque per-impl token handed back to `checkin/2`.".
-type token() :: dynamic().

-doc "Start a pool registered under `Name`.".
-callback start_pool(name(), opts()) -> {ok, pid()} | {error, term()}.

-doc "Stop a previously-started pool.".
-callback stop_pool(name()) -> ok.

-doc "Lease a connection from the pool. Returns the conn and a checkin token.".
-callback checkout(name(), checkout_opts()) ->
    {ok, conn(), token()} | {error, term()}.

-doc "Return a leased connection to the pool.".
-callback checkin(name(), token()) -> ok.

-doc """
Transfer ownership of a checked-out connection to another process.

The new owner becomes responsible for calling `checkin/2`. `GiftData` is
delivered as a message to `NewOwner` if the underlying pool supports
it; otherwise it is silently dropped.
""".
-callback give_away(token(), pid(), term()) -> ok | {error, term()}.

-doc "Optional. Resize a running pool to `Size` connections.".
-callback resize(name(), pos_integer()) -> ok | {error, term()}.

-optional_callbacks([resize/2]).

-doc "Equivalent to `with_conn(Mod, Name, Fun, #{})`.".
-spec with_conn(module(), name(), fun((conn()) -> Result)) -> Result | {error, term()}.
with_conn(Mod, Name, Fun) ->
    with_conn(Mod, Name, Fun, #{}).

-doc """
Lease a connection, run `Fun(Conn)`, and check it back in.

Equivalent to:
```erlang
case Mod:checkout(Name, Opts) of
    {ok, Conn, Token} ->
        try Fun(Conn) after Mod:checkin(Name, Token) end;
    {error, _} = Err ->
        Err
end.
```

Returns `{error, Reason}` if checkout fails, otherwise the value of `Fun`.
""".
-spec with_conn(module(), name(), fun((conn()) -> Result), checkout_opts()) ->
    Result | {error, term()}.
with_conn(Mod, Name, Fun, Opts) ->
    case Mod:checkout(Name, Opts) of
        {ok, Conn, Token} ->
            try
                Fun(Conn)
            after
                Mod:checkin(Name, Token)
            end;
        {error, _} = Err ->
            Err
    end.
