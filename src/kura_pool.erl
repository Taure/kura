-module(kura_pool).
-moduledoc """
Pluggable connection pool behaviour for kura.

A pool implementation manages a set of database connection workers
(such as `kura_pg_conn`). The default implementation, `kura_pool_hnc`,
wraps [hnc](https://github.com/hnc-agency/hnc).

Users can plug in their own pool by adopting this behaviour, e.g. to
integrate an existing `poolboy` setup or Elixir's `db_connection`.

## Required callbacks

* `start_pool/2`, `stop_pool/1` lifecycle.
* `checkout/2`, `checkin/2` for process-bound conn lease.

## Optional callbacks

* `give_away/3` transfers ownership of a checked-out conn to another process.
  Required for sandbox mode.
* `resize/2` adjusts pool size at runtime.

SQL transaction semantics (`BEGIN`/`COMMIT`/`ROLLBACK`) belong to the adapter
layer (`kura_adapter_postgres`, `kura_adapter_sqlite`, ...), not the pool.
Use `with_conn/3,4` to lease a connection for the duration of a transaction.
""".

-export([with_conn/3, with_conn/4]).

-export_type([name/0, pool_opts/0, checkout_opts/0, token/0]).

-type name() :: atom().
-type pool_opts() :: map().
-type checkout_opts() :: map().
-type token() :: dynamic().

-callback start_pool(name(), pool_opts()) -> {ok, pid()} | {error, term()}.
-callback stop_pool(name()) -> ok.
-callback checkout(name(), checkout_opts()) ->
    {ok, Conn :: pid(), token()} | {error, term()}.
-callback checkin(name(), token()) -> ok.
-callback give_away(token(), pid(), term()) -> ok | {error, term()}.
-callback resize(name(), pos_integer()) -> ok | {error, term()}.

-optional_callbacks([give_away/3, resize/2]).

-doc """
Run `Fun` against a checked-out connection, releasing it on completion.

Equivalent to `with_conn(Mod, Name, Fun, #{})`.
""".
-spec with_conn(module(), name(), fun((pid()) -> Result)) -> Result | {error, term()}.
with_conn(Mod, Name, Fun) ->
    with_conn(Mod, Name, Fun, #{}).

-doc """
Run `Fun` against a checked-out connection with `Opts` (e.g. `#{timeout => 5000}`),
releasing the connection on completion. Returns `{error, Reason}` if checkout fails.
""".
-spec with_conn(module(), name(), fun((pid()) -> Result), checkout_opts()) ->
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
