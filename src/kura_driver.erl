-module(kura_driver).
-moduledoc """
Pluggable database driver behaviour.

A `kura_driver` implementation is the seam between kura's portable
query layer and a specific database client library. Each impl wraps
one client (`pgo`, `esqlite`, `mysql_otp`, ...) behind a uniform
query/transaction surface.

The pool layer (`kura_pool`) hands out a connection. The driver layer
runs SQL on it. Splitting them lets a single pool impl (e.g. an
in-memory test pool) be paired with multiple drivers, and lets a
driver be swapped without touching pool semantics.

## Three call shapes

`query/5` runs SQL on a pool. The driver picks (or already has) a
connection. Used by the common case where the caller does not own a
checkout.

`query_on/4` runs SQL on a specific connection the caller already
holds. Used by the sandbox path: the test process owns a checked-out
conn and wants every query routed to it.

`transaction/3` wraps a fun in a transaction. The driver chooses how
to thread connection context into queries inside the fun (process
dict, explicit arg, etc.); kura code inside the fun calls
`kura_db:query/3` as usual and the driver's `query/5` impl observes
the transaction context.

## Today

The canonical impl is `kura_driver_pgo`. It uses pgo's process-dict
based transaction context (`pgo_transaction_connection`) so existing
code that does `kura_db:transaction(Repo, fun() -> kura_repo:insert(...) end)`
continues to work transparently.
""".

-export_type([sql/0, params/0, opts/0, query_result/0]).

-doc "Query SQL string. Driver impls accept iodata.".
-type sql() :: iodata().

-doc "Parameter list in the order placeholders appear in the SQL.".
-type params() :: [term()].

-doc """
Driver-specific options. The pgo driver honors `decode_opts`. Other
drivers may ignore unknown keys or define their own.
""".
-type opts() :: map().

-doc """
Driver-specific result shape. Today this is whatever the underlying
client returns (`#{rows := [...], num_rows := N, ...}` for pgo). A
future kura layer may normalize this; the driver behaviour does not
prescribe a shape.
""".
-type query_result() :: dynamic().

-doc """
Run SQL on a pool. The driver leases a connection (via `PoolMod`)
or threads through an active transaction context, runs the query,
and returns the result.
""".
-callback query(PoolMod, Pool, SQL, Params, Opts) -> query_result() when
    PoolMod :: module(),
    Pool :: kura_pool:name(),
    SQL :: sql(),
    Params :: params(),
    Opts :: opts().

-doc """
Run SQL on a specific connection the caller already owns. Used by
the sandbox path where a test process holds a checked-out conn for
its lifetime.
""".
-callback query_on(Conn, SQL, Params, Opts) -> query_result() when
    Conn :: kura_pool:conn(),
    SQL :: sql(),
    Params :: params(),
    Opts :: opts().

-doc """
Wrap `Fun` in a database transaction over a pool. Queries inside
`Fun` route to the transaction's connection by whatever convention
the driver uses (pgo: process dict).

`Opts` carries driver-specific options. The pgo driver honors
`pool_options => [{timeout, infinity | non_neg_integer()}]` for
long-running streams. Other drivers may ignore unknown keys.
""".
-callback transaction(PoolMod, Pool, Fun, Opts) -> term() when
    PoolMod :: module(),
    Pool :: kura_pool:name(),
    Fun :: fun(() -> term()),
    Opts :: opts().

-doc "Optional. Create the database if missing. Called by `kura_migrator`.".
-callback ensure_database(Config :: map()) -> ok | {error, term()}.

-doc "Optional. Round-trip a trivial query against `Pool` (e.g. SELECT 1).".
-callback probe_pool(Pool :: kura_pool:name()) -> ok | {error, term()}.

-optional_callbacks([ensure_database/1, probe_pool/1]).
