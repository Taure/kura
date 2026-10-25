-module(kura_driver).
-moduledoc """
Pluggable database driver behaviour.

A `kura_driver` implementation is the seam between kura's portable
query layer and a specific database client library. Each impl wraps
one client (`minato`, `esqlite`, `mysql_otp`, ...) behind a uniform
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

`transaction/4` wraps a fun in a transaction. The driver chooses how
to thread connection context into queries inside the fun (process
dict, explicit arg, etc.); kura code inside the fun calls
`kura_db:query/3` as usual and the driver's `query/5` impl observes
the transaction context.

## Today

Two drivers ship as separate packages: `kura_driver_minato` in
`kura_postgres` (PostgreSQL via minato) and `kura_driver_sqlite` in
`kura_sqlite` (SQLite via esqlite). Both keep the in-flight transaction
in the process dictionary, so code that does
`kura_db:transaction(Repo, fun() -> kura_repo_worker:insert(...) end)` finds
the transaction's connection without being handed it.
""".

-export_type([sql/0, params/0, opts/0, query_result/0, transaction_failure/0, transaction_error/0]).

-doc "Query SQL string. Driver impls accept iodata.".
-type sql() :: iodata().

-doc "Parameter list in the order placeholders appear in the SQL.".
-type params() :: [term()].

-doc """
Driver-specific options. The minato driver honors `decode_opts` and
`timeout`, which is a deadline after which the statement is cancelled
on the server. Other drivers may ignore unknown keys or define their
own.
""".
-type opts() :: map().

-doc """
Driver-specific result shape. Today this is whatever the underlying
client returns (`#{rows := [...], num_rows := N, ...}` for both
shipped drivers). A future kura layer may normalize this; the driver
behaviour does not prescribe a shape.
""".
-type query_result() :: dynamic().

-doc """
Why a transaction could not finish, as raised in
`{DriverMod, {Failure, Reason}}`. See `c:transaction/4`.
""".
-type transaction_failure() ::
    begin_failed
    | savepoint_failed
    | release_failed
    | rollback_to_failed
    | commit_failed
    | commit_unknown
    | no_connection.

-doc "What `c:transaction/4` raises when the transaction cannot finish.".
-type transaction_error() ::
    {module(), {transaction_failure(), term()}}
    | {module(), transaction_ended_by_statement}
    | transaction_rolled_back.

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
Wrap `Fun` in a database transaction over a pool, and return what `Fun`
returns once the transaction has committed.

Queries inside `Fun` route to the transaction's connection by whatever
convention the driver uses (both shipped drivers: the process
dictionary). A process spawned from `Fun` is outside the transaction.

## Outermost and nested

Called while the same process is already inside a transaction on the
same pool, the driver runs `Fun` on that transaction's connection under
a `SAVEPOINT` rather than checking out another one. A normal return
releases the savepoint; an exception rolls back to it and is re-raised,
leaving the work before it in place. Nested work is therefore kept only
if the outermost transaction commits.

A driver must not flatten a nested call into the outer transaction,
running `Fun` as if it were part of it: a raise inside `Fun` would then
leave its writes in place. A driver that cannot run a nested call under
a savepoint raises `{DriverMod, {savepoint_failed, unsupported}}`
without running `Fun`.

An exception raised by `Fun` at the outermost level rolls the
transaction back and is re-raised unchanged. A `{error, _}` returned by
`Fun` is a normal return and commits; callers that want it to roll back
throw instead.

## Failures

A transaction that cannot finish raises, so a failure is never mistaken
for a value `Fun` returned. A driver raises:

- `error({DriverMod, {Failure, Reason}})` with a `t:transaction_failure/0`:
  - `begin_failed` / `savepoint_failed`: the transaction or savepoint
    did not start, and `Fun` did not run.
  - `release_failed`: a nested savepoint could not be released, and its
    work was rolled back, unless a statement had already ended the
    transaction (see below).
  - `rollback_to_failed`: a nested `Fun` raised and rolling back to its
    savepoint failed, so the driver rolled the whole transaction back.
  - `commit_failed`: the database refused the commit, and nothing the
    transaction wrote was kept.
  - `commit_unknown`: no definite answer to the commit came back (a
    timeout, a lost connection, an I/O error), so it may or may not have
    been kept.
  - `no_connection`: no connection could be checked out, and `Fun` did
    not run.
- `error(transaction_rolled_back)` when the database rolled the work
  back itself because a statement inside `Fun` failed. At the outermost
  level nothing was kept. Nested, it means the savepoint's work was
  undone; on PostgreSQL the outer transaction is still open and may
  catch it and carry on. SQLite ends the whole transaction on some
  errors (`ON CONFLICT ROLLBACK`, a full disk, an I/O error), so there a
  nested `transaction_rolled_back` can mean the outermost transaction is
  gone, and every statement after it raises the same.
- `error({DriverMod, transaction_ended_by_statement})` when a statement
  run inside `Fun` ended the transaction itself (see below).

A driver may emit a subset of these, and the set may grow in a minor
version, so a caller that matches on `Failure` keeps a clause for one it
does not know. `Reason` is opaque and
driver-specific; match on `Failure`, not on `Reason`. A driver must not
put row values in it, so a server error is reduced to its code and
constraint rather than its message or detail.

kura core does not rewrite these. The one place core runs a transaction
itself is a sandboxed test (`m:kura_sandbox`): there `Fun` runs under a
savepoint on the sandbox connection, and a savepoint that will not start
or release raises `{DriverMod, {savepoint_failed, _}}` or
`transaction_rolled_back` the same way.

## Transaction control in user SQL

Do not run `COMMIT`, `END`, `ROLLBACK` or `RELEASE` through a query
inside `Fun`. kura core passes the statement to the driver unchanged and
does not detect it. Whatever the statement committed stays committed,
and the statements after it are no longer in the transaction. A driver
should raise `{DriverMod, transaction_ended_by_statement}` once it
notices: `kura_driver_sqlite` checks after every statement. A driver
that cannot tell may let the outermost transaction return normally, with
the work after the statement run in autocommit, and a nested one fail
its `RELEASE`.

## Options

`Opts` carries driver-specific options; the minato driver takes none
here, because a statement's deadline is set per query rather than per
transaction. Drivers ignore keys they do not know.
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
