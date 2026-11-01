# Migrations

Kura migrations provide version-tracked DDL operations for managing your database schema.

## Creating a Migration

Migration modules must:
- Be named `m<YYYYMMDDHHMMSS>_<descriptive_name>`
- Implement the `kura_migration` behaviour
- Export `up/0` and `down/0` returning a list of operations

```erlang
-module(m20250101120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"users", [
        #kura_column{name = id, type = id, primary_key = true},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
    ]}].

down() ->
    [{drop_table, ~"users"}].
```

Place migration files in `src/migrations/` (or any subdirectory under `src/`). Kura automatically discovers them by scanning the application's compiled modules for names matching the `m<YYYYMMDDHHMMSS>_<name>` pattern - no configuration needed.

Since migrations are regular `.erl` files in `src/`, they are compiled normally by rebar3. If you use `{src_dirs, [{"src", [{recursive, true}]}]}` in your `rebar.config`, subdirectories like `src/migrations/` are included automatically.

## DDL Operations

### Create Table

```erlang
{create_table, ~"table_name", [
    #kura_column{name = id, type = id, primary_key = true},
    #kura_column{name = name, type = string, nullable = false},
    #kura_column{name = score, type = integer, default = 0},
    #kura_column{name = active, type = boolean, default = true}
]}.
```

Column options:
- `primary_key` - `true | false` (default: `false`)
- `nullable` - `true | false` (default: `true`)
- `default` - literal value (integer, float, binary, boolean) or `undefined` for none

A four-element `create_table` takes a list of table-level constraints as
its last argument. Use `{primary_key, Cols}` for a composite primary key
(rather than an inline `primary_key = true` column):

```erlang
{create_table, ~"memberships", [
    #kura_column{name = org_id, type = uuid, nullable = false},
    #kura_column{name = user_id, type = uuid, nullable = false},
    #kura_column{name = role, type = string}
], [{primary_key, [org_id, user_id]}]}.
```

emits `PRIMARY KEY ("org_id", "user_id")`. The other table constraints are
`{unique, Cols}` and `{check, SqlBinary}`.

### Drop Table

```erlang
{drop_table, ~"table_name"}.
```

### Alter Table

```erlang
{alter_table, ~"users", [
    {add_column, #kura_column{name = bio, type = text}},
    {drop_column, old_field},
    {rename_column, old_name, new_name},
    {modify_column, score, float}
]}.
```

### Create Index

Indexes use a map-based options format with auto-generated names following Ecto conventions (`{table}_{columns}_index`):

```erlang
%% Simple unique index
{create_index, ~"users", [email], #{unique => true}}.
%% Generates: CREATE UNIQUE INDEX "users_email_index" ON "users" ("email")

%% Non-unique index
{create_index, ~"posts", [user_id], #{}}.
%% Generates: CREATE INDEX "posts_user_id_index" ON "posts" ("user_id")

%% Composite index
{create_index, ~"users", [first_name, last_name], #{}}.
%% Generates: CREATE INDEX "users_first_name_last_name_index" ON ...

%% Partial index
{create_index, ~"users", [email], #{unique => true, where => ~"email IS NOT NULL"}}.
%% Generates: CREATE UNIQUE INDEX "users_email_index" ON "users" ("email") WHERE email IS NOT NULL
```

The index name is auto-generated via `kura_migration:index_name/2`. If you need a custom name, the legacy 5-tuple format is still supported:

```erlang
{create_index, ~"my_custom_idx", ~"users", [email], [unique]}.
```

`concurrently => true` (`concurrently` in the list form) builds the index
without blocking writes to the table, and only works in a
[non-transactional migration](#non-transactional-migrations):

```erlang
{create_index, ~"users", [email], #{concurrently => true}}.
%% Generates: CREATE INDEX CONCURRENTLY "users_email_index" ON "users" ("email")
```

### Drop Index

```erlang
{drop_index, ~"users_email_index"}.
```

### Raw SQL

```erlang
{execute, ~"ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0)"}.
```

## Running Migrations

```erlang
%% Run all pending migrations
{ok, AppliedVersions} = kura_migrator:migrate(my_repo).
```

All pending migrations run inside a single transaction, unless one of them
is [non-transactional](#non-transactional-migrations). On Postgres, the
transaction holds a transaction-level advisory lock so concurrent nodes never
run migrations in parallel. SQLite (single-writer) relies on its serial
write-transaction semantics. The migrator gates the advisory-lock SQL on
the configured pool declaring the `advisory_locks` capability — backends
that don't declare it skip it. If any migration fails, the entire batch
rolls back and `{error, Reason}` is returned. The `schema_migrations` table
is updated row-by-row inside the same transaction, so partial progress is
impossible.

The lock is polled: the migrator tries `pg_try_advisory_xact_lock`, and a
transaction that misses it ends and a new one tries again
`migration_lock_retry_interval` milliseconds later (`kura` app env, default
100; anything below 10 is taken as 10). Waiting inside a blocking `pg_advisory_xact_lock` would keep a
transaction open that a `CREATE INDEX CONCURRENTLY` on the node holding the
lock has to wait for, and the two would deadlock. Nodes on an older kura
wait the blocking way; see the
[upgrade note](#upgrade-note-before-the-first-non-transactional-migration).

Inside `kura_db:transaction/2`, or with a sandbox connection checked out, the
lock is tried once and a miss returns `{error, migration_lock_busy}`, since
every retry would run inside that one open transaction.

A waiting migrator logs a warning every 30 seconds with the time it has
waited. The wait has no limit unless `migration_lock_timeout` (`kura` app
env, milliseconds, default `infinity`) is set, after which the operation
returns `{error, migration_lock_timeout}`.

The lock key is a fixed constant every kura migrator shares, and any role
that can connect to the database can take the same advisory lock and hold
it, stalling the migrations of every deploy. Set a finite
`migration_lock_timeout` in production, longer than your slowest migration
run, so a stuck or hostile holder fails the deploy instead of hanging it:

```erlang
{kura, [{migration_lock_timeout, 600000}]}
```

## Non-Transactional Migrations

Some statements refuse to run inside a transaction: `CREATE INDEX
CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `ALTER TYPE ... ADD VALUE` on older
Postgres versions. A migration opts out of the transaction by exporting
`transactional/0` returning `false`:

```erlang
-module(m20250102120000_index_users_email).
-behaviour(kura_migration).
-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [{create_index, ~"users", [email], #{concurrently => true}}].

down() ->
    [{execute, ~"DROP INDEX CONCURRENTLY \"users_email_index\""}].
```

### Upgrade note: before the first non-transactional migration

Deploy a kura with non-transactional migrations on every node that runs the
migrator before shipping the first `transactional() -> false` migration. A
node still on an older kura waits for the advisory lock the blocking way,
which deadlocks with a `CREATE INDEX CONCURRENTLY` on the node holding it.
It also ignores `transactional/0` and `concurrently`, so if it is the node
that runs the migration it builds the index with a plain `CREATE INDEX`
inside a transaction, blocking writes to the table for the whole build.

On Postgres use kura_postgres 1.1.5 or later. 1.1.3 and earlier do not
declare the `concurrent_indexes` capability, and a migration with
`concurrently => true` is refused on them with `{error,
{concurrent_indexes_unsupported, PoolModule}}` before anything in the run
executes. 1.1.5 also brings minato 1.1.3's session reset on checkin, which
releases the advisory lock of a connection whose unlock failed.

### What changes for such a migration

- **Each statement commits on its own.** The statements run one at a time on
  a connection checked out for the migration, outside any transaction.
- **The version is recorded last.** It is written to `schema_migrations` only
  after the last statement succeeds. If a statement fails, the statements
  before it stay committed, the version is not recorded, and the migration
  stays pending; `migrate/1` returns `{error, {migration_failed, Version,
  Reason}}` and the error log gives `committed_statements`. Fix the cause and
  undo or account for what was committed before migrating again. A failed
  `CREATE INDEX CONCURRENTLY` leaves an invalid index behind that a retry
  trips over, so drop it first (see
  [Recovering from a failed concurrent build](#recovering-from-a-failed-concurrent-build)).
  The simplest rule: one statement per
  non-transactional migration.
- **The run is split around it.** Pending migrations are chosen once, under
  the lock. Transactional migrations before the non-transactional one run in
  one transaction and commit; the ones after it run in further transactions.
  If the non-transactional migration fails, the earlier ones stay applied and
  the later ones do not run.
- **The lock is still held.** The migration's connection takes the
  session-level lock (`pg_try_advisory_lock`, polled the same way) before it
  re-reads `schema_migrations`, and releases it after recording the version,
  so no other node runs it at the same time. The lock is released between the
  steps of a run, so another node booting meanwhile may apply the next step
  itself, but every step re-reads `schema_migrations` under the lock: each
  migration runs once, in order.
- **Rollback** runs `down/0` the same way, and deletes the version last.

`concurrently => true` in a migration that is still transactional is refused
with `{error, {concurrently_in_transaction, Version, Module}}` before any
migration in the run executes, rather than left for Postgres to reject
part-way.

A run with a non-transactional migration pending is refused with `{error,
{non_transactional_in_transaction, Version, Module}}`, before anything in it
executes, when the caller is inside `kura_db:transaction/2` or holds a
`kura_sandbox` connection for the repo's pool. The enclosing transaction would
keep the lock held while the migration waited for it on a connection of its
own. Migrate before checking out the sandbox in a test setup.

A non-transactional migration holds a session-level lock and runs its
statements one by one on the same connection, so it needs that connection to
be one server session throughout. A pooler that hands a client a different
server connection per transaction, such as PgBouncer in transaction mode, is
not supported for non-transactional migrations: migrate through a direct
connection, or a pooler in session mode.

### Long index builds

A statement in a migration runs under the repo pool's read timeout. With
`kura_postgres` that is the pool's `timeout` option, 15 seconds by default,
and a per-statement timeout can only shorten it. `CREATE INDEX CONCURRENTLY`
on a large table runs longer than that: the client gives up, the server
cancels the build, and an `INVALID` index is left behind.

Rather than raising the application pool's `timeout`, which would let every
query the application makes hang that long, migrate through a repo of its
own. It points at the same database with its own small pool and a long or
`infinity` timeout, and reads the same migrations through the same
`otp_app/0` (and `migration_apps/0`, if the application repo has one):

```erlang
-module(my_migration_repo).
-behaviour(kura_repo).
-export([otp_app/0]).

otp_app() -> my_app.
```

```erlang
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            database => "my_app",
            pool_size => 10
        },
        my_migration_repo => #{
            backend => kura_backend_postgres,
            database => "my_app",
            pool_size => 2,
            timeout => infinity
        }
    }}
]}].
```

```erlang
{ok, _} = kura_migrator:migrate(my_migration_repo).
```

Both repos share `schema_migrations` and the advisory lock, since both are
in the same database, so the application repo's view of what is applied is
the same.

### Recovering from a failed concurrent build

A `CREATE INDEX CONCURRENTLY` that fails, because the client timed out, the
statement was cancelled, or Postgres's deadlock detector aborted it
(`deadlock detected`), leaves an `INVALID` index and the migration pending.
A re-run then fails on the name that already exists. Find the index, drop
it, and migrate again:

```sql
SELECT indexrelid::regclass AS index FROM pg_index WHERE NOT indisvalid;
DROP INDEX CONCURRENTLY IF EXISTS "users_email_index";
```

An invalid index is not used for reads but is still maintained on every
write, so do not leave one behind.

If the node dies after the build succeeded but before the version was
recorded, the index is `VALID` and the migration still pending, and a re-run
fails because the index already exists. Check that the index is the one the
migration builds and is valid, then record the version without running it
again with `fake/2`, which stamps the pending migrations up to and including
that version (use `fake/3` on a repo fed by more than one application):

```erlang
{ok, [20250102120000]} = kura_migrator:fake(my_repo, 20250102120000).
```

Only do this when every pending migration up to that version is in the
database already; `kura_migrator:status/1` lists them.

### On SQLite

SQLite has no concurrent index build and no advisory locks.
`concurrently => true` compiles to a plain `CREATE INDEX` there, because the
SQLite pool does not declare the `concurrent_indexes` capability and the
repo's dialect is not Postgres, so one migration serves both backends. It is
still refused in a transactional migration, on SQLite too, so a migration
that passes there passes on Postgres.

A non-transactional migration on SQLite runs each statement in autocommit on
one pinned connection and takes no lock: run one migrator per database.
Because every statement runs on the same connection, a `PRAGMA` holds for the
statements after it, which makes the documented table rebuild possible:
`PRAGMA foreign_keys = OFF`, rebuild the referenced table, `PRAGMA
foreign_key_check`, `PRAGMA foreign_keys = ON`. Turn foreign keys back on in
the same migration. From the next kura_sqlite release (kura_sqlite#26) the
pool also turns foreign keys back on whenever a connection is checked in, so
a migration that fails between `OFF` and `ON` does not hand the next caller a
connection without them. On an earlier kura_sqlite it does not, and that
connection keeps foreign keys off.

By default, `migrate/1` also calls `ensure_database/1` first, creating the
configured database if it does not yet exist. To disable, set
`{kura, [{ensure_database, false}]}` in your sys.config.

## Migrations From More Than One Application

An application that extends another - shipping its own tables alongside the
host's - keeps its migrations in its own `src/`. Tell the repo about it with
the optional `kura_repo` callback `migration_apps/0`:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0, migration_apps/0]).

otp_app() -> my_app.

migration_apps() -> [my_gdpr_extension].
```

The application owning the repo module is always included, so the callback
lists only the extras. Discovery then scans every listed application.

The order comes from the OTP `applications` list in each `.app` file - the
dependency graph the release already declares. A dependency's migrations all
run before its dependents', regardless of version numbers, so an extension
can safely add a foreign key to a table the host application creates. An
application in between that ships no migrations of its own still orders the
two that do. Ties break alphabetically, so the order is stable across boots.

Two rules follow from `schema_migrations` recording versions and nothing
else:

- **Versions are global.** Two applications shipping the same
  `<YYYYMMDDHHMMSS>` is an error - `{error, {duplicate_migration_version,
  [{Version, [{App, Module}]}]}}` - naming every claimant. Nothing runs.
- **Every listed application must be loaded.** One that is not gives
  `{error, {migration_apps_not_loaded, Apps}}` rather than quietly
  contributing no migrations.

There is no `app` column and no per-application version counter. Adding
`migration_apps/0` to an existing repo needs no schema change and no
backfill.

### Where to call `migrate/1` from

"Every listed application must be loaded" rules out one tempting place:
the host application's own `start/2`.

An extension depends on its host, so the host's `applications` list does
not name it - the dependency points the other way. `ensure_all_started(my_app)`
therefore loads and starts `my_app` *before* anything that depends on it
is loaded, and a `migrate/1` call inside `my_app`'s `start/2` hard-fails
with `{error, {migration_apps_not_loaded, [my_gdpr_extension]}}`. Nothing
is wrong with the configuration; the extension simply does not exist yet
at that instant.

A release does not have this problem: `relx` loads every application in
the boot script before starting any of them, so an extension named by
`migration_apps/0` is loaded by the time the host starts. `rebar3 shell`,
`ensure_all_started/1` in a test, and an escript building its own
application set all do.

Two ways out:

```erlang
%% Explicit, and works everywhere - load before you migrate.
[application:load(A) || A <- [my_gdpr_extension]],
{ok, _} = kura_migrator:migrate(my_repo).
```

or move the call out of `start/2` and into whatever starts the system
once every application is up - a release boot hook, or a top-level
`main/1`. Migrating from inside a supervisor's start-up also means a
failed migration takes the host down with a supervisor report rather
than a readable error, so it is worth moving regardless.

## Rolling Back

```erlang
%% Roll back the last migration
{ok, RolledBack} = kura_migrator:rollback(my_repo).

%% Roll back the last N migrations
{ok, RolledBack} = kura_migrator:rollback(my_repo, 3).
```

The window is the N highest applied versions - version is the only ordering
`schema_migrations` can offer. Within that window, migrations run in reverse
apply order, so a dependency's `down/0` never runs before its dependents'.

Every version in the window must resolve to a migration module. One that
does not aborts the rollback with
`{error, {unknown_applied_versions, Versions}}` and changes nothing. The
usual cause is a migration module deleted from source while its
`schema_migrations` row remains. Versions outside the window are not
affected, so an old deleted migration does not block unrelated rollbacks.

### Rolling back a repo with more than one application

`rollback/1,2` **refuses** a repo whose migrations come from more than one
application:

```erlang
{error, {ambiguous_rollback, [my_app, my_gdpr_extension]}} =
    kura_migrator:rollback(my_repo, 3).
```

A version window is the only window `schema_migrations` can express, and
across applications it stops meaning anything. "The last three" can take
one migration each from three unrelated extensions and leave every one of
them half-migrated. Worse, a version window is not even in dependency
order: a dependency's migration can carry the *newer* timestamp, so the
window can undo the thing a still-applied dependent is built on. Naming
one application is the only well-defined request, so kura makes you name
it:

```erlang
%% Roll back the last 2 migrations of my_gdpr_extension only.
{ok, RolledBack} = kura_migrator:rollback(my_repo, my_gdpr_extension, 2).
```

`rollback/3`'s window is the N highest applied versions **among those the
named application's migrations claim**, so no other application's
migrations can enter it whatever their timestamps are. An application not
in `migration_apps/1` gives `{error, {unknown_migration_app, App, Apps}}`.

kura cannot check the other direction for you: `schema_migrations` records
no application, so it cannot tell that rolling an application back leaves a
*dependent* application's tables referencing something gone. Roll dependents
back first.

A single-application repo - every repo without `migration_apps/0` - is not
a multi-application set, so `rollback/1,2` are unchanged for it.

## Checking Status

```erlang
Status = kura_migrator:status(my_repo).
%% Returns: [{Version, Module, up | pending}, ...]
```

## Baselining an Existing Database

When you adopt Kura on a database that already has tables - typically after
`rebar3 kura gen_schemas` introspects it into schema modules - the first
`compile` generates `create_table` migrations for tables that already exist,
and running them would fail. `fake/1` records every pending migration as
applied **without executing any DDL**, so you can baseline and then migrate
for real from there.

```erlang
%% Stamp the introspected baseline as applied (runs no DDL)
{ok, Faked} = kura_migrator:fake(my_repo).

%% Real migrations added later run normally
{ok, Applied} = kura_migrator:migrate(my_repo).
```

`fake/1` stamps *every* pending migration, so only run it when each pending
migration corresponds to schema that already exists - check `status/1` first.
The versions it stamps are logged at warning level. It runs under the same
advisory-locked transaction as `migrate/1`.

### Baselining up to a version

When the pending set mixes the introspected baseline with genuinely new
migrations, `fake/1` would stamp the new ones too, and their tables would
never be created. Pass the last baseline version instead: `fake/2` with an
integer stamps only the pending migrations at or below it.

```erlang
%% Stamp the introspected baseline, nothing after it.
{ok, _} = kura_migrator:fake(my_repo, 20260301120000).

%% The new migrations run for real.
{ok, _} = kura_migrator:migrate(my_repo).
```

The version must belong to a discovered migration, otherwise
`{error, {unknown_migration_version, Version}}` and nothing is stamped.

### Baselining a repo with more than one application

`fake/1` **refuses** a repo whose migrations come from more than one
application:

```erlang
{error, {ambiguous_fake, [my_app, my_gdpr_extension]}} =
    kura_migrator:fake(my_repo).
```

This is the most destructive thing kura could otherwise be asked to do
quietly. Baselining is what you do to a database that already has the
tables - but an extension installed at the same time has none. Stamping
its migrations too records tables as created that were never created, and
because `migrate/1` skips applied versions it will never create them
either. The first query against one of them fails at some unrelated
moment, with nothing in `schema_migrations` to suggest why.

Baseline one application at a time with `fake/2`, which leaves every other
application's migrations pending so `migrate/1` still runs them for real:

```erlang
%% The host's tables exist; stamp them.
{ok, _} = kura_migrator:fake(my_repo, my_app).

%% The extension is new; build its tables properly.
{ok, _} = kura_migrator:migrate(my_repo).
```

An application not in `migration_apps/1` gives
`{error, {unknown_migration_app, App, Apps}}`. To baseline one application
up to a version, use `fake/3`: `kura_migrator:fake(my_repo, my_app, Version)`.

## Schema-Level Indexes

Instead of manually writing index operations in migrations, you can declare indexes on your schema module. This is the recommended approach - it keeps index definitions alongside your schema and allows [rebar3_kura](https://github.com/Taure/rebar3_kura) to auto-generate the migration operations for you.

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"users".

fields() ->
    [#kura_field{name = id, type = uuid, primary_key = true, nullable = false},
     #kura_field{name = username, type = string, nullable = false},
     #kura_field{name = email, type = string},
     #kura_field{name = phone_number, type = string}].

indexes() ->
    [{[username], #{unique => true}},
     {[email], #{unique => true}},
     {[phone_number], #{unique => true, where => ~"phone_number IS NOT NULL"}}].
```

Unique indexes declared via `indexes/0` are automatically registered as changeset constraints - no manual `unique_constraint/2` calls needed. When the database raises a unique-constraint violation (mapped per dialect — `users_email_index` on Postgres, the equivalent on SQLite), it maps to `{email, <<"has already been taken">>}` on the changeset.

## Schema Migrations Table

Kura automatically creates a `schema_migrations` table to track which migrations have been applied. This table is created on first use of `migrate/1`, `fake/1`, `rollback/1`, or `status/1`.

Creation runs under the migration advisory lock, in a transaction of its
own that commits before the migration transaction takes the lock again.
`CREATE TABLE IF NOT EXISTS` is not atomic against a concurrent creator -
two sessions can both pass the existence check, and the loser fails on
`pg_type`'s unique index - so two nodes booting together need the lock to
create it safely, just as they do to run migrations.

If the statement fails - a role without `CREATE` on the schema, most
likely - the entry point returns
`{error, {schema_migrations_failed, Reason}}` instead of continuing on to
fail later against a table that was never created.
