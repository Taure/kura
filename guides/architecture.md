# Architecture

This document describes the internal architecture of Kura and how the
modules fit together.

## Module Overview

```
┌─────────────────────────────────────────────────┐
│                  User Code                      │
│         (YourRepo, YourSchema modules)          │
└──────────┬──────────────────────┬───────────────┘
           │                      │
    ┌──────▼──────┐        ┌──────▼──────┐
    │  kura_repo  │        │ kura_schema │
    │ (behaviour) │        │ (behaviour) │
    └──────┬──────┘        └──────┬──────┘
           │                      │
    ┌──────▼──────────────────────▼───────────────┐
    │            kura_repo_worker                  │
    │  (query execution, type conversion, txns)   │
    └──────┬──────────────┬───────────────────────┘
           │              │
    ┌──────▼──────┐ ┌─────▼──────────┐
    │ kura_query  │ │ kura_changeset │
    │ (builder)   │ │ (validation)   │
    └──────┬──────┘ └────────────────┘
           │
    ┌──────▼──────────────┐
    │ kura_query_compiler │
    │ (SQL generation)    │
    └──────┬──────────────┘
           │
    ┌──────▼──────┐
    │     pgo     │
    │ (PG driver) │
    └─────────────┘
```

## Core Modules

### `kura_schema`

Behaviour that defines the shape of a database table. Schemas declare
their table name, fields (via `#kura_field{}` records), primary key,
and optionally associations and embeds. Field metadata is cached in
`persistent_term` for fast lookups at runtime.

### `kura_changeset`

Tracks changes between a schema's current data and incoming params.
Provides casting, validations (`validate_required`, `validate_format`,
`validate_length`, etc.), and constraint declarations that map PostgreSQL
constraint errors back to user-facing field errors.

### `kura_changeset_assoc`

Handles nested association and embed casting (`cast_assoc/2,3`,
`cast_embed/2,3`, `put_assoc/3`). Builds child changesets from nested
params and manages the `assoc_changes` list on the parent changeset.

### `kura_query`

Composable, functional query builder. Queries are built by chaining
calls like `from/1`, `where/2`, `order_by/2`, `limit/2`, and
`preload/2`. The query record accumulates conditions without executing
anything - execution happens in `kura_repo_worker`.

### `kura_query_compiler`

Translates a `#kura_query{}` record into parameterized SQL (`$1`, `$2`,
etc.) suitable for `pgo`. Handles SELECT, INSERT, UPDATE, DELETE, and
bulk operations. Supports fragments for raw SQL escape hatches.

### `kura_repo` and `kura_repo_worker`

`kura_repo` is a thin behaviour - user repo modules implement `otp_app/0`
(plus optional `init/1` to mutate config at runtime). The repo module itself
holds no CRUD callbacks; operations are invoked as
`kura_repo_worker:insert(MyRepo, CS)` etc., or wrapped with thin
delegations like `insert(CS) -> kura_repo_worker:insert(?MODULE, CS).`
in the user's repo module.

Database configuration is read from `application:get_env(kura, ...)` first
(`host`, `port`, `database`, `user`, `password`, `pool_size`, `ssl`,
`socket_options`). The legacy `application:get_env(OtpApp, RepoModule)`
form is still accepted as a fallback. The worker handles:

- Query execution via `pgo`
- Type conversion between Erlang terms and PostgreSQL values (dump/load)
- PostgreSQL error mapping (constraint violations → changeset errors)
- Transaction wrapping for changesets with `assoc_changes`
- Query telemetry (optional logging of queries, durations, results)

### `kura_preloader`

Executes preload queries after the primary query completes. Uses
`WHERE ... IN (...)` queries rather than JOINs, supporting nested
preloads and all association types including many-to-many via join tables.

### `kura_multi`

Transaction pipelines inspired by `Ecto.Multi`. Lets you compose named
steps (`insert`, `update`, `delete`, `run`) where each step can access
the accumulated results of previous steps. The entire pipeline executes
inside a single database transaction.

### `kura_types`

Type system that defines how Erlang values map to PostgreSQL column types.
Each type implements `cast` (user input → internal), `dump` (internal →
PG), and `load` (PG → internal). Supported types include `id`, `integer`,
`float`, `string`, `text`, `boolean`, `date`, `utc_datetime`, `uuid`,
`jsonb`, `{enum, [atom()]}`, `{array, T}`, and `{embed, Type, Module}`.

### `kura_migration` and `kura_migrator`

`kura_migration` is a behaviour for writing migrations with `up/0` and
`down/0` callbacks (plus optional `safe/0` to acknowledge intentionally
unsafe operations during rolling deployments). `kura_migrator` discovers
migration modules, maintains a `schema_migrations` table, and applies
pending migrations in order. All migrations run inside a single
transaction guarded by a PostgreSQL advisory lock, so concurrent nodes
do not race.

## Other Modules

- **`kura_db`** - central wrapper around `pgo`. All query execution,
  transactions, pool lookup, sandbox routing, and telemetry events go
  through here.
- **`kura_type`** - behaviour for user-defined custom field types
  (`cast/1`, `dump/1`, `load/1`, `pg_type/0`).
- **`kura_query_cache`** - ETS-backed cache for compiled SQL strings,
  keyed by query shape.
- **`kura_paginator`** - offset and cursor pagination on top of
  `kura_query`.
- **`kura_stream`** - server-side cursor streaming for very large
  result sets.
- **`kura_tenant`** - process-dictionary-scoped schema prefix for
  multi-tenant deployments.
- **`kura_sandbox`** - test sandbox that runs each test in its own
  transaction and rolls back on completion.
- **`kura_audit`** / **`kura_audit_log`** - automatic change tracking
  with actor context, written to a separate audit table.
- **`kura_app`** / **`kura_sup`** - OTP application and root
  supervisor. The application callback also starts the configured pool
  and seeds `pg_types` defaults.

## Data Flow

A typical insert operation flows through the system like this:

1. User calls `kura_repo_worker:insert(MyRepo, Changeset)` (or a thin wrapper on the repo module).
2. If the changeset is invalid, returns `{error, CS}` immediately.
3. If `assoc_changes` is non-empty, or the schema declares lifecycle hooks that need a transaction, the operation is wrapped in `kura_db:transaction_ok/2`.
4. `kura_types:dump/2` converts each changed field to its PostgreSQL representation.
5. `kura_query_compiler:insert/4` generates a parameterized `INSERT ... RETURNING ...` statement.
6. `pgo` executes the query through the configured pool.
7. `kura_db:load_row/2` loads the returned row back into Erlang terms.
8. Any association changes are persisted with the parent's primary key set as their foreign key.
9. PostgreSQL constraint errors are mapped back to changeset errors via the constraints declared on the changeset.
