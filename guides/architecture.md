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
    │ (facade, cache)     │
    └──────┬──────────────┘
           │
    ┌──────▼──────────────────────────────────┐
    │  kura_pool / kura_driver / kura_dialect │
    │  / kura_capabilities (behaviours)       │
    └──────┬──────────────────────┬───────────┘
           │                      │
    ┌──────▼──────┐        ┌──────▼──────┐
    │ kura_pool_pgo │      │ kura_pool_sqlite │
    │ kura_driver_pgo │    │ kura_driver_sqlite │
    │ kura_dialect_pg │    │ kura_dialect_sqlite │
    │   (via pgo)    │     │  (via esqlite)     │
    └──────┬──────┘        └──────┬──────┘
       Postgres                 SQLite
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

### `kura_query_compiler` and `kura_dialect`

`#kura_query{}` is the portable AST. `kura_dialect` is the behaviour
that turns it into SQL bytes for a specific backend.

- `kura_dialect_pg` is the default ANSI-ish SQL emitter (`$N` placeholders,
  `RETURNING`, `ON CONFLICT (...) DO UPDATE`, etc.). Lives in kura core.
- `kura_dialect_sqlite` (in `kura_sqlite`) delegates AST emission to
  `kura_dialect_pg` and post-processes `$N` → `?N`. Type/default emission
  is overridden via `column_type/1` and `format_default/1`.
- `kura_query_compiler` is the public-facing facade. It owns the query
  cache and resolves the configured dialect via
  `application:get_env(kura, dialect)`. The dialect is set automatically
  when you configure `{backend, kura_backend_postgres|kura_backend_sqlite}`.

### `kura_repo` and `kura_repo_worker`

`kura_repo` is a thin behaviour - user repo modules implement `otp_app/0`
(plus optional `init/1` to mutate config at runtime). The repo module itself
holds no CRUD callbacks; operations are invoked as
`kura_repo_worker:insert(MyRepo, CS)` etc., or wrapped with thin
delegations like `insert(CS) -> kura_repo_worker:insert(?MODULE, CS).`
in the user's repo module.

Database configuration is read from `application:get_env(kura, ...)` first
(`backend`, plus connection keys like `database`, `pool_size`, and any
backend-specific keys such as `host`/`port`/`user`/`password` for Postgres).
The legacy `application:get_env(OtpApp, RepoModule)` form is still accepted
as a fallback. The worker handles:

- Query execution via the configured `kura_driver`
- Type conversion between Erlang terms and database values (dump/load)
- Constraint-violation mapping back to changeset errors
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

Type system that defines how Erlang values map to database column types.
Each type implements `cast` (user input → internal), `dump` (internal →
backend), and `load` (backend → internal). Supported types include `id`,
`integer`, `float`, `string`, `text`, `boolean`, `date`, `utc_datetime`,
`uuid`, `jsonb`, `{enum, [atom()]}`, `{array, T}`, and `{embed, Type, Module}`.
The backend dialect's `column_type/1` callback determines the SQL column
type emitted in DDL.

### `kura_migration` and `kura_migrator`

`kura_migration` is a behaviour for writing migrations with `up/0` and
`down/0` callbacks (plus optional `safe/0` to acknowledge intentionally
unsafe operations during rolling deployments). `kura_migrator` discovers
migration modules, maintains a `schema_migrations` table, and applies
pending migrations in order. All migrations run inside a single
transaction. On Postgres, the transaction is guarded by an advisory
lock so concurrent nodes do not race; on SQLite the single-writer
transaction handles serialisation. The advisory-lock SQL is gated on
the configured pool declaring the `advisory_locks` capability.

## Other Modules

- **`kura_db`** - central facade. All query execution, transactions,
  pool lookup, sandbox routing, and telemetry events go through here.
  Resolves the configured `kura_pool` and `kura_driver` per repo and
  routes calls accordingly.
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
  supervisor. The application callback resolves the configured backend
  aggregator (sets `dialect`, `pool_module`, `driver_module` from
  `kura_backend_postgres` / `kura_backend_sqlite`) and starts the
  configured pool. `pg_types` defaults are seeded only when the active
  dialect is `kura_dialect_pg`.
- **`kura_capabilities`** - capability flags for backends. A backend
  module implements the optional `capabilities/0` callback to declare
  features it supports (`returning`, `jsonb`, `listen_notify`,
  `select_for_update_skip_locked`, ...). Consumers call
  `kura_capabilities:require/2` from supervisor init so the
  application refuses to start on a backend that lacks a required
  feature instead of failing at runtime.

## Data Flow

A typical insert operation flows through the system like this:

1. User calls `kura_repo_worker:insert(MyRepo, Changeset)` (or a thin wrapper on the repo module).
2. If the changeset is invalid, returns `{error, CS}` immediately.
3. If `assoc_changes` is non-empty, or the schema declares lifecycle hooks that need a transaction, the operation is wrapped in `kura_db:transaction_ok/2`.
4. `kura_types:dump/2` converts each changed field to its backend representation.
5. `kura_query_compiler:insert/4` generates a parameterized `INSERT ... RETURNING ...` statement (via the configured dialect).
6. The configured `kura_driver` executes the query through the `kura_pool` checkout.
7. `kura_db:load_row/2` loads the returned row back into Erlang terms.
8. Any association changes are persisted with the parent's primary key set as their foreign key.
9. Database constraint errors are mapped back to changeset errors via the constraints declared on the changeset.
