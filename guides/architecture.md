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
anything — execution happens in `kura_repo_worker`.

### `kura_query_compiler`

Translates a `#kura_query{}` record into parameterized SQL (`$1`, `$2`,
etc.) suitable for `pgo`. Handles SELECT, INSERT, UPDATE, DELETE, and
bulk operations. Supports fragments for raw SQL escape hatches.

### `kura_repo` and `kura_repo_worker`

`kura_repo` is a thin behaviour — user repo modules implement `config/0`
and delegate operations to `kura_repo_worker`. The worker handles:

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
`down/0` callbacks. `kura_migrator` discovers migration modules,
maintains a `schema_migrations` table, and applies pending migrations
in order.

## Data Flow

A typical insert operation flows through the system like this:

1. User calls `MyRepo:insert(Changeset)`
2. `kura_repo_worker:insert/2` validates the changeset
3. If `assoc_changes` is non-empty, wraps everything in a transaction
4. `kura_query_compiler` generates `INSERT INTO ... RETURNING *`
5. `kura_types` dumps each changed field to its PG representation
6. `pgo` executes the query
7. `kura_types` loads the returned row back into Erlang terms
8. If there are association changes, child records are persisted with the parent's PK set as their foreign key
9. On PG constraint errors, the error is mapped back to a changeset error via declared constraints
