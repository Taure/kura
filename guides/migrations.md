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
    [{create_table, <<"users">>, [
        #kura_column{name = id, type = id, primary_key = true},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
    ]}].

down() ->
    [{drop_table, <<"users">>}].
```

Place migration files in `priv/migrations/`. Kura automatically discovers them using `code:priv_dir/1` based on the application that owns the repo module — no configuration needed.

Migration `.erl` files are compiled at runtime, so you don't need to add them to `extra_src_dirs` or any build config.

If you need custom paths, you can override discovery via `sys.config`:

```erlang
{kura, [
    {migration_paths, [{priv_dir, my_app, "migrations"}]}
]}.
```

## DDL Operations

### Create Table

```erlang
{create_table, <<"table_name">>, [
    #kura_column{name = id, type = id, primary_key = true},
    #kura_column{name = name, type = string, nullable = false},
    #kura_column{name = score, type = integer, default = 0},
    #kura_column{name = active, type = boolean, default = true}
]}.
```

Column options:
- `primary_key` — `true | false` (default: `false`)
- `nullable` — `true | false` (default: `true`)
- `default` — literal value (integer, float, binary, boolean) or `undefined` for none

### Drop Table

```erlang
{drop_table, <<"table_name">>}.
```

### Alter Table

```erlang
{alter_table, <<"users">>, [
    {add_column, #kura_column{name = bio, type = text}},
    {drop_column, old_field},
    {rename_column, old_name, new_name},
    {modify_column, score, float}
]}.
```

### Create Index

```erlang
{create_index, <<"users_email_index">>, <<"users">>, [email], [unique]}.

%% Partial index
{create_index, <<"users_active_email_idx">>, <<"users">>, [email],
    [unique, {where, <<"\"active\" = TRUE">>}]}.
```

### Drop Index

```erlang
{drop_index, <<"users_email_index">>}.
```

### Raw SQL

```erlang
{execute, <<"ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0)">>}.
```

## Running Migrations

```erlang
%% Run all pending migrations
{ok, AppliedVersions} = kura_migrator:migrate(my_repo).
```

Each migration runs in its own transaction. If a migration fails, it is rolled back and subsequent migrations are not attempted.

## Rolling Back

```erlang
%% Roll back the last migration
{ok, RolledBack} = kura_migrator:rollback(my_repo).

%% Roll back the last N migrations
{ok, RolledBack} = kura_migrator:rollback(my_repo, 3).
```

## Checking Status

```erlang
Status = kura_migrator:status(my_repo).
%% Returns: [{Version, Module, up | pending}, ...]
```

## Schema Migrations Table

Kura automatically creates a `schema_migrations` table to track which migrations have been applied. This table is created on first use of `migrate/1`, `rollback/1`, or `status/1`.
