# Kura

Database layer for Erlang — Ecto-equivalent abstractions in pure Erlang, targeting PostgreSQL via [pgo](https://github.com/erleans/pgo).

## Features

- **Schema** — behaviour-based schema definitions with type metadata
- **Changeset** — cast external params, validate, track changes and errors
- **Query Builder** — composable, functional query construction
- **SQL Compiler** — parameterized SQL generation (no string interpolation)
- **Repo** — CRUD operations with automatic type conversion and PG error mapping
- **Migrations** — DDL operations with version tracking

## Quick Start

### Define a Schema

```erlang
-module(user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, primary_key/0]).

table() -> <<"users">>.
primary_key() -> id.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = email, type = string, nullable = false},
        #kura_field{name = age, type = integer},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].
```

### Define a Repo

```erlang
-module(my_repo).
-behaviour(kura_repo).

-export([config/0, start/0, all/1, get/2, insert/1, update/1, delete/1]).

config() ->
    #{pool => my_repo, database => <<"myapp">>,
      hostname => <<"localhost">>, port => 5432,
      username => <<"postgres">>, password => <<>>,
      pool_size => 10}.

start() -> kura_repo_worker:start(?MODULE).
all(Q) -> kura_repo_worker:all(?MODULE, Q).
get(Schema, Id) -> kura_repo_worker:get(?MODULE, Schema, Id).
insert(CS) -> kura_repo_worker:insert(?MODULE, CS).
update(CS) -> kura_repo_worker:update(?MODULE, CS).
delete(CS) -> kura_repo_worker:delete(?MODULE, CS).
```

### Changesets

```erlang
%% Cast and validate external params
CS = kura_changeset:cast(user, #{}, #{<<"name">> => <<"Alice">>, <<"email">> => <<"alice@example.com">>}, [name, email, age]),
CS1 = kura_changeset:validate_required(CS, [name, email]),
CS2 = kura_changeset:validate_format(CS1, email, <<"@">>),
CS3 = kura_changeset:validate_length(CS2, name, [{min, 1}, {max, 100}]),

%% Insert
{ok, User} = my_repo:insert(CS3).
```

### Query Builder

```erlang
Q = kura_query:from(user),
Q1 = kura_query:where(Q, {age, '>', 18}),
Q2 = kura_query:where(Q1, {'or', [{role, <<"admin">>}, {role, <<"moderator">>}]}),
Q3 = kura_query:select(Q2, [name, email]),
Q4 = kura_query:order_by(Q3, [{name, asc}]),
Q5 = kura_query:limit(Q4, 10),

{ok, Users} = my_repo:all(Q5).
```

Supported conditions: `=`, `!=`, `<`, `>`, `<=`, `>=`, `like`, `ilike`, `in`, `not_in`, `is_nil`, `is_not_nil`, `between`, `{'and', [...]}`, `{'or', [...]}`, `{'not', ...}`, `{fragment, SQL, Params}`.

### Migrations

```erlang
-module(m20240115120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, <<"users">>, [
        #kura_column{name = id, type = id, primary_key = true, nullable = false},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false},
        #kura_column{name = age, type = integer},
        #kura_column{name = inserted_at, type = utc_datetime},
        #kura_column{name = updated_at, type = utc_datetime}
    ]},
    {create_index, <<"users_email_index">>, <<"users">>, [email], [unique]}].

down() ->
    [{drop_table, <<"users">>}].
```

Run migrations:

```erlang
kura_migrator:migrate(my_repo).
kura_migrator:rollback(my_repo).
kura_migrator:status(my_repo).
```

## Type Mapping

| Kura | PostgreSQL | Erlang |
|---|---|---|
| `id` | `BIGSERIAL` | `integer()` |
| `integer` | `INTEGER` | `integer()` |
| `float` | `DOUBLE PRECISION` | `float()` |
| `string` | `VARCHAR(255)` | `binary()` |
| `text` | `TEXT` | `binary()` |
| `boolean` | `BOOLEAN` | `boolean()` |
| `date` | `DATE` | `{Y, M, D}` |
| `utc_datetime` | `TIMESTAMPTZ` | `calendar:datetime()` |
| `uuid` | `UUID` | `binary()` |
| `jsonb` | `JSONB` | `map()` |
| `{array, T}` | `T[]` | `list()` |

## Configuration

```erlang
%% sys.config
[{kura, [
    {migration_paths, ["priv/migrations"]},
    {log_queries, false}
]}].
```

## Requirements

- Erlang/OTP 26+
- PostgreSQL 14+
