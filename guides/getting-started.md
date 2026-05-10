# Getting Started

## Installation

Add kura to your `rebar.config` dependencies:

```erlang
{deps, [
    kura
]}.
```

## Define a Schema

Create a schema module that implements the `kura_schema` behaviour:

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).

table() -> ~"users".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = name, type = string},
     #kura_field{name = email, type = string},
     #kura_field{name = inserted_at, type = utc_datetime},
     #kura_field{name = updated_at, type = utc_datetime}].
```

Supported types: `id`, `integer`, `float`, `string`, `text`, `boolean`, `date`, `utc_datetime`, `uuid`, `jsonb`, `{array, Type}`.

## Define a Repo

Create a repo module that implements the `kura_repo` behaviour:

```erlang
-module(my_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> my_app.
```

Then add database configuration to your `sys.config`. Pick a backend
package and point kura at it via `{backend, ...}`:

```erlang
%% Postgres
[{kura, [
    {repo, my_repo},
    {backend, kura_backend_postgres},
    {host, "localhost"},
    {port, 5432},
    {database, "my_app_dev"},
    {user, "postgres"},
    {password, "postgres"},
    {pool_size, 10}
]}].
```

```erlang
%% SQLite
[{kura, [
    {repo, my_repo},
    {backend, kura_backend_sqlite},
    {database, <<"my_app.db">>},
    {pool_size, 4}
]}].
```

Kura starts the configured pool automatically. UUID primary keys are
auto-generated on insert when no value is provided.

### Multiple repos

Run two or more repos in the same app, each with its own backend, by
swapping `{repo, ...}` for a `{repos, #{Name => Cfg}}` map:

```erlang
%% Postgres primary + SQLite analytics
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            database => "my_app_dev",
            user => "postgres",
            pool_size => 10
        },
        analytics_repo => #{
            backend => kura_backend_sqlite,
            database => <<":memory:">>
        }
    }}
]}].
```

Each repo module declares itself the same way (`-behaviour(kura_repo)`
+ `otp_app/0`). Queries through `my_repo` emit Postgres SQL; queries
through `analytics_repo` emit SQLite SQL. The query cache is keyed per
repo so the dialects never share entries.

## Define a Migration

Create a migration module under `src/` (any subdirectory works, e.g. `src/migrations/`). Migrations are regular Erlang modules and must be compiled by rebar3 - files in `priv/` are not compiled and will not be discovered.

The module name must match `m<YYYYMMDDHHMMSS>_<name>`:

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
    ]},
    {create_index, ~"users", [email], #{unique => true}}].

down() ->
    [{drop_index, ~"users_email_index"},
     {drop_table, ~"users"}].
```

If your `rebar.config` does not already enable recursive `src/` compilation, add it so subdirectories are picked up:

```erlang
{erl_opts, [{src_dirs, [{"src", [{recursive, true}]}]}]}.
```

## Run Migrations

Once the `kura` application starts, the connection pool is up. Run pending migrations with:

```erlang
{ok, _Versions} = kura_migrator:migrate(my_repo).
```

If you start the pool manually outside of `kura_app` (for example in a test setup), use `kura_repo_worker:start/1` first.

## Basic CRUD

### Insert

```erlang
CS = kura_changeset:cast(my_user, #{}, #{~"name" => ~"Alice", ~"email" => ~"alice@example.com"}, [name, email]),
CS1 = kura_changeset:validate_required(CS, [name, email]),
{ok, User} = kura_repo_worker:insert(my_repo, CS1).
```

### Read

```erlang
%% By primary key
{ok, User} = kura_repo_worker:get(my_repo, my_user, 1),

%% All rows
{ok, Users} = kura_repo_worker:all(my_repo, kura_query:from(my_user)),

%% With conditions
Q = kura_query:where(kura_query:from(my_user), {name, ~"Alice"}),
{ok, Users} = kura_repo_worker:all(my_repo, Q).
```

### Update

```erlang
CS = kura_changeset:cast(my_user, User, #{~"name" => ~"Bob"}, [name]),
{ok, Updated} = kura_repo_worker:update(my_repo, CS).
```

### Delete

```erlang
CS = kura_changeset:cast(my_user, User, #{}, []),
{ok, Deleted} = kura_repo_worker:delete(my_repo, CS).
```
