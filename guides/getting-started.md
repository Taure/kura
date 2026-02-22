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

-export([table/0, fields/0, primary_key/0]).

table() -> <<"users">>.
primary_key() -> id.

fields() ->
    [#kura_field{name = id, type = id},
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

-export([config/0]).

config() ->
    #{database => <<"my_app_dev">>,
      hostname => <<"localhost">>,
      port => 5432,
      username => <<"postgres">>,
      password => <<"postgres">>,
      pool_size => 10}.
```

## Define a Migration

Create a migration module in `priv/migrations/`:

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

## Start the Pool & Run Migrations

```erlang
ok = kura_repo_worker:start(my_repo),
{ok, _Versions} = kura_migrator:migrate(my_repo).
```

## Basic CRUD

### Insert

```erlang
CS = kura_changeset:cast(my_user, #{}, #{<<"name">> => <<"Alice">>, <<"email">> => <<"alice@example.com">>}, [name, email]),
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
Q = kura_query:where(kura_query:from(my_user), {name, <<"Alice">>}),
{ok, Users} = kura_repo_worker:all(my_repo, Q).
```

### Update

```erlang
CS = kura_changeset:cast(my_user, User, #{<<"name">> => <<"Bob">>}, [name]),
{ok, Updated} = kura_repo_worker:update(my_repo, CS).
```

### Delete

```erlang
CS = kura_changeset:cast(my_user, User, #{}, []),
{ok, Deleted} = kura_repo_worker:delete(my_repo, CS).
```
