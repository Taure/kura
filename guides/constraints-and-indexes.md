# Constraints and Indexes

This guide walks through translating SQL table definitions into Kura schemas, migrations, and changeset validations. If you're coming from SQL or Ecto, this will feel familiar.

## From SQL to Kura

Let's start with a typical SQL table:

```sql
CREATE TABLE users
(
    id            UUID PRIMARY KEY,
    username      VARCHAR(255) NOT NULL UNIQUE,
    phone_number  VARCHAR(255) UNIQUE,
    email         VARCHAR(255) UNIQUE,
    avatar        VARCHAR(255),
    password      VARCHAR(255) NOT NULL,
    inserted_at   TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX users_username_index ON users (username);
CREATE UNIQUE INDEX users_email_index ON users (email);
CREATE UNIQUE INDEX users_phone_number_index ON users (phone_number)
    WHERE phone_number IS NOT NULL;
```

In Kura, this maps to three pieces: a **schema**, **indexes**, and a **migration**.

### The Schema

```erlang
-module(user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"users".

fields() ->
    [#kura_field{name = id, type = uuid, primary_key = true, nullable = false},
     #kura_field{name = username, type = string, nullable = false},
     #kura_field{name = phone_number, type = string},
     #kura_field{name = email, type = string},
     #kura_field{name = avatar, type = string},
     #kura_field{name = password, type = string, nullable = false, virtual = true},
     #kura_field{name = password_hash, type = string, nullable = false},
     #kura_field{name = inserted_at, type = utc_datetime, nullable = false},
     #kura_field{name = updated_at, type = utc_datetime, nullable = false}].

indexes() ->
    [{[username], #{unique => true}},
     {[email], #{unique => true}},
     {[phone_number], #{unique => true, where => ~"phone_number IS NOT NULL"}}].
```

Key points:

- `nullable = false` maps to SQL `NOT NULL`
- `primary_key = true` maps to SQL `PRIMARY KEY`
- `indexes/0` declares which columns get unique indexes
- `virtual = true` on `password` means it participates in casting/validation but is never stored in the database — you'd hash it before insert

### The Migration

```erlang
-module(m20260306120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"users", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = username, type = string, nullable = false},
        #kura_column{name = phone_number, type = string},
        #kura_column{name = email, type = string},
        #kura_column{name = avatar, type = string},
        #kura_column{name = password_hash, type = string, nullable = false},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
    ]},
    {create_index, ~"users", [username], #{unique => true}},
    {create_index, ~"users", [email], #{unique => true}},
    {create_index, ~"users", [phone_number], #{unique => true,
        where => ~"phone_number IS NOT NULL"}}].

down() ->
    [{drop_index, ~"users_phone_number_index"},
     {drop_index, ~"users_email_index"},
     {drop_index, ~"users_username_index"},
     {drop_table, ~"users"}].
```

> If you use [rebar3_kura](https://github.com/Taure/rebar3_kura), the migration is generated automatically from your schema — you don't write it by hand.

### The Changeset

Because `indexes/0` declares unique indexes, Kura **automatically registers** changeset constraints. When a duplicate insert violates a unique index, you get a friendly error instead of a crash:

```erlang
registration_changeset(Params) ->
    CS = kura_changeset:cast(user, #{}, Params, [username, email, phone_number, password]),
    CS1 = kura_changeset:validate_required(CS, [username, password]),
    CS2 = kura_changeset:validate_length(CS1, username, [{min, 3}, {max, 30}]),
    CS3 = kura_changeset:validate_length(CS2, password, [{min, 8}]),
    CS4 = kura_changeset:validate_format(CS3, email, ~"^[^@]+@[^@]+$"),
    hash_password(CS4).

%% Insert — constraint errors are handled automatically
case my_repo:insert(registration_changeset(Params)) of
    {ok, User} ->
        User;
    {error, #kura_changeset{errors = [{username, ~"has already been taken"}]}} ->
        %% username was a duplicate
        handle_error(username_taken)
end.
```

No manual `unique_constraint/2` calls needed — the `indexes/0` callback handles it.

## Constraints

### How Constraint Errors Work

When PostgreSQL rejects an insert or update due to a constraint violation, Kura maps the error back to a changeset field error instead of crashing. This mapping works through **constraint declarations** — records that say "if PG constraint X fires, put error Y on field Z".

There are three ways to declare constraints:

1. **`indexes/0`** — auto-registers unique constraints for indexes (recommended for single/multi-column unique)
2. **`constraints/0`** — auto-registers constraints for composite unique and check constraints
3. **Manual** — `unique_constraint/2`, `foreign_key_constraint/2`, `check_constraint/3` on the changeset

### Unique Constraints

For single-column unique constraints, use `indexes/0` (shown above). The constraint is registered automatically.

For **composite** unique constraints (multiple columns together), use `constraints/0`:

```erlang
-module(participant).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, constraints/0]).

table() -> ~"participants".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true, nullable = false},
     #kura_field{name = chat_id, type = uuid, nullable = false},
     #kura_field{name = user_id, type = uuid, nullable = false}].

constraints() ->
    [{unique, [chat_id, user_id]}].
```

This auto-generates the constraint name `participants_chat_id_user_id_key` and maps violations to the first column (`chat_id`) with `"has already been taken"`.

In the migration, declare it as a table-level constraint:

```erlang
{create_table, ~"participants", [
    #kura_column{name = id, type = id, primary_key = true, nullable = false},
    #kura_column{name = chat_id, type = uuid, nullable = false},
    #kura_column{name = user_id, type = uuid, nullable = false}
], [{unique, [chat_id, user_id]}]}.
```

### Foreign Key Constraints

Foreign keys are declared in migrations via `references` on `#kura_column{}`. To get friendly errors when a referenced row doesn't exist, declare the constraint on the changeset:

```erlang
changeset(Params) ->
    CS = kura_changeset:cast(post, #{}, Params, [title, user_id]),
    CS1 = kura_changeset:validate_required(CS, [title, user_id]),
    kura_changeset:foreign_key_constraint(CS1, user_id).
```

Now if you insert a post with a `user_id` that doesn't exist:

```erlang
{error, #kura_changeset{errors = [{user_id, ~"does not exist"}]}}
```

Instead of a raw PostgreSQL error.

### Check Constraints

Check constraints enforce arbitrary SQL conditions. Declare them with `constraints/0` on the schema or as table-level constraints in the migration:

```erlang
%% Migration
{create_table, ~"orders", [
    #kura_column{name = id, type = id, primary_key = true},
    #kura_column{name = quantity, type = integer, nullable = false}
], [{check, ~"quantity > 0"}]}.
```

To map the violation to a field error, use `check_constraint/3` on the changeset:

```erlang
changeset(Params) ->
    CS = kura_changeset:cast(order, #{}, Params, [quantity]),
    kura_changeset:check_constraint(CS, ~"orders_check", quantity, #{
        message => ~"must be positive"
    }).
```

Or declare it on the schema with `constraints/0`:

```erlang
constraints() ->
    [{check, ~"quantity > 0"}].
```

### Manual Constraint Declarations

When you need custom constraint names or messages, use the manual functions:

```erlang
CS1 = kura_changeset:unique_constraint(CS, email, #{
    name => ~"users_email_unique",
    message => ~"is already registered"
}),
CS2 = kura_changeset:foreign_key_constraint(CS1, team_id),
CS3 = kura_changeset:check_constraint(CS2, ~"positive_balance", balance, #{
    message => ~"cannot be negative"
}).
```

### When to Use What

| Scenario | Use |
|---|---|
| Single-column unique | `indexes/0` with `#{unique => true}` |
| Composite unique (e.g. join table) | `constraints/0` with `{unique, [cols]}` |
| Foreign key error mapping | `foreign_key_constraint/2` on changeset |
| Check constraint error mapping | `constraints/0` with `{check, expr}` or `check_constraint/3` on changeset |
| Custom error messages | Manual `unique_constraint/3`, `foreign_key_constraint/3`, `check_constraint/4` |

## A More Complex Example: Foreign Keys and Composite Indexes

```sql
CREATE TABLE posts
(
    id          UUID PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    body        TEXT,
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    inserted_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX posts_user_id_index ON posts (user_id);

CREATE TABLE posts_tags
(
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id  UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);
```

### Schema

```erlang
-module(post).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, associations/0, indexes/0]).

table() -> ~"posts".

fields() ->
    [#kura_field{name = id, type = uuid, primary_key = true, nullable = false},
     #kura_field{name = title, type = string, nullable = false},
     #kura_field{name = body, type = text},
     #kura_field{name = user_id, type = uuid, nullable = false},
     #kura_field{name = inserted_at, type = utc_datetime, nullable = false},
     #kura_field{name = updated_at, type = utc_datetime, nullable = false}].

associations() ->
    [#kura_assoc{name = author, type = belongs_to, schema = user, foreign_key = user_id},
     #kura_assoc{name = tags, type = many_to_many, schema = tag,
                 join_through = ~"posts_tags", join_keys = {post_id, tag_id}}].

indexes() ->
    [{[user_id], #{}}].
```

### Migration

```erlang
-module(m20260306130000_create_posts).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"posts", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = title, type = string, nullable = false},
        #kura_column{name = body, type = text},
        #kura_column{name = user_id, type = uuid, nullable = false,
                     references = {~"users", id}, on_delete = cascade},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
    ]},
    {create_index, ~"posts", [user_id], #{}},
    {create_table, ~"posts_tags", [
        #kura_column{name = post_id, type = uuid, nullable = false,
                     references = {~"posts", id}, on_delete = cascade},
        #kura_column{name = tag_id, type = uuid, nullable = false,
                     references = {~"tags", id}, on_delete = cascade}
    ], [{unique, [post_id, tag_id]}]}].

down() ->
    [{drop_table, ~"posts_tags"},
     {drop_index, ~"posts_user_id_index"},
     {drop_table, ~"posts"}].
```

Note:

- `references = {~"users", id}` generates `REFERENCES "users"("id")`
- `on_delete = cascade` generates `ON DELETE CASCADE`
- The join table uses table-level `{unique, [post_id, tag_id]}` for the composite primary key constraint
- Non-unique indexes use an empty map `#{}` for options

## Upserts (ON CONFLICT)

When you have unique indexes, you can use upserts to handle conflicts gracefully at insert time:

```erlang
%% Do nothing on conflict (skip the duplicate)
my_repo:insert(CS, #{on_conflict => {email, nothing}}).

%% Replace all fields on conflict
my_repo:insert(CS, #{on_conflict => {email, replace_all}}).

%% Replace specific fields on conflict
my_repo:insert(CS, #{on_conflict => {email, {replace, [name, updated_at]}}}).

%% Use a named constraint instead of a column
my_repo:insert(CS, #{on_conflict => {{constraint, ~"users_email_index"}, nothing}}).

%% Multi-column conflict target (matches a multi-column unique INDEX)
my_repo:insert(CS, #{on_conflict =>
    {{columns, [world_id, zone_x, zone_y]}, {replace, [updated_at, payload]}}}).
```

Use `{columns, [...]}` when your unique index spans multiple columns. `kura_migration`'s
`create_index ... #{unique => true}` emits a unique INDEX (not a CONSTRAINT), so
`ON CONFLICT (cols)` is the form PostgreSQL accepts — `{constraint, Name}` only
works for indexes promoted to constraints (e.g. via `ADD CONSTRAINT ... UNIQUE USING INDEX`).
Column order in the target must match the index definition.

## Quick Reference

| SQL | Kura Schema | Kura Migration |
|---|---|---|
| `NOT NULL` | `nullable = false` | `nullable = false` |
| `PRIMARY KEY` | `primary_key = true` | `primary_key = true` |
| `UNIQUE` (single col) | `indexes/0` with `#{unique => true}` | `{create_index, T, [col], #{unique => true}}` |
| `UNIQUE` (multi col) | `constraints/0` with `{unique, [cols]}` | 4-tuple `{create_table, T, Cols, [{unique, [cols]}]}` |
| `REFERENCES` | — | `references = {Table, col}` on `#kura_column{}` |
| `ON DELETE CASCADE` | — | `on_delete = cascade` on `#kura_column{}` |
| `CHECK (expr)` | `constraints/0` with `{check, Expr}` | `{check, Expr}` in table constraints |
| `CREATE INDEX` | `indexes/0` with `#{}` | `{create_index, T, [cols], #{}}` |
| `CREATE UNIQUE INDEX ... WHERE` | `indexes/0` with `#{unique => true, where => Expr}` | `{create_index, T, [cols], #{unique => true, where => Expr}}` |
