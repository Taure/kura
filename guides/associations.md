# Associations

Kura supports `belongs_to`, `has_one`, `has_many`, and `many_to_many` associations with preloading. This guide covers both the schema-level association definitions and the database-level foreign keys that back them.

## From SQL to Kura

A typical relational schema in SQL:

```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE posts (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    body TEXT,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    inserted_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE comments (
    id UUID PRIMARY KEY,
    body TEXT NOT NULL,
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE tags (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE posts_tags (
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);
```

In Kura, this involves two layers:

1. **Migrations** — define foreign keys, cascade rules, and indexes at the database level
2. **Schemas** — define associations for querying and preloading at the application level

### The Migration

```erlang
-module(m20260306120000_create_blog).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"users", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = name, type = string, nullable = false}
    ]},
    {create_table, ~"posts", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = title, type = string, nullable = false},
        #kura_column{name = body, type = text},
        #kura_column{name = user_id, type = uuid, nullable = false,
                     references = {~"users", id}, on_delete = cascade},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
    ]},
    {create_index, ~"posts", [user_id], #{}},
    {create_table, ~"comments", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = body, type = text, nullable = false},
        #kura_column{name = post_id, type = uuid, nullable = false,
                     references = {~"posts", id}, on_delete = cascade},
        #kura_column{name = user_id, type = uuid,
                     references = {~"users", id}, on_delete = set_null}
    ]},
    {create_index, ~"comments", [post_id], #{}},
    {create_index, ~"comments", [user_id], #{}},
    {create_table, ~"tags", [
        #kura_column{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_column{name = name, type = string, nullable = false}
    ]},
    {create_table, ~"posts_tags", [
        #kura_column{name = post_id, type = uuid, nullable = false,
                     references = {~"posts", id}, on_delete = cascade},
        #kura_column{name = tag_id, type = uuid, nullable = false,
                     references = {~"tags", id}, on_delete = cascade}
    ], [{unique, [post_id, tag_id]}]}].

down() ->
    [{drop_table, ~"posts_tags"},
     {drop_table, ~"tags"},
     {drop_table, ~"comments"},
     {drop_table, ~"posts"},
     {drop_table, ~"users"}].
```

### Foreign Key Options

The `#kura_column{}` record supports:

| Field | Type | SQL Generated |
|---|---|---|
| `references` | `{Table, Column}` | `REFERENCES "table"("column")` |
| `on_delete` | `cascade \| restrict \| set_null \| no_action` | `ON DELETE ...` |
| `on_update` | `cascade \| restrict \| set_null \| no_action` | `ON UPDATE ...` |

Common patterns:

- **`cascade`** — delete the child when the parent is deleted (posts when user is deleted)
- **`set_null`** — set the FK to NULL when the parent is deleted (keep the comment, lose the author)
- **`restrict`** — prevent deleting the parent if children exist
- **`no_action`** — like restrict, but checked at end of transaction

### The Schemas

Now define the associations at the application level:

```erlang
-module(user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"users".

fields() ->
    [#kura_field{name = id, type = uuid, primary_key = true, nullable = false},
     #kura_field{name = name, type = string, nullable = false}].

associations() ->
    [#kura_assoc{name = posts, type = has_many, schema = post, foreign_key = user_id},
     #kura_assoc{name = comments, type = has_many, schema = comment, foreign_key = user_id}].
```

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
     #kura_assoc{name = comments, type = has_many, schema = comment, foreign_key = post_id},
     #kura_assoc{name = tags, type = many_to_many, schema = tag,
                 join_through = ~"posts_tags", join_keys = {post_id, tag_id}}].

indexes() ->
    [{[user_id], #{}}].
```

```erlang
-module(comment).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"comments".

fields() ->
    [#kura_field{name = id, type = uuid, primary_key = true, nullable = false},
     #kura_field{name = body, type = text, nullable = false},
     #kura_field{name = post_id, type = uuid, nullable = false},
     #kura_field{name = user_id, type = uuid}].

associations() ->
    [#kura_assoc{name = post, type = belongs_to, schema = post, foreign_key = post_id},
     #kura_assoc{name = author, type = belongs_to, schema = user, foreign_key = user_id}].
```

## Defining Associations

### belongs_to

The current schema holds the foreign key:

```erlang
#kura_assoc{name = author, type = belongs_to, schema = my_user, foreign_key = user_id}.
```

### has_many

The related schema holds the foreign key:

```erlang
%% In my_user schema
associations() ->
    [#kura_assoc{name = posts, type = has_many, schema = my_post, foreign_key = user_id}].
```

### has_one

Like `has_many` but returns a single record (or `nil`):

```erlang
#kura_assoc{name = profile, type = has_one, schema = my_profile, foreign_key = user_id}.
```

## Preloading

### In Queries

Add preloads to a query — they are executed after the main query:

```erlang
Q = kura_query:from(my_post),
Q1 = kura_query:preload(Q, [author]),
{ok, Posts} = kura_repo_worker:all(my_repo, Q1).
%% Each post now has an `author` key with the loaded user map (or `nil`).
```

### Standalone Preloading

Preload associations on already-fetched records:

```erlang
%% Single record
Post1 = kura_repo_worker:preload(my_repo, my_post, Post, [author]).

%% List of records
Posts1 = kura_repo_worker:preload(my_repo, my_post, Posts, [author]).
```

### Nested Preloads

Preload associations on related records:

```erlang
%% Preload author, then preload the author's posts
Q = kura_query:preload(kura_query:from(my_post), [{author, [posts]}]).

%% Standalone
Post1 = kura_repo_worker:preload(my_repo, my_post, Post, [{author, [posts]}]).
```

## How Preloading Works

Preloading uses separate queries (not JOINs) for efficiency:

1. Collect all foreign key values from the parent records
2. Execute a single `WHERE fk IN (...)` query for the related records
3. Map the results back to each parent record

This avoids N+1 queries and produces clean, predictable results.

- `belongs_to`: Looks up by the parent's foreign key → related primary key
- `has_many`: Looks up by the parent's primary key → related foreign key, returns a list
- `has_one`: Same as `has_many` but returns a single record or `nil`

## Cascading Deletes

Cascade behavior is handled at the **database level** via `on_delete` on `#kura_column{}` in your migration. When you delete a parent record, PostgreSQL automatically handles the children:

```erlang
%% Deleting a user cascades to their posts (on_delete = cascade)
CS = kura_changeset:cast(user, User, #{}, []),
{ok, _} = my_repo:delete(CS).
%% All posts by this user are automatically deleted by PostgreSQL
%% Comments on those posts are also deleted (post → comment cascade)
```

No Kura code needed — the database does the work.

### Handling Foreign Key Errors

If you try to delete a record that has children with `on_delete = restrict` (or no cascade), PostgreSQL raises a foreign key violation. Use `foreign_key_constraint/2` to get a friendly error:

```erlang
delete_changeset(User) ->
    CS = kura_changeset:cast(user, User, #{}, []),
    kura_changeset:foreign_key_constraint(CS, id).

case my_repo:delete(delete_changeset(User)) of
    {ok, _} ->
        ok;
    {error, #kura_changeset{errors = [{id, ~"does not exist"}]}} ->
        %% Can't delete — children still reference this user
        {error, has_children}
end.
```

## Many-to-Many

Many-to-many associations use a join table:

```erlang
#kura_assoc{name = tags, type = many_to_many, schema = tag,
            join_through = ~"posts_tags", join_keys = {post_id, tag_id}}.
```

- `join_through` — the join table name
- `join_keys` — `{FK to self, FK to related}`

Persisting via `cast_assoc` or `put_assoc` deletes existing join rows and inserts new ones. See the [Nested Changesets](cast_assoc.md) guide for details.
