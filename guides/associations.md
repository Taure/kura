# Associations

Kura supports `belongs_to`, `has_one`, and `has_many` associations with preloading.

## Defining Associations

Add an `associations/0` callback to your schema module:

```erlang
-module(my_post).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, primary_key/0, associations/0]).

table() -> <<"posts">>.
primary_key() -> id.

fields() ->
    [#kura_field{name = id, type = id},
     #kura_field{name = title, type = string},
     #kura_field{name = user_id, type = integer}].

associations() ->
    [#kura_assoc{name = author, type = belongs_to, schema = my_user, foreign_key = user_id}].
```

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
