# Query Builder

The query builder provides composable, functional query construction. Build queries by chaining functions, then execute with `kura_repo_worker:all/2` or `kura_repo_worker:one/2`.

## Basic Queries

```erlang
%% All users
Q = kura_query:from(my_user),
{ok, Users} = kura_repo_worker:all(my_repo, Q).

%% With conditions
Q = kura_query:from(my_user),
Q1 = kura_query:where(Q, {active, true}),
{ok, Users} = kura_repo_worker:all(my_repo, Q1).
```

## WHERE Conditions

### Equality

```erlang
kura_query:where(Q, {name, ~"Alice"}).
kura_query:where(Q, {name, '=', ~"Alice"}).
```

### Comparison Operators

```erlang
kura_query:where(Q, {age, '>', 18}).
kura_query:where(Q, {age, '>=', 18}).
kura_query:where(Q, {age, '<', 65}).
kura_query:where(Q, {age, '<=', 65}).
kura_query:where(Q, {age, '!=', 0}).
```

### IN / NOT IN

```erlang
kura_query:where(Q, {role, in, [~"admin", ~"moderator"]}).
kura_query:where(Q, {status, not_in, [~"banned", ~"deleted"]}).
```

### BETWEEN

```erlang
kura_query:where(Q, {age, between, {18, 65}}).
```

### LIKE / ILIKE

```erlang
kura_query:where(Q, {name, like, ~"A%"}).
kura_query:where(Q, {email, ilike, ~"%@example.com"}).
```

### NULL Checks

```erlang
kura_query:where(Q, {deleted_at, is_nil}).
kura_query:where(Q, {email, is_not_nil}).
```

### Boolean Combinators

```erlang
%% AND (multiple where calls are implicitly AND'd)
Q1 = kura_query:where(Q, {active, true}),
Q2 = kura_query:where(Q1, {age, '>', 18}).

%% Explicit AND
kura_query:where(Q, {'and', [{active, true}, {age, '>', 18}]}).

%% OR
kura_query:where(Q, {'or', [{role, ~"admin"}, {role, ~"moderator"}]}).

%% NOT
kura_query:where(Q, {'not', {banned, true}}).
```

### Fragments

Raw SQL fragments with `?` placeholders:

```erlang
kura_query:where(Q, {fragment, ~"lower(?) = ?", [~"Email", ~"alice@example.com"]}).
```

## SELECT

```erlang
%% Specific fields
kura_query:select(Q, [name, email]).
```

## ORDER BY

```erlang
kura_query:order_by(Q, [{name, asc}]).
kura_query:order_by(Q, [{inserted_at, desc}, {name, asc}]).
```

## LIMIT / OFFSET

```erlang
Q1 = kura_query:limit(Q, 10),
Q2 = kura_query:offset(Q1, 20).
```

## GROUP BY / HAVING

`having/2` accepts the same condition shapes as `where/2`. To filter on an
aggregate, use a fragment:

```erlang
Q1 = kura_query:group_by(Q, [role]),
Q2 = kura_query:having(Q1, {fragment, ~"count(*) > ?", [5]}).
```

A non-aggregate `HAVING` works with the regular tuple form too:

```erlang
kura_query:having(Q1, {age, '>', 5}).
```

## DISTINCT

```erlang
%% Simple DISTINCT
kura_query:distinct(Q).

%% DISTINCT ON (PostgreSQL-specific)
kura_query:distinct(Q, [email]).
```

## LOCK

```erlang
kura_query:lock(Q, ~"FOR UPDATE").
kura_query:lock(Q, ~"FOR SHARE").
```

## Schema Prefix

```erlang
kura_query:prefix(Q, ~"my_schema").
```

## JOINs

The `On` argument is `{LeftCol, RightCol}`. `LeftCol` is on the previous
table (the `from` for the first join, or the last joined table for chained
joins). `RightCol` is on the joined table.

```erlang
Q = kura_query:from(my_user),
Q1 = kura_query:join(Q, inner, my_post, {id, user_id}).
%% => SELECT ... FROM "my_user"
%%      INNER JOIN "my_post" ON "my_user"."id" = "my_post"."user_id"

%% With alias
Q1 = kura_query:join(Q, left, my_post, {id, user_id}, p).
%% => ... LEFT JOIN "my_post" AS "p" ON ...
```

For chained joins the left side automatically advances to the just-joined
table, so column names are taken from there:

```erlang
Q1 = kura_query:join(Q, inner, my_post, {id, user_id}),
Q2 = kura_query:join(Q1, inner, my_comment, {id, post_id}).
%% => ... INNER JOIN "my_comment" ON "my_post"."id" = "my_comment"."post_id"
```

Join types: `inner`, `left`, `right`, `full`.

## Aggregates

```erlang
%% COUNT(*)
Q1 = kura_query:count(Q),
{ok, [#{count := Count}]} = kura_repo_worker:all(my_repo, Q1).

%% COUNT(field)
kura_query:count(Q, email).

%% SUM / AVG / MIN / MAX
kura_query:sum(Q, amount).
kura_query:avg(Q, score).
kura_query:min(Q, price).
kura_query:max(Q, price).
```

## Vector search (pgvector)

`kura_query:distance/3` builds the distance between a `vector` or
`{vector, N}` field and a query vector. Use it in `order_by/2` for a
nearest-neighbour search and in `select_expr/2` to return the distance.

```erlang
D = kura_query:distance(embedding, cosine, QueryVector),
Q = kura_query:from(memory),
Q1 = kura_query:select_expr(Q, [{id, id}, {content, content}, {dist, D}]),
Q2 = kura_query:order_by(Q1, [{D, asc}]),
Q3 = kura_query:limit(Q2, 10),
{ok, Rows} = kura_repo_worker:all(my_repo, Q3).
%% SELECT "id" AS "id", "content" AS "content", "embedding" <=> $1 AS "dist"
%% FROM "memories" ORDER BY "embedding" <=> $2 ASC LIMIT $3
```

| Metric | Operator | Index opclass |
|---|---|---|
| `l2` | `<->` | `vector_l2_ops` |
| `cosine` | `<=>` | `vector_cosine_ops` |
| `inner_product` | `<#>` | `vector_ip_ops` |

For every metric a smaller value is nearer. `inner_product` is the
negative inner product, as pgvector returns it. Order `asc` for the
nearest rows first; `desc` cannot use an HNSW or IVFFlat index.

The query vector is a list of numbers. `distance/3` casts it and dumps it
to pgvector's text form straight away, so the query holds a binary and
binds it as one parameter. It raises on an unknown metric, an empty list
or a list holding anything but numbers.

`order_by/2` and `select_expr/2` check a distance against the schema:

- the query must be `from/1` a schema, not a table name;
- the field must be a `vector` or `{vector, N}` field;
- for `{vector, N}`, the query vector must have `N` elements.

Not supported yet: a distance in `where` or `having` (a threshold), in
`group_by/2` or in a window from `over/2`. Each raises `{kura,
{distance_not_supported_here, Clause}}`. Keyset pagination over a
distance with `kura_paginator`, which builds its own `ORDER BY`, is not
supported either. The SQLite backend has no distance operators.

### Installing the extension

kura does not create the extension, and a migration should not either.
pgvector is not a trusted extension, so the role an app connects as
usually cannot create it, on CloudNativePG and on managed Postgres alike.
Run `CREATE EXTENSION IF NOT EXISTS vector` as a superuser when the
cluster or database is set up, for example in CNPG's
`postInitApplicationSQL`.

Until `create_index` takes an access method and an operator class, add
an HNSW index with an `execute` step in a migration:

```erlang
{execute, ~"CREATE INDEX memories_embedding_cos_index ON memories USING hnsw (embedding vector_cosine_ops)"}
```

### Search-time settings

HNSW recall depends on `hnsw.ef_search` (default 40). A filtered search,
a `where` on a tenant or any other column, can return fewer rows than the
`limit` unless `hnsw.iterative_scan` (pgvector 0.8+) is on. Set both with
`SET LOCAL` inside a transaction, so they last until the transaction
ends and never reach the next query that checks out the connection:

```erlang
kura_repo_worker:transaction(my_repo, fun() ->
    {ok, _} = kura_repo_worker:query(my_repo, ~"SET LOCAL hnsw.ef_search = 100", []),
    {ok, _} = kura_repo_worker:query(my_repo, ~"SET LOCAL hnsw.iterative_scan = relaxed_order", []),
    kura_repo_worker:all(my_repo, Q3)
end).
```

## Preloading (in queries)

See the [Associations](associations.md) guide for preloading details.

```erlang
Q = kura_query:from(my_post),
Q1 = kura_query:preload(Q, [author]),
{ok, Posts} = kura_repo_worker:all(my_repo, Q1).
```
