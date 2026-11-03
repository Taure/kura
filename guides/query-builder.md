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

A value compared to a field of the `from` schema is converted only where it
then binds the value the database would parse from it: an enum atom binds
its binary, and a binary in cast form binds the value it spells on an
`id`, `integer`, `bigint`, `smallint`, `float`, `boolean`, `date` or
`time` field (`~"42"` for an integer), or on a datetime without an offset
other than `Z` and without fractional seconds (`~"2026-01-01T00:00:00Z"`).
Everything else is bound as given: decimal, jsonb, embed, string, uuid,
array and custom fields, values the type rejects, and
fields of joined schemas, CTEs and bare table names. Conditions from
`having/2`, preloads and many-to-many lookups are not converted.

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

Once a query has a join, a bare field of the `from` schema is qualified
with the `from` table in `select`, `where`, `having`, `order_by`,
`group_by` and `distinct`, so a column both tables have (`id`,
`inserted_at`) names the `from` table's column. A name the `from` schema
does not declare is left bare, for the database to find in the joined
tables; it must be unique among them. Under attribute tenancy the tenant
field stays bare too, so a join of two tables that both carry it is still
refused (see the [Multitenancy](multitenancy.md) guide). An `order_by`
name that is a `select_expr/2` alias stays bare, so it keeps naming the
expression. To read a column of a joined table
that the `from` schema also has, select it with a fragment:

```erlang
Q = kura_query:join(kura_query:from(post), inner, user, {author_id, id}),
Q1 = kura_query:select_expr(Q, [
    {id, id},
    {title, title},
    {author_id_check, {fragment, ~"\"users\".\"id\"", []}}
]),
Q2 = kura_query:where(Q1, {id, PostId}).
%% => SELECT "posts"."id" AS "id", "posts"."title" AS "title", "users"."id" AS ...
%%      FROM "posts" INNER JOIN "users" ON "posts"."author_id" = "users"."id"
%%      WHERE "posts"."id" = $1
```

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
Q2 = kura_query:order_by(Q1, [{dist, asc}]),
Q3 = kura_query:limit(Q2, 10),
{ok, Rows} = kura_repo_worker:all(my_repo, Q3).
%% SELECT "id" AS "id", "content" AS "content", "embedding" <=> $1 AS "dist"
%% FROM "memories" ORDER BY "dist" ASC LIMIT $2
```

When the distance is selected, order by its alias as above. The vector
is sent once, and Postgres matches the alias to the select expression,
so an HNSW or IVFFlat index still serves the ordering. Ordering by `D`
itself works too, and is the form to use when the distance is not
selected; with both, the vector is sent twice.

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
binds it as one parameter. It raises on an unknown metric, an empty list,
a list holding anything but numbers, or more than 16000 dimensions.

A query embedding appears in telemetry and query-log params like any
other bound value, and an embedding can be partly inverted back to the
text it encodes, so treat those params as you would the text itself.

### Distance thresholds

`where/2` and `having/2` take `{Distance, Op, Threshold}`, where `Op` is
`'<'`, `'<='`, `'>'` or `'>='` and `Threshold` is a number. It nests
under `'and'`, `'or'` and `'not'` like any other condition.

```erlang
D = kura_query:distance(embedding, cosine, QueryVector),
Q = kura_query:select_expr(kura_query:from(memory), [{id, id}, {dist, D}]),
Q1 = kura_query:where(Q, {D, '<', 0.35}),
Q2 = kura_query:limit(kura_query:order_by(Q1, [{dist, asc}]), 10).
%% SELECT "id" AS "id", "embedding" <=> $1 AS "dist" FROM "memories"
%% WHERE "embedding" <=> $2 < $3 ORDER BY "dist" ASC LIMIT $4
```

The vector is sent once for each place the distance appears. The query
cache keys on the shape, so searches that differ only in the vector and
the threshold share one compiled entry.

A threshold alone does not use an HNSW or IVFFlat index: the index serves
an `ORDER BY` distance with a `limit`. With both, the index returns the
nearest candidates and the threshold filters them afterwards, so a tight
threshold can leave fewer rows than the `limit`, or none. With
`hnsw.iterative_scan` on (see search-time settings below) the scan
continues until it has `limit` rows or has visited `hnsw.max_scan_tuples`
tuples (20,000 by default); it does not stop when the distances pass the
threshold, so a threshold that few rows meet costs the full scan budget.
For IVFFlat with `ivfflat.iterative_scan` on, the limit is
`ivfflat.max_probes`, the most lists the scan probes.
pgvector's recommended form for a distance filter is a materialized CTE
that orders by distance with a `limit`, with the threshold applied
outside it. The query builder cannot express that yet.

### Checks

`order_by/2`, `select_expr/2`, `where/2` and `having/2` check a distance
against the schema:

- the query must be `from/1` a schema, not a table name;
- the field must be a `vector` or `{vector, N}` field;
- for `{vector, N}`, the query vector must have `N` elements.

A threshold that is not a number raises `{kura,
{invalid_distance_threshold, Field}}`.

Not supported: a distance in any other condition of `where` or `having`
(another operator, `between`, or a value or list element of a
condition), in `group_by/2`, `distinct/2`, `select/2` (and the aggregate
helpers), `count/2`, a `join` column or a window from `over/2`. Each
raises `{kura, {distance_not_supported_here, Clause}}`. The SQLite backend
has no distance operators. From the release after kura_sqlite 1.0.1,
kura_sqlite raises `{unsupported, vector_distance}` on a query that holds
one; earlier releases fail with a SQLite syntax error.

A distance is checked like its column by the encrypted-field guard. Over
a CTE whose encrypted columns cannot be named, every column may hold
ciphertext, so `order_by/2` raises `{kura, {encrypted_field_in_order_by,
Field}}`, `select_expr/2` raises `{kura, {encrypted_field_in_select,
Field}}`, and a threshold in `where/2` or `having/2` raises `{kura,
{encrypted_field_in_where, Field}}` or `{kura, {encrypted_field_in_having,
Field}}`. The reason names the field, never the query vector.

`kura_paginator:cursor_paginate/3` replaces the query's `order_by` with
its cursor field, so it cannot page through nearest neighbours. A
distance in `order_by` raises `{kura, {distance_not_supported_here,
cursor_paginate}}`; an `order_by` on the alias of a selected distance is
replaced like any other ordering. `kura_paginator:paginate/3` keeps the
ordering.

### Installing the extension

Distance queries require pgvector >= 0.8.2 (CVE-2026-3172). Run `ALTER
EXTENSION vector UPDATE` after upgrading the binary, since the server
keeps the old extension version until then.

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
a `where` on a tenant or any other column, can return fewer rows than
the `limit` unless `hnsw.iterative_scan` (pgvector 0.8+) is on. For a
distance threshold it helps only within `hnsw.max_scan_tuples`, as
described above. Set both with
`SET LOCAL` inside a transaction, so they last until the transaction
ends and never reach the next query that checks out the connection:

```erlang
kura_repo_worker:transaction(my_repo, fun() ->
    {ok, _} = kura_repo_worker:query(my_repo, ~"SET LOCAL hnsw.ef_search = 100", []),
    {ok, _} = kura_repo_worker:query(my_repo, ~"SET LOCAL hnsw.iterative_scan = strict_order", []),
    kura_repo_worker:all(my_repo, Q3)
end).
```

Use `strict_order`. `relaxed_order` scans faster but can return rows
slightly out of distance order, and is only safe when the results are
sorted again afterwards, which pgvector does with a materialized CTE
that the query builder does not express.

## Preloading (in queries)

See the [Associations](associations.md) guide for preloading details.

```erlang
Q = kura_query:from(my_post),
Q1 = kura_query:preload(Q, [author]),
{ok, Posts} = kura_repo_worker:all(my_repo, Q1).
```
