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
kura_query:where(Q, {name, <<"Alice">>}).
kura_query:where(Q, {name, '=', <<"Alice">>}).
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
kura_query:where(Q, {role, in, [<<"admin">>, <<"moderator">>]}).
kura_query:where(Q, {status, not_in, [<<"banned">>, <<"deleted">>]}).
```

### BETWEEN

```erlang
kura_query:where(Q, {age, between, {18, 65}}).
```

### LIKE / ILIKE

```erlang
kura_query:where(Q, {name, like, <<"A%">>}).
kura_query:where(Q, {email, ilike, <<"%@example.com">>}).
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
kura_query:where(Q, {'or', [{role, <<"admin">>}, {role, <<"moderator">>}]}).

%% NOT
kura_query:where(Q, {'not', {banned, true}}).
```

### Fragments

Raw SQL fragments with `?` placeholders:

```erlang
kura_query:where(Q, {fragment, <<"lower(?) = ?">>, [<<"Email">>, <<"alice@example.com">>]}).
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

```erlang
Q1 = kura_query:group_by(Q, [role]),
Q2 = kura_query:having(Q1, {count, '>', 5}).
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
kura_query:lock(Q, <<"FOR UPDATE">>).
kura_query:lock(Q, <<"FOR SHARE">>).
```

## Schema Prefix

```erlang
kura_query:prefix(Q, <<"my_schema">>).
```

## JOINs

```erlang
Q = kura_query:from(my_user),
Q1 = kura_query:join(Q, inner, posts, {user_id, id}).

%% With alias
Q1 = kura_query:join(Q, left, posts, {user_id, id}, p).
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

## Preloading (in queries)

See the [Associations](associations.md) guide for preloading details.

```erlang
Q = kura_query:from(my_post),
Q1 = kura_query:preload(Q, [author]),
{ok, Posts} = kura_repo_worker:all(my_repo, Q1).
```
