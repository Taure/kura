# Optimistic Locking

Optimistic locking prevents lost updates when multiple processes try to modify the same record concurrently. Instead of acquiring database locks, it checks that the record has not changed since it was read.

## How It Works

1. Your schema has a `lock_version` field (integer, defaults to `0`)
2. When you update, `optimistic_lock/2` increments `lock_version` in the changeset
3. The generated `UPDATE` query includes `WHERE lock_version = <old_value>`
4. If another process updated the record first (changing `lock_version`), the `WHERE` clause matches zero rows and Kura returns `{error, stale}`

## Schema Setup

Add a `lock_version` integer field to your schema:

```erlang
-module(my_product).
-behaviour(kura_schema).

-export([table/0, fields/0]).

table() -> ~"products".

fields() ->
    #{
        id => #{type => integer, primary_key => true},
        name => #{type => string},
        price => #{type => integer},
        lock_version => #{type => integer, default => 0}
    }.
```

Make sure the corresponding migration includes the column:

```erlang
kura_migration:create_table(~"products", [
    {~"id", ~"serial PRIMARY KEY"},
    {~"name", ~"text NOT NULL"},
    {~"price", ~"integer NOT NULL"},
    {~"lock_version", ~"integer NOT NULL DEFAULT 0"}
]).
```

## Using Optimistic Locking

Apply `kura_changeset:optimistic_lock/2` to your changeset before updating:

```erlang
update_price(Repo, Product, NewPrice) ->
    CS = kura_changeset:cast(my_product, Product, #{~"price" => NewPrice}, [price]),
    CS1 = kura_changeset:optimistic_lock(CS, lock_version),
    kura_repo_worker:update(Repo, CS1).
```

`optimistic_lock/2` does two things:

- Puts a change that increments the lock field (e.g., `lock_version` goes from `0` to `1`)
- Marks the changeset so that the repo worker generates a `WHERE lock_version = 0` clause on the UPDATE

## Handling Stale Records

When a conflict is detected, the update returns `{error, stale}`. You typically want to re-fetch the record and retry or inform the user:

```erlang
case update_price(my_repo, Product, 2999) of
    {ok, Updated} ->
        Updated;
    {error, stale} ->
        %% Record was modified by another process since we read it.
        %% Re-fetch and decide what to do.
        {ok, Fresh} = kura_repo_worker:get(my_repo, my_product, maps:get(id, Product)),
        %% Retry or return a conflict error to the caller
        update_price(my_repo, Fresh, 2999)
end.
```

## Concurrent Update Example

Consider two processes reading the same product and then updating it:

```
Process A                          Process B
─────────                          ─────────
Read product (lock_version = 0)
                                   Read product (lock_version = 0)
Update price to 2999
  UPDATE ... SET lock_version=1
  WHERE lock_version=0
  → {ok, Updated}
                                   Update price to 3999
                                     UPDATE ... SET lock_version=1
                                     WHERE lock_version=0
                                     → {error, stale}
                                     (no rows matched!)
```

Process B's update fails because `lock_version` is now `1` in the database, not `0` as expected. This prevents Process B from silently overwriting Process A's change.

## Important Notes

- The lock field can be any integer field, not just `lock_version` -- pass whatever atom your schema uses to `optimistic_lock/2`.
- Optimistic locking only applies to updates. Inserts and deletes are unaffected.
- If you forget to call `optimistic_lock/2`, the update proceeds without the lock check (standard behavior).
- This approach works best when conflicts are rare. If conflicts are frequent, consider pessimistic locking (`SELECT ... FOR UPDATE`) via `kura_query:lock/2` instead.
