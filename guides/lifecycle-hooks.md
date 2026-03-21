# Lifecycle Hooks

Lifecycle hooks let you run custom logic before or after insert, update, and delete operations. They are defined directly on schema modules as optional callbacks.

## Available Hooks

| Hook | Fires | Receives | Returns |
|------|-------|----------|---------|
| `before_insert/1` | Before a row is inserted | `#kura_changeset{}` | `{ok, #kura_changeset{}}` or `{error, #kura_changeset{}}` |
| `after_insert/1` | After a row is inserted | `map()` (the loaded record) | `{ok, map()}` or `{error, term()}` |
| `before_update/1` | Before a row is updated | `#kura_changeset{}` | `{ok, #kura_changeset{}}` or `{error, #kura_changeset{}}` |
| `after_update/1` | After a row is updated | `map()` (the loaded record) | `{ok, map()}` or `{error, term()}` |
| `before_delete/1` | Before a row is deleted | `map()` (the existing record) | `ok` or `{error, term()}` |
| `after_delete/1` | After a row is deleted | `map()` (the deleted record) | `ok` or `{error, term()}` |

All hooks are optional callbacks on `kura_schema`. If a hook is not defined, the operation proceeds as normal.

## Implementing Hooks

Export the hook functions from your schema module:

```erlang
-module(my_article).
-behaviour(kura_schema).

-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).
-export([before_insert/1, after_insert/1, before_delete/1]).

table() -> <<"articles">>.

fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = title, type = string},
     #kura_field{name = slug, type = string},
     #kura_field{name = published, type = boolean},
     #kura_field{name = inserted_at, type = utc_datetime},
     #kura_field{name = updated_at, type = utc_datetime}].

before_insert(CS) ->
    Title = kura_changeset:get_change(CS, title, <<>>),
    Slug = slugify(Title),
    {ok, kura_changeset:put_change(CS, slug, Slug)}.

after_insert(Record) ->
    logger:info("Article created: ~p", [maps:get(id, Record)]),
    {ok, Record}.

before_delete(Record) ->
    case maps:get(published, Record) of
        true -> {error, cannot_delete_published};
        false -> ok
    end.
```

You only need to implement the hooks you care about. There is no need to define no-op hooks.

## Before Hooks

Before hooks receive either a changeset (`before_insert`, `before_update`) or the existing record (`before_delete`). They run after changeset validations pass but before the SQL query executes.

### Modifying the changeset

`before_insert` and `before_update` can modify the changeset before it reaches the database. This is useful for setting derived fields:

```erlang
before_insert(CS) ->
    Password = kura_changeset:get_change(CS, password),
    Hash = hash_password(Password),
    {ok, kura_changeset:put_change(CS, password_hash, Hash)}.
```

### Rejecting an operation

Return `{error, Changeset}` with errors added to reject the operation:

```erlang
before_update(CS) ->
    case kura_changeset:get_change(CS, role) of
        <<"superadmin">> ->
            {error, kura_changeset:add_error(CS, role, <<"cannot assign superadmin">>)};
        _ ->
            {ok, CS}
    end.
```

For `before_delete`, return `{error, Reason}` with any term:

```erlang
before_delete(Record) ->
    case maps:get(status, Record) of
        <<"active">> -> {error, cannot_delete_active_record};
        _ -> ok
    end.
```

## After Hooks

After hooks receive the loaded record (a map) returned from the database. They run after the SQL query succeeds.

### Triggering side effects

```erlang
after_insert(Record) ->
    notify_subscribers(Record),
    {ok, Record}.

after_update(Record) ->
    invalidate_cache(maps:get(id, Record)),
    {ok, Record}.

after_delete(Record) ->
    cleanup_attachments(maps:get(id, Record)),
    ok.
```

### Enriching the returned record

`after_insert` and `after_update` can modify the record returned to the caller:

```erlang
after_insert(Record) ->
    {ok, Record#{computed_field => derive_value(Record)}}.
```

## Error Handling and Transactions

### Automatic transactions with after hooks

When a schema defines any `after_*` hook, Kura automatically wraps the entire operation in a database transaction. This is determined by `kura_schema:has_after_hook/2` at call time.

If you only define `before_*` hooks and no `after_*` hooks, no extra transaction is created (the operation runs as a single statement).

### Rolling back on error

When an after hook returns an error, the transaction is rolled back. The insert/update/delete that already executed is undone:

```erlang
after_insert(Record) ->
    case send_welcome_email(Record) of
        ok ->
            {ok, Record};
        {error, Reason} ->
            %% The INSERT is rolled back — no row is persisted
            {error, Reason}
    end.
```

For before hooks, returning `{error, Changeset}` prevents the SQL from executing at all, so there is nothing to roll back.

### Error propagation

The error from a hook propagates directly to the caller:

```erlang
%% If before_insert returns {error, CS}, you get {error, CS}
%% If after_insert returns {error, Reason}, you get {error, Reason}
case kura_repo_worker:insert(MyRepo, CS) of
    {ok, Record} -> handle_success(Record);
    {error, #kura_changeset{} = ErrCS} -> handle_validation_error(ErrCS);
    {error, Reason} -> handle_after_hook_error(Reason)
end.
```

## Common Use Cases

**Derived fields** -- Compute slugs, hashes, or denormalized values in `before_insert`/`before_update`.

**Access control** -- Reject operations based on business rules in `before_*` hooks.

**Audit logging** -- Record who changed what in `after_insert`/`after_update`/`after_delete`.

**Cache invalidation** -- Bust caches in `after_update`/`after_delete`.

**Notifications** -- Send emails, publish PubSub messages, or enqueue jobs in `after_*` hooks.

**Cleanup** -- Remove associated files or external resources in `after_delete`.

## Best Practices

**Keep hooks fast.** Hooks run inside the request path (and possibly inside a transaction). Expensive work like sending emails should be dispatched to a background job rather than done synchronously.

**Avoid circular calls.** A hook that inserts or updates another schema with its own hooks can create unexpected chains. Be deliberate about hook interactions.

**Use before hooks for data transformation.** Deriving field values belongs in `before_insert`/`before_update`, not in changeset functions scattered across your application.

**Use after hooks for side effects.** Anything that should only happen when the database write succeeds belongs in `after_*` hooks.

**Be aware of the transaction boundary.** After hooks run inside a transaction. If your side effect fails and returns an error, the database write is rolled back. If you want fire-and-forget side effects that do not affect the write, return `{ok, Record}` or `ok` regardless of the side effect outcome.

**Prefer `kura_multi` for complex workflows.** If you need to coordinate multiple inserts/updates with rollback semantics, `kura_multi` gives you explicit control. Hooks are best for schema-local concerns.
