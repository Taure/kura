## Kura v1.9.0 — Lifecycle Hooks, Audit Trail, Multitenancy, and more

It's been a busy few weeks since the initial release. Kura has grown from a basic Ecto-style database layer into something with real production features. Here's what's new in v1.9.0.

### Lifecycle Hooks

Schema modules can now define `before_insert`, `after_insert`, `before_update`, `after_update`, `before_delete`, and `after_delete` callbacks. These run inside the same transaction as the operation, so returning an error rolls everything back.

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, after_insert/1]).

table() -> ~"users".
fields() -> [
    #kura_field{name = id, type = id, primary_key = true},
    #kura_field{name = name, type = string},
    #kura_field{name = email, type = string}
].

after_insert(Record) ->
    logger:info("User created: ~p", [maps:get(id, Record)]),
    {ok, Record}.
```

### Audit Trail

Built on top of lifecycle hooks, `kura_audit` tracks inserts, updates, and deletes in an `audit_log` table. It captures who made the change (actor context), what changed (automatic diff computation for updates), and stores everything as JSON.

```erlang
%% Set the actor for the current request
kura_audit:set_actor(~"user-123", #{ip => ~"10.0.0.1"}).

%% In your schema hooks:
before_update(CS) ->
    kura_audit:stash(CS),  %% capture old data for diff
    {ok, CS}.

after_insert(Record) ->
    kura_audit:log(my_repo, ?MODULE, insert, Record),
    {ok, Record}.
```

Migration setup is one line: `kura_audit:migration_up()` returns the DDL operations.

### Multitenancy

Two strategies out of the box:

**Schema prefix** — each tenant gets a PostgreSQL schema:

```erlang
kura_tenant:set_tenant({prefix, ~"tenant_abc"}).
%% All queries now target the tenant_abc schema
```

**Attribute-based** — shared table with automatic tenant_id filtering:

```erlang
kura_tenant:set_tenant({attribute, {org_id, 42}}).
%% Queries get WHERE org_id = 42, inserts get org_id set automatically
```

### Pagination

Both offset-based and cursor-based (keyset) pagination via `kura_paginator`:

```erlang
%% Offset
{ok, Page} = kura_paginator:paginate(my_repo, Q, #{page => 2, page_size => 20}).
%% #{entries => [...], total_entries => 150, total_pages => 8, ...}

%% Cursor (efficient for large datasets)
{ok, Page} = kura_paginator:cursor_paginate(my_repo, Q, #{limit => 20, after => LastCursor}).
%% #{entries => [...], has_next => true, start_cursor => ..., end_cursor => ...}
```

### Streaming

Process large result sets in batches without loading everything into memory, using PostgreSQL server-side cursors:

```erlang
kura_stream:stream(my_repo, Q, fun(Batch) ->
    [process(Row) || Row <- Batch],
    ok
end, #{batch_size => 500}).
```

### Optimistic Locking

Detect concurrent update conflicts with a version field:

```erlang
CS = kura_changeset:cast(my_schema, Record, Params, [name, email]),
CS1 = kura_changeset:optimistic_lock(CS, lock_version),
case my_repo:update(CS1) of
    {ok, Updated} -> Updated;
    {error, stale} -> %% someone else updated first
end.
```

### Other improvements

- **eqWAlizer clean** — removed 19 of 28 type suppressions with proper typed helpers. The remaining 9 are genuine eqWAlizer limitations (pgo returns `any()`, union clause narrowing limits).
- **ELP lint clean** — zero warnings across the entire codebase, migrated all binary strings to OTP 28 sigil syntax (`~""`).
- **21 guides** in the documentation covering every feature.
- Requires **OTP 28+** (up from 27).

### Links

- GitHub: https://github.com/Taure/kura
- Hex: https://hex.pm/packages/kura
- Docs: https://hexdocs.pm/kura

Feedback welcome as always.
