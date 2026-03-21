# Audit Trail

Kura provides built-in audit trail support via `kura_audit` and `kura_audit_log`. Every insert, update, and delete on an audited schema is recorded in an `audit_log` table with full before/after snapshots and a diff of what changed.

## Setup

### 1. Create the audit_log migration

The `kura_audit` module ships migration helpers that create the table and indexes for you:

```erlang
-module(m20260320120000_create_audit_log).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() -> kura_audit:migration_up().
down() -> kura_audit:migration_down().
```

This creates the `audit_log` table with the following columns:

| Column | Type | Description |
|---|---|---|
| `id` | `id` | Primary key |
| `table_name` | `string` | Source table name |
| `record_id` | `string` | Primary key of the affected record |
| `action` | `string` | `"insert"`, `"update"`, or `"delete"` |
| `old_data` | `jsonb` | Full record before the change (updates, deletes) |
| `new_data` | `jsonb` | Full record after the change (inserts, updates) |
| `changes` | `jsonb` | Diff of changed fields (updates only) |
| `actor` | `string` | Who made the change |
| `metadata` | `jsonb` | Additional context (IP address, request ID, etc.) |
| `inserted_at` | `utc_datetime` | When the audit entry was created |

Four indexes are created automatically: `(table_name, record_id)`, `(action)`, `(actor)`, and `(inserted_at)`.

### 2. Run the migration

```erlang
{ok, _} = kura_migrator:migrate(my_repo).
```

## The kura_audit_log Schema

`kura_audit_log` is a standard `kura_schema` behaviour module that backs the `audit_log` table. You can query it like any other schema:

```erlang
%% Find all changes to a specific record
Q = kura_query:from(kura_audit_log),
Q1 = kura_query:where(Q, #{table_name => <<"users">>, record_id => <<"42">>}),
{ok, Entries} = my_repo:all(Q1).

%% Find all deletes by a specific actor
Q = kura_query:from(kura_audit_log),
Q1 = kura_query:where(Q, #{action => <<"delete">>, actor => <<"admin-1">>}),
{ok, Entries} = my_repo:all(Q1).
```

## Integrating with Schema Hooks

To audit a schema, export the relevant lifecycle hooks and delegate to `kura_audit`:

```erlang
-module(my_item).
-behaviour(kura_schema).

-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).
-export([before_update/1, after_insert/1, after_update/1, after_delete/1]).

table() -> <<"items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = value, type = integer},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

before_update(CS) ->
    kura_audit:stash(CS),
    {ok, CS}.

after_insert(Record) ->
    kura_audit:log(my_repo, ?MODULE, insert, Record),
    {ok, Record}.

after_update(Record) ->
    kura_audit:log(my_repo, ?MODULE, update, Record),
    {ok, Record}.

after_delete(Record) ->
    kura_audit:log(my_repo, ?MODULE, delete, Record),
    ok.
```

Each hook serves a specific purpose:

- **`before_update/1`** -- Calls `kura_audit:stash/1` to capture the old record data in the process dictionary before the UPDATE executes. This is required for diff computation.
- **`after_insert/1`** -- Logs the new record. The audit entry stores the full record in `new_data`.
- **`after_update/1`** -- Logs the updated record. The audit entry stores `old_data`, `new_data`, and a `changes` diff.
- **`after_delete/1`** -- Logs the deleted record. The audit entry stores the full record in `old_data`.

## Actor Context

The actor context identifies *who* made a change. It is stored in the process dictionary and automatically attached to every audit log entry.

### Setting the actor

```erlang
%% Simple: just an actor ID
kura_audit:set_actor(<<"user-123">>).

%% With metadata (IP, request ID, etc.)
kura_audit:set_actor(<<"user-123">>, #{
    ip => <<"192.168.1.1">>,
    request_id => <<"req-abc-456">>
}).
```

### Reading and clearing

```erlang
%% Returns the actor binary, or undefined if not set
Actor = kura_audit:get_actor().

%% Remove actor context from the process
kura_audit:clear_actor().
```

### Scoped actor with `with_actor`

`with_actor/2,3` sets the actor for the duration of a function, then restores the previous actor (or clears it). This is the recommended approach for request handlers:

```erlang
%% In a Nova controller
handle_delete(#{auth := #{user_id := UserId}}, _State) ->
    kura_audit:with_actor(UserId, fun() ->
        CS = kura_changeset:cast(my_item, Record, #{}, []),
        {ok, _} = my_repo:delete(CS)
    end),
    {json, 200, #{}, #{status => <<"deleted">>}}.

%% With metadata
kura_audit:with_actor(<<"admin-1">>, #{reason => <<"cleanup">>}, fun() ->
    {ok, _} = my_repo:delete(CS)
end).
```

The previous actor is always restored in an `after` block, so it is safe even if the function raises.

## How Update Diffs Work

For updates, the audit trail computes a field-level diff showing exactly what changed.

The mechanism uses two steps:

1. **Stash** -- In `before_update/1`, `kura_audit:stash/1` saves the changeset's current `data` (the old record) in the process dictionary, keyed by schema module.

2. **Diff** -- In `after_update/1`, `kura_audit:log/4` retrieves the stashed data and calls `compute_diff/2`. This compares each field in the old and new records, producing a map of only the fields that changed:

```erlang
%% If name changed from "foo" to "bar" and value stayed the same:
#{name => #{old => <<"foo">>, new => <<"bar">>}}
```

If `stash/1` was not called (i.e., `before_update/1` is missing), the update is still logged but without `old_data` or `changes` -- only `new_data` is recorded.

Virtual fields are automatically excluded from audit data.

## JSON-Safe Serialization

Since audit data is stored as JSONB, all values are sanitized before insertion:

- **Datetimes** (`{{Y,M,D},{H,Mi,S}}`) are converted to ISO 8601 strings: `"2026-03-20T12:00:00Z"`
- **Dates** (`{Y,M,D}`) are converted to `"2026-03-20"`
- **Atoms** (other than `true`, `false`, `null`) are converted to binary strings
- **`undefined`** is converted to `null`
- **Nested maps and lists** are recursively sanitized

This means you do not need to worry about Erlang-specific types breaking JSON encoding.

## Best Practices

- **Always set actor context.** Without it, audit entries have `null` for the actor field, making it impossible to trace who made a change.

- **Use `with_actor/2,3` for request scope.** This ensures the actor is always cleaned up, even on errors. Set it once at the top of your request handler or middleware.

- **Always implement `before_update/1` with `stash/1`.** Without it, updates lose the old data and diff. The insert and delete hooks alone are not sufficient for full audit coverage.

- **Keep metadata small.** The metadata field is useful for request IDs, IP addresses, or reason codes. Avoid putting large payloads in it.

- **Query audit logs with indexes in mind.** The default indexes cover `(table_name, record_id)`, `action`, `actor`, and `inserted_at`. Filter by these columns for efficient lookups.

- **Consider retention.** Audit logs grow indefinitely. Plan a retention strategy (e.g., archiving old entries, partitioning by date) for production systems.
