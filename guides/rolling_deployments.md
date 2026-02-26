# Rolling Deployments

## The Problem

During a rolling deployment, old and new versions of your application run
simultaneously. Migrations that remove or rename columns cause crashes on old
nodes that still reference those columns.

```text
Timeline:
  Node A (old code) ──SELECT email FROM users──▶ 💥 column does not exist
  Node B (new code) ──migration: DROP COLUMN email──▶ ✓
```

Kura detects these dangerous operations at migration time and logs warnings.

## Dangerous Operations

| Operation | Risk |
|---|---|
| `drop_column` | Old code still queries the column |
| `rename_column` | Old code still queries by original name |
| `modify_column` | Type change may be incompatible with old code |
| `drop_table` | Old code still references the table |
| `add_column` (NOT NULL, no default) | Old code inserts without the column, violating the constraint |

## The Expand-Contract Pattern

Split dangerous migrations into safe steps across multiple deployments:

1. **Expand** — Add the new structure alongside the old
2. **Deploy** — Roll out code that works with both old and new structures
3. **Contract** — Remove the old structure once all nodes use the new code

### Drop a Column

**Instead of:**

```erlang
up() -> [{alter_table, <<"users">>, [{drop_column, avatar}]}].
```

**Do this in three steps:**

1. Deploy code that stops reading/writing `avatar`
2. Run migration to drop the column:
   ```erlang
   up() -> [{alter_table, <<"users">>, [{drop_column, avatar}]}].

   safe() -> [{drop_column, avatar}].
   ```

### Rename a Column

**Instead of:**

```erlang
up() -> [{alter_table, <<"users">>, [{rename_column, name, full_name}]}].
```

**Do this:**

1. Add the new column:
   ```erlang
   up() -> [{alter_table, <<"users">>, [
       {add_column, #kura_column{name = full_name, type = string}}
   ]}].
   ```
2. Backfill data: `UPDATE users SET full_name = name`
3. Deploy code that reads from `full_name` and writes to both
4. Deploy code that only uses `full_name`
5. Drop the old column:
   ```erlang
   up() -> [{alter_table, <<"users">>, [{drop_column, name}]}].

   safe() -> [{drop_column, name}].
   ```

### Add a Required Column

**Instead of:**

```erlang
up() -> [{alter_table, <<"users">>, [
    {add_column, #kura_column{name = role, type = string, nullable = false}}
]}].
```

**Do this:**

1. Add the column as nullable:
   ```erlang
   up() -> [{alter_table, <<"users">>, [
       {add_column, #kura_column{name = role, type = string}}
   ]}].
   ```
2. Deploy code that writes `role` on all inserts
3. Backfill existing rows
4. Set NOT NULL:
   ```erlang
   up() -> [{execute, <<"ALTER TABLE users ALTER COLUMN role SET NOT NULL">>}].
   ```

### Change a Column Type

**Instead of:**

```erlang
up() -> [{alter_table, <<"users">>, [{modify_column, age, text}]}].
```

**Do this:**

1. Add a new column with the target type
2. Backfill data with a type conversion
3. Deploy code that reads from the new column
4. Drop the old column

## Suppressing Warnings

When you've followed the expand-contract pattern and are ready for the final
contraction step, implement the `safe/0` callback to suppress the warning:

```erlang
-module(m20250601120000_drop_legacy_avatar).
-behaviour(kura_migration).

up() ->
    [{alter_table, <<"users">>, [{drop_column, avatar}]}].

down() ->
    [{alter_table, <<"users">>, [
        {add_column, #kura_column{name = avatar, type = string}}
    ]}].

safe() ->
    [{drop_column, avatar}].
```

Each entry in the `safe/0` list suppresses the warning for a specific operation:

| Entry | Suppresses |
|---|---|
| `{drop_column, Col}` | Dropping column `Col` |
| `{rename_column, Col}` | Renaming column `Col` |
| `{modify_column, Col}` | Modifying column `Col` type |
| `{add_column, Col}` | Adding NOT NULL column `Col` without default |
| `drop_table` | Dropping the table |

## Warning Format

Warnings are emitted via `logger:warning/2` before each migration runs:

```text
Kura: unsafe operation in m20250601_drop_email: drop_column email on "users" —
  Deploy code that stops using the column first, then drop in a later migration
```
