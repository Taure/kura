# Enum Types

Kura supports enum fields stored as `VARCHAR(255)` in PostgreSQL. No `CREATE TYPE` is needed -- values are validated at the application level.

## Defining Enum Fields

Add an enum field to your schema with `{enum, [atom()]}`:

```erlang
fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = title, type = string},
     #kura_field{name = status, type = {enum, [draft, published, archived]}}].
```

The allowed values are atoms. The field is stored as `VARCHAR(255)` in the database.

## Casting

`kura_types:cast/2` accepts atoms, binaries, and charlists. The value is validated against the allowed list and always returned as an atom:

```erlang
kura_types:cast({enum, [draft, published]}, draft).
%% {ok, draft}

kura_types:cast({enum, [draft, published]}, <<"draft">>).
%% {ok, draft}

kura_types:cast({enum, [draft, published]}, "draft").
%% {ok, draft}

kura_types:cast({enum, [draft, published]}, <<"unknown">>).
%% {error, <<"is not a valid enum value">>}
```

Binary and charlist inputs are converted using `binary_to_existing_atom/2` and `list_to_existing_atom/1` respectively. If the atom doesn't already exist in the VM, casting fails.

## Dump (Erlang to PostgreSQL)

Atoms are converted to binaries for storage:

```erlang
kura_types:dump({enum, [draft, published]}, draft).
%% {ok, <<"draft">>}
```

## Load (PostgreSQL to Erlang)

Binaries from the database are converted back to atoms:

```erlang
kura_types:load({enum, [draft, published]}, <<"draft">>).
%% {ok, draft}
```

Load uses `binary_to_existing_atom/2` first, falling back to `binary_to_atom/2` if the atom doesn't exist yet. This fallback ensures data migration safety -- if you remove a value from your enum list, existing rows can still be loaded without crashing.

## Querying

Since enums are stored as `VARCHAR(255)`, use binary values in where clauses:

```erlang
Q = kura_query:from(post),
Q1 = kura_query:where(Q, {status, <<"published">>}),
{ok, Posts} = my_repo:all(Q1).
```

## Migration Behavior

Enum fields generate `VARCHAR(255)` columns. Adding or removing values from the enum list in your schema doesn't require a new migration since the underlying column type stays the same.

In `rebar3_kura`, the `types_equal/2` function treats all `{enum, _}` tuples as equal:

```erlang
types_equal({enum, _}, {enum, _}) -> true;
types_equal(A, B) -> A =:= B.
```

This prevents no-op `ALTER TABLE` statements when the only difference is the enum value list.
