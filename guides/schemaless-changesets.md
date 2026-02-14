# Schemaless Changesets

Schemaless changesets let you validate data without defining a schema module. They are useful for validation-only scenarios like login forms, search filters, or API input where no database table is involved.

## Creating a Schemaless Changeset

Pass a types map as the first argument to `cast/4` instead of a schema module:

```erlang
Types = #{email => string, password => string},
CS = kura_changeset:cast(Types, #{}, Params, [email, password]).
```

The types map uses the same type atoms as schema field definitions (`string`, `integer`, `boolean`, `float`, etc.).

## Validation

All existing validators work on schemaless changesets:

```erlang
Types = #{email => string, password => string},
CS = kura_changeset:cast(Types, #{}, Params, [email, password]),
CS1 = kura_changeset:validate_required(CS, [email, password]),
CS2 = kura_changeset:validate_format(CS1, email, <<"@">>),
CS3 = kura_changeset:validate_length(CS2, password, [{min, 8}]).
```

## Applying Changes

Use `apply_action/2` to extract the validated data:

```erlang
case kura_changeset:apply_action(CS, :validate) of
    {ok, Data} ->
        %% Data is a map with changes merged into the base data
        Data;
    {error, CS1} ->
        %% CS1 has errors
        CS1#kura_changeset.errors
end.
```

## Constraints

Constraint declarations (`unique_constraint`, `foreign_key_constraint`) require an explicit `name` option on schemaless changesets, since there is no schema table to derive the default name from:

```erlang
%% This works
CS1 = kura_changeset:unique_constraint(CS, email, #{name => <<"users_email_key">>}).

%% This raises an error — no table to derive the constraint name
CS1 = kura_changeset:unique_constraint(CS, email).
```

`check_constraint/3,4` works unchanged since it always requires an explicit constraint name.

## Examples

### Login Form

```erlang
login(Params) ->
    Types = #{email => string, password => string},
    CS = kura_changeset:cast(Types, #{}, Params, [email, password]),
    CS1 = kura_changeset:validate_required(CS, [email, password]),
    CS2 = kura_changeset:validate_format(CS1, email, <<"@">>),
    case kura_changeset:apply_action(CS2, validate) of
        {ok, #{email := Email, password := Password}} ->
            authenticate(Email, Password);
        {error, Changeset} ->
            {error, Changeset}
    end.
```

### Search Params

```erlang
parse_search(Params) ->
    Types = #{q => string, page => integer, per_page => integer},
    CS = kura_changeset:cast(Types, #{page => 1, per_page => 20}, Params, [q, page, per_page]),
    CS1 = kura_changeset:validate_number(CS, page, [{greater_than, 0}]),
    CS2 = kura_changeset:validate_number(CS1, per_page, [{greater_than, 0}, {less_than_or_equal_to, 100}]),
    kura_changeset:apply_action(CS2, validate).
```

## Limitations

- Schemaless changesets **cannot be persisted** via `kura_repo` — they are validation-only
- Association casting (`cast_assoc`, `put_assoc`) is not supported on schemaless changesets
- The `schema` field on the changeset record is `undefined`
