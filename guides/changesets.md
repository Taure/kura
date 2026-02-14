# Changesets

Changesets are the core data validation layer. They cast external parameters, track changes against existing data, validate fields, and declare database constraints.

## Creating a Changeset

Use `kura_changeset:cast/4` to create a changeset from a schema module, existing data, external params, and a list of allowed fields:

```erlang
%% New record (empty data)
CS = kura_changeset:cast(my_user, #{}, Params, [name, email, age]).

%% Existing record (update)
CS = kura_changeset:cast(my_user, ExistingUser, Params, [name, email]).
```

Parameters can have binary or atom keys — they are normalized automatically. Only fields in the allowed list are cast. Values are automatically cast to the type defined in the schema.

## Validations

### Required Fields

```erlang
CS1 = kura_changeset:validate_required(CS, [name, email]).
```

Checks that each field is present and non-blank (not `undefined`, `null`, or `<<>>`).

### Format

```erlang
CS1 = kura_changeset:validate_format(CS, email, <<"^[^@]+@[^@]+$">>).
```

Validates a binary field against a regex pattern.

### Length

```erlang
CS1 = kura_changeset:validate_length(CS, name, [{min, 2}, {max, 100}]).
CS2 = kura_changeset:validate_length(CS, code, [{is, 6}]).
```

Options: `{min, N}`, `{max, N}`, `{is, N}`. Works on binaries (byte size) and lists (length).

### Number

```erlang
CS1 = kura_changeset:validate_number(CS, age, [{greater_than_or_equal_to, 0}, {less_than, 150}]).
```

Options: `{greater_than, N}`, `{less_than, N}`, `{greater_than_or_equal_to, N}`, `{less_than_or_equal_to, N}`, `{equal_to, N}`.

### Inclusion

```erlang
CS1 = kura_changeset:validate_inclusion(CS, role, [<<"admin">>, <<"user">>, <<"guest">>]).
```

### Custom Validation

```erlang
CS1 = kura_changeset:validate_change(CS, email, fun(Val) ->
    case binary:match(Val, <<"@">>) of
        nomatch -> {error, <<"must contain @">>};
        _ -> ok
    end
end).
```

## Constraint Declarations

Constraints map PostgreSQL constraint violations to friendly changeset errors on insert/update.

### Unique Constraint

```erlang
CS1 = kura_changeset:unique_constraint(CS, email).

%% With custom constraint name and message
CS1 = kura_changeset:unique_constraint(CS, email, #{
    name => <<"users_email_index">>,
    message => <<"is already registered">>
}).
```

### Foreign Key Constraint

```erlang
CS1 = kura_changeset:foreign_key_constraint(CS, team_id).
```

### Check Constraint

```erlang
CS1 = kura_changeset:check_constraint(CS, <<"users_age_check">>, age, #{
    message => <<"must be positive">>
}).
```

## Working with Changes

```erlang
%% Get a changed value
Name = kura_changeset:get_change(CS, name).
Name = kura_changeset:get_change(CS, name, <<"default">>).

%% Get the effective field value (changes take precedence over data)
Email = kura_changeset:get_field(CS, email).

%% Manually set a change
CS1 = kura_changeset:put_change(CS, role, <<"admin">>).

%% Add a custom error
CS1 = kura_changeset:add_error(CS, email, <<"is not allowed">>).
```

## Applying Changes

```erlang
%% Merge changes into data (ignores validity)
Map = kura_changeset:apply_changes(CS).

%% Apply with action check — returns error if invalid
{ok, Map} = kura_changeset:apply_action(CS, insert).
{error, CS1} = kura_changeset:apply_action(InvalidCS, insert).
```

## Error Handling

Errors are stored as `[{atom(), binary()}]` in the changeset's `errors` field. The `valid` field is `false` when any errors are present.

```erlang
case kura_repo_worker:insert(my_repo, CS) of
    {ok, Record} ->
        %% success
        Record;
    {error, #kura_changeset{errors = Errors}} ->
        %% Errors is e.g. [{email, <<"has already been taken">>}]
        Errors
end.
```
