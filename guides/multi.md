# Multi

`kura_multi` provides transaction pipelines that group multiple repo operations into a single atomic transaction. If any step fails, the entire transaction is rolled back.

## Creating a Pipeline

```erlang
Multi = kura_multi:new().
```

## Adding Steps

Each step has a unique name (atom) and an operation.

### Insert

```erlang
CS = kura_changeset:cast(my_user, #{}, #{<<"name">> => <<"Alice">>}, [name]),
M1 = kura_multi:insert(Multi, create_user, CS).
```

### Update

```erlang
CS = kura_changeset:cast(my_user, ExistingUser, #{<<"name">> => <<"Bob">>}, [name]),
M1 = kura_multi:update(Multi, update_user, CS).
```

### Delete

```erlang
CS = kura_changeset:cast(my_user, User, #{}, []),
M1 = kura_multi:delete(Multi, delete_user, CS).
```

### Run (arbitrary function)

```erlang
M1 = kura_multi:run(Multi, send_email, fun(Results) ->
    User = maps:get(create_user, Results),
    %% do something...
    {ok, sent}
end).
```

## Dynamic Steps with Functions

Steps can take a function that receives previous results, enabling dependent operations:

```erlang
Multi = kura_multi:new(),
M1 = kura_multi:insert(Multi, create_user,
    kura_changeset:cast(my_user, #{}, #{<<"name">> => <<"Alice">>}, [name])),
M2 = kura_multi:insert(M1, create_profile, fun(#{create_user := User}) ->
    kura_changeset:cast(my_profile, #{}, #{<<"user_id">> => maps:get(id, User)}, [user_id])
end).
```

## Executing

```erlang
case kura_repo_worker:multi(my_repo, Multi) of
    {ok, Results} ->
        %% Results is a map of step_name => result
        User = maps:get(create_user, Results),
        Profile = maps:get(create_profile, Results);
    {error, FailedStep, Value, Completed} ->
        %% FailedStep: atom name of the failed step
        %% Value: the error value
        %% Completed: map of successfully completed steps before failure
        io:format("Step ~p failed: ~p~n", [FailedStep, Value])
end.
```

## Appending Multis

Combine two multi pipelines:

```erlang
Combined = kura_multi:append(Multi1, Multi2).
```

Operations from `Multi2` are appended after `Multi1`. Step names must be unique across both.

## Error Handling

- If a step returns `{error, Value}`, the transaction is rolled back
- For insert/update/delete steps, errors come from changeset validation or DB constraint violations
- For `run` steps, your function must return `{ok, Result}` or `{error, Reason}`
- Duplicate step names raise an error at build time: `{duplicate_step, Name}`
