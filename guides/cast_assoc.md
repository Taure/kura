# Nested Changesets: cast_assoc and put_assoc

`cast_assoc` and `put_assoc` let you build child changesets alongside a parent changeset. On insert or update, Kura persists the parent and children together in a transaction.

## cast_assoc

Casts nested params from the parent changeset's params map into child changesets.

```erlang
Params = #{
    title => <<"My Post">>,
    body => <<"Hello world">>,
    comments => [
        #{body => <<"Great post!">>},
        #{body => <<"Thanks for sharing">>}
    ]
},
CS = kura_changeset:cast(post, #{}, Params, [title, body]),
CS1 = kura_changeset:cast_assoc(CS, comments).
```

### How It Works

1. Looks up the association via `kura_schema:association/2`
2. Gets nested params from the changeset params under the association name key
3. If the key is missing, returns the changeset unchanged
4. Builds child changesets using a default cast function (or a custom `with` function)
5. For `has_many`: iterates the param list, setting `action = insert` (no PK) or `action = update` (PK matches an existing record)
6. For `has_one`: single param map produces a single changeset
7. Stores results in `assoc_changes` and sets `valid = false` if any child is invalid

The default cast function includes all non-virtual, non-primary-key, non-foreign-key fields as allowed:

```erlang
%% Roughly equivalent to:
fun(Data, ChildParams) ->
    kura_changeset:cast(comment, Data, ChildParams, [body, ...])
end
```

### Custom `with` Function

Override the default casting with a custom function that receives the existing data and child params:

```erlang
WithFun = fun(Data, ChildParams) ->
    CS = kura_changeset:cast(comment, Data, ChildParams, [body]),
    kura_changeset:validate_required(CS, [body])
end,
CS1 = kura_changeset:cast_assoc(CS, comments, #{with => WithFun}).
```

### Update Detection (has_many)

For `has_many`, Kura checks whether each child param map contains the primary key:

- **No PK (or PK is `undefined`)**: builds a new changeset with `action = insert`
- **PK present**: looks up the matching record from existing data, builds a changeset against it with `action = update`

```erlang
%% Existing post with comments already loaded
ExistingPost = #{id => 1, title => <<"Old">>, comments => [
    #{id => 10, body => <<"Existing comment">>}
]},
Params = #{
    title => <<"Updated">>,
    comments => [
        #{id => 10, body => <<"Edited comment">>},  %% update (has PK)
        #{body => <<"New comment">>}                  %% insert (no PK)
    ]
},
CS = kura_changeset:cast(post, ExistingPost, Params, [title]),
CS1 = kura_changeset:cast_assoc(CS, comments).
```

## put_assoc

Directly set association changes from maps or pre-built changesets. Unlike `cast_assoc`, the values don't come from the changeset's params.

```erlang
CS = kura_changeset:cast(post, #{}, Params, [title, body]),
CS1 = kura_changeset:put_assoc(CS, comments, [
    #{body => <<"Great!">>},
    #{body => <<"Thanks!">>}
]).
```

Maps are automatically coerced into changesets. You can also pass pre-built changesets directly:

```erlang
ChildCS = kura_changeset:cast(comment, #{}, #{body => <<"Nice">>}, [body]),
CS1 = kura_changeset:put_assoc(CS, comments, [ChildCS]).
```

## Transaction Behavior

When a parent changeset has `assoc_changes`:

1. A transaction is opened
2. The parent is inserted or updated first
3. Each child changeset is persisted with the foreign key automatically set to the parent's primary key value
4. The parent row is returned with association data merged in

If `assoc_changes` is empty, the insert/update runs without transaction overhead.

## cast_assoc vs put_assoc

| | cast_assoc | put_assoc |
|---|---|---|
| Source | Params from changeset | Maps or changesets you provide |
| Casting | Automatic (or custom `with`) | Maps auto-cast, changesets used as-is |
| Use case | Form/API input processing | Programmatic association building |
