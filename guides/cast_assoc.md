# Nested Changesets: cast_assoc and put_assoc

`cast_assoc` and `put_assoc` let you build child changesets alongside a parent changeset. On insert or update, Kura persists the parent and children together in a transaction.

## cast_assoc

Casts nested params from the parent changeset's params map into child changesets.

```erlang
Params = #{
    title => ~"My Post",
    body => ~"Hello world",
    comments => [
        #{body => ~"Great post!"},
        #{body => ~"Thanks for sharing"}
    ]
},
CS = kura_changeset:cast(post, #{}, Params, [title, body]),
CS1 = kura_changeset:cast_assoc(CS, comments).
```

### How It Works

1. Looks up the association via `kura_schema:association/2`
2. Gets nested params from the changeset params under the association name key
3. If the key is missing, returns the changeset unchanged
4. Builds child changesets from the `cast` list or the `with` function you pass, or from every castable field when you pass neither
5. For `has_many`: iterates the param list, setting `action = insert` (no PK) or `action = update` (PK matches an existing record)
6. For `has_one`: single param map produces a single changeset
7. Stores results in `assoc_changes` and sets `valid = false` if any child is invalid

### Say Which Fields the Params May Set

Nested params usually arrive in a request, so name the child fields a
request is allowed to set. Either list them:

```erlang
CS1 = kura_changeset:cast_assoc(CS, comments, #{cast => [body]}).
```

or hand the child to its own changeset function, which can validate it
too. `with` takes `{Module, Function}`, called as
`Module:Function(Data, ChildParams)`, or a fun of the same two arguments:

```erlang
%% in comment.erl
changeset(Data, Params) ->
    CS = kura_changeset:cast(comment, Data, Params, [body]),
    kura_changeset:validate_required(CS, [body]).

CS1 = kura_changeset:cast_assoc(CS, comments, #{with => {comment, changeset}}).
```

`with` wins when both are given. A `cast` list may not name the child's
single-column primary key or its foreign key: the first identifies the
row and the second is set from the parent. Naming one, or a field the
child does not have, raises
`{kura, {invalid_nested_cast, Schema, Name, {cast, Fields}}}`.

For a `many_to_many` association the child is a row other parents may
share. `#{cast => []}` links and unlinks without letting the params edit
the linked row.

### Without an Allow-List

With neither option, the params may set every non-virtual field of the
child except those two keys. A request can then set fields the form never
showed, such as `approved`, `inserted_at` or `deleted_at`.

Make the allow-list mandatory:

```erlang
{kura, [{strict_assoc_cast, true}]}
```

`cast_assoc`, `cast_embed` and `put_assoc` of a raw map then raise
`{kura, {nested_cast_needs_allow_list, Schema, Name}}` unless they are
given `cast` or `with`. Left unset, the permissive default stays and kura
logs one warning per boot the first time a nested cast has no allow-list.
`false` keeps the permissive default without the warning. The default
becomes `true` in kura 3.0.

### Update Detection (has_many)

For `has_many`, Kura checks whether each child param map contains the primary key:

- **No PK (or PK is `undefined`)**: builds a new changeset with `action = insert`
- **PK present**: looks up the matching record from existing data, builds a changeset against it with `action = update`

```erlang
%% Existing post with comments already loaded
ExistingPost = #{id => 1, title => ~"Old", comments => [
    #{id => 10, body => ~"Existing comment"}
]},
Params = #{
    title => ~"Updated",
    comments => [
        #{id => 10, body => ~"Edited comment"},  %% update (has PK)
        #{body => ~"New comment"}                  %% insert (no PK)
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
    #{body => ~"Great!"},
    #{body => ~"Thanks!"}
]).
```

Maps are cast into changesets. `put_assoc/4` takes the same `cast` and `with` options as `cast_assoc/3` for them, and `strict_assoc_cast` requires one:

```erlang
CS1 = kura_changeset:put_assoc(CS, comments, [#{body => ~"Great!"}], #{cast => [body]}).
```

You can also pass pre-built changesets directly, which are used as they are:

```erlang
ChildCS = kura_changeset:cast(comment, #{}, #{body => ~"Nice"}, [body]),
CS1 = kura_changeset:put_assoc(CS, comments, [ChildCS]).
```

## Transaction Behavior

When a parent changeset has `assoc_changes`:

1. A transaction is opened
2. The parent is inserted or updated first
3. Each child changeset is persisted with the foreign key automatically set to the parent's primary key value
4. The parent row is returned with association data merged in

If the parent or any child fails to write, the transaction is rolled back
and nothing is kept. A failed parent returns `{error, ParentCS}` as it
would without associations. A failed child returns `{error, ParentCS}`
with `valid = false`, where the failed child changeset, carrying its
errors, takes the place of the one you passed in `assoc_changes`:

```erlang
{error, ErrCS} = kura_repo_worker:insert(my_repo, CS),
[_, FailedComment] = maps:get(comments, ErrCS#kura_changeset.assoc_changes),
FailedComment#kura_changeset.errors.
```

A child that fails for a reason other than its changeset, such as a
stale optimistic lock or an `after_*` hook returning `{error, Reason}`,
adds a `base` error to the parent changeset. A failed `many_to_many`
join row is reported on the parent changeset the way a constraint error
on the parent is when it violates a constraint, and as `could not be
saved` on the association otherwise.

A `many_to_many` child given by key alone links an existing row. That
row is looked up first, through the same tenant-scoped read as any
query, and a row that is not found adds `does not exist` on the
association instead of a join row.

Inside an enclosing `kura_repo_worker:transaction/2` the write runs as a
savepoint, so a failure undoes only this write and, on PostgreSQL, the
enclosing transaction carries on. SQLite ends the whole transaction on
some errors (`ON CONFLICT ROLLBACK`, a full disk, an I/O error); after
one of those the enclosing transaction is gone as well.

If `assoc_changes` is empty, the insert/update runs without transaction overhead.

## cast_assoc vs put_assoc

| | cast_assoc | put_assoc |
|---|---|---|
| Source | Params from changeset | Maps or changesets you provide |
| Casting | `cast` list or `with` function | Maps cast the same way, changesets used as-is |
| Use case | Form/API input processing | Programmatic association building |
