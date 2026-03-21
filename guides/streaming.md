# Streaming

`kura_stream` provides server-side cursor streaming for processing large result sets in batches without loading everything into memory at once.

## How It Works

Under the hood, streaming runs inside a PostgreSQL transaction and uses server-side cursors:

1. `DECLARE CURSOR` opens a named cursor for the query
2. `FETCH N` retrieves the next batch of rows
3. Your callback processes each batch
4. Steps 2-3 repeat until no more rows are returned
5. `CLOSE` cleans up the cursor (guaranteed via `after` block)

This means rows are fetched on demand from PostgreSQL rather than buffered in the BEAM VM.

## Basic Usage

Pass a query and a callback function. The callback receives a list of loaded schema maps for each batch:

```erlang
Q = kura_query:from(my_user),
ok = kura_stream:stream(my_repo, Q, fun(Batch) ->
    lists:foreach(fun(User) ->
        io:format(~"Processing: ~p~n", [maps:get(name, User)])
    end, Batch),
    ok
end).
```

The default batch size is 500 rows.

## Custom Batch Size

Use the `batch_size` option to control how many rows are fetched per `FETCH`:

```erlang
Q = kura_query:from(my_event),
ok = kura_stream:stream(my_repo, Q, fun(Batch) ->
    process_events(Batch),
    ok
end, #{batch_size => 100}).
```

Smaller batch sizes use less memory per iteration but require more round trips to PostgreSQL. Larger batch sizes reduce round trips but increase memory usage per batch.

## Use Cases

### Exporting Data

```erlang
export_users_to_csv(Repo) ->
    Q = kura_query:where(kura_query:from(my_user), {active, true}),
    {ok, File} = file:open(~"/tmp/users.csv", [write]),
    ok = kura_stream:stream(Repo, Q, fun(Batch) ->
        lists:foreach(fun(User) ->
            Line = io_lib:format(~"~s,~s~n", [
                maps:get(name, User),
                maps:get(email, User)
            ]),
            file:write(File, Line)
        end, Batch),
        ok
    end, #{batch_size => 1000}),
    file:close(File).
```

### Batch Updates

```erlang
Q = kura_query:where(kura_query:from(my_user), {verified, false}),
ok = kura_stream:stream(my_repo, Q, fun(Batch) ->
    lists:foreach(fun(User) ->
        CS = kura_changeset:cast(my_user, User, #{}, []),
        CS1 = kura_changeset:put_change(CS, verified, true),
        {ok, _} = kura_repo_worker:update(my_repo, CS1)
    end, Batch),
    ok
end, #{batch_size => 200}).
```

## Error Handling

If the callback or any database operation fails, the transaction is rolled back and `stream/3,4` returns `{error, Reason}`:

```erlang
case kura_stream:stream(my_repo, Q, fun(Batch) ->
    process(Batch),
    ok
end) of
    ok -> done;
    {error, Reason} -> logger:error(~"Stream failed: ~p", [Reason])
end.
```

## Important Notes

- The entire stream runs inside a single PostgreSQL transaction. Long-running streams hold a connection and a transaction open for the duration.
- Schema types are properly loaded for each row, just like `kura_repo_worker:all/2`.
- Queries can include filters, ordering, and joins -- any valid `kura_query` works.
