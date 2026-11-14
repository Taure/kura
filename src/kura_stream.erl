-module(kura_stream).
-moduledoc """
Server-side cursor streaming for processing large result sets in batches.
It needs a backend with cursors (`DECLARE`, `FETCH`), which PostgreSQL has
and SQLite does not.

```erlang
Q = kura_query:where(kura_query:from(my_user), {active, true}),
kura_stream:stream(my_repo, Q, fun(Batch) ->
    [process(Row) || Row <- Batch],
    ok
end, #{batch_size => 100}).
```
""".

-include("kura.hrl").

-export([stream/3, stream/4]).

-ifdef(TEST).
-export([narrow_transaction_result/1]).
-endif.

-doc "Stream query results in batches of 500, calling Fun for each batch.".
-spec stream(module(), #kura_query{}, fun(([map()]) -> ok)) -> ok | {error, term()}.
stream(RepoMod, Query, Fun) ->
    stream(RepoMod, Query, Fun, #{}).

-doc """
Stream query results with options. Opts: `#{batch_size => pos_integer()}`.

The query is scoped as `kura_repo_worker:all/2` scopes it: soft-deleted rows
are left out unless the query asks for them with `kura_query:with_deleted/1`,
and a prefix tenant applies.

Raises `{kura, {unsupported_capability, cursors, PoolModule}}` on a backend
that declares no `cursors` (see `m:kura_capabilities`), before any
statement runs.
""".
-spec stream(module(), #kura_query{}, fun(([map()]) -> ok), map()) -> ok | {error, term()}.
stream(RepoMod, Query, Fun, Opts) ->
    ok = check_cursors(RepoMod),
    BatchSize = maps:get(batch_size, Opts, 500),
    {SQL, Params} = kura_query_compiler:to_sql(RepoMod, kura_repo_worker:scope_read(Query)),
    Schema = Query#kura_query.from,
    CursorName = generate_cursor_name(),
    DeclareSQL = iolist_to_binary([
        ~"DECLARE ",
        CursorName,
        ~" CURSOR FOR ",
        SQL
    ]),
    FetchSQL = iolist_to_binary([
        ~"FETCH ",
        integer_to_binary(BatchSize),
        ~" FROM ",
        CursorName
    ]),
    CloseSQL = iolist_to_binary([~"CLOSE ", CursorName]),
    Result = kura_db:transaction(RepoMod, fun() ->
        _ = kura_db:query(
            RepoMod, DeclareSQL, Params, kura_redact:query_params(Query, Params)
        ),
        try
            fetch_loop(RepoMod, FetchSQL, Schema, Fun)
        after
            kura_db:query(RepoMod, CloseSQL, [])
        end
    end),
    narrow_transaction_result(Result).

-spec check_cursors(module()) -> ok.
check_cursors(RepoMod) ->
    case kura_repo:settings(RepoMod) of
        #{pool_module := PoolMod} when is_atom(PoolMod) ->
            kura_capabilities:check(PoolMod, cursors);
        _ ->
            ok
    end.

-spec narrow_transaction_result(term()) -> ok | {error, term()}.
narrow_transaction_result(ok) -> ok;
narrow_transaction_result({error, _} = Err) -> Err.

fetch_loop(RepoMod, FetchSQL, Schema, Fun) ->
    case kura_db:query(RepoMod, FetchSQL, []) of
        #{rows := []} ->
            ok;
        #{rows := Rows} ->
            Loaded = kura_db:load_rows(RepoMod, Schema, Rows),
            Fun(Loaded),
            fetch_loop(RepoMod, FetchSQL, Schema, Fun);
        {error, _} = Err ->
            Err
    end.

generate_cursor_name() ->
    Ref = erlang:unique_integer([positive]),
    iolist_to_binary([~"kura_cursor_", integer_to_binary(Ref)]).
