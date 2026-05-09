-module(kura_stream).
-moduledoc """
Server-side cursor streaming for processing large result sets in batches.

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

-define(POOL_IMPL, kura_pool_hnc).

-doc "Stream query results in batches of 500, calling Fun for each batch.".
-spec stream(module(), #kura_query{}, fun(([map()]) -> ok)) -> ok | {error, term()}.
stream(RepoMod, Query, Fun) ->
    stream(RepoMod, Query, Fun, #{}).

-doc "Stream query results with options. Opts: `#{batch_size => pos_integer()}`.".
-spec stream(module(), #kura_query{}, fun(([map()]) -> ok), map()) -> ok | {error, term()}.
stream(RepoMod, Query, Fun, Opts) ->
    BatchSize = maps:get(batch_size, Opts, 500),
    {SQL, Params} = kura_query_compiler:to_sql(Query),
    Schema = Query#kura_query.from,
    Pool = kura_db:get_pool(RepoMod),
    CursorName = generate_cursor_name(),
    DeclareSQL = iolist_to_binary([~"DECLARE ", CursorName, ~" CURSOR FOR ", SQL]),
    FetchSQL = iolist_to_binary([
        ~"FETCH ", integer_to_binary(BatchSize), ~" FROM ", CursorName
    ]),
    CloseSQL = iolist_to_binary([~"CLOSE ", CursorName]),
    Result = kura_pool:with_conn(?POOL_IMPL, Pool, fun(Worker) ->
        case kura_pg_conn:get_conn(Worker) of
            {ok, Conn} ->
                run_stream(Conn, DeclareSQL, Params, FetchSQL, CloseSQL, Schema, Fun);
            {error, _} = Err ->
                Err
        end
    end),
    narrow_transaction_result(Result).

-spec narrow_transaction_result(term()) -> ok | {error, term()}.
narrow_transaction_result(ok) -> ok;
narrow_transaction_result({error, _} = Err) -> Err.

run_stream(Conn, DeclareSQL, Params, FetchSQL, CloseSQL, Schema, Fun) ->
    {ok, [], []} = epgsql:squery(Conn, ~"BEGIN"),
    try
        case kura_db:translate_result(epgsql:equery(Conn, DeclareSQL, Params), DeclareSQL) of
            #{} ->
                Result = fetch_loop(Conn, FetchSQL, Schema, Fun),
                _ = epgsql:squery(Conn, CloseSQL),
                {ok, [], []} = epgsql:squery(Conn, ~"COMMIT"),
                Result;
            {error, _} = DeclareErr ->
                _ = epgsql:squery(Conn, ~"ROLLBACK"),
                DeclareErr
        end
    catch
        Class:Reason:Stack ->
            _ = epgsql:squery(Conn, ~"ROLLBACK"),
            erlang:raise(Class, Reason, Stack)
    end.

fetch_loop(Conn, FetchSQL, Schema, Fun) ->
    case kura_db:translate_result(epgsql:equery(Conn, FetchSQL, []), FetchSQL) of
        #{rows := []} ->
            ok;
        #{rows := Rows} ->
            Loaded = [kura_db:load_row(Schema, Row) || Row <- Rows],
            Fun(Loaded),
            fetch_loop(Conn, FetchSQL, Schema, Fun);
        {error, _} = Err ->
            Err
    end.

generate_cursor_name() ->
    Ref = erlang:unique_integer([positive]),
    iolist_to_binary([~"kura_cursor_", integer_to_binary(Ref)]).
