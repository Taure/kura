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
    Pool = get_pool(RepoMod),
    CursorName = generate_cursor_name(),
    DeclareSQL = iolist_to_binary([
        <<"DECLARE ">>,
        CursorName,
        <<" CURSOR FOR ">>,
        SQL
    ]),
    FetchSQL = iolist_to_binary([
        <<"FETCH ">>,
        integer_to_binary(BatchSize),
        <<" FROM ">>,
        CursorName
    ]),
    CloseSQL = iolist_to_binary([<<"CLOSE ">>, CursorName]),
    pgo:transaction(
        Pool,
        fun() ->
            _ = pgo:query(DeclareSQL, Params),
            try
                fetch_loop(FetchSQL, Schema, Fun)
            after
                pgo:query(CloseSQL, [])
            end
        end,
        #{}
    ).

fetch_loop(FetchSQL, Schema, Fun) ->
    case pgo:query(FetchSQL, []) of
        #{rows := []} ->
            ok;
        #{rows := Rows} ->
            Loaded = [load_row(Schema, Row) || Row <- Rows],
            Fun(Loaded),
            fetch_loop(FetchSQL, Schema, Fun);
        {error, _} = Err ->
            Err
    end.

load_row(SchemaMod, Row) when is_atom(SchemaMod) ->
    case code:ensure_loaded(SchemaMod) of
        {module, SchemaMod} ->
            case erlang:function_exported(SchemaMod, fields, 0) of
                true ->
                    Types = kura_schema:field_types(SchemaMod),
                    maps:fold(
                        fun(K, V, Acc) ->
                            case maps:find(K, Types) of
                                {ok, Type} ->
                                    case kura_types:load(Type, V) of
                                        {ok, Loaded} -> Acc#{K => Loaded};
                                        {error, _} -> Acc#{K => V}
                                    end;
                                error ->
                                    Acc#{K => V}
                            end
                        end,
                        #{},
                        Row
                    );
                false ->
                    Row
            end;
        _ ->
            Row
    end.

get_pool(RepoMod) ->
    Config = RepoMod:config(),
    maps:get(pool, Config, RepoMod).

generate_cursor_name() ->
    Ref = erlang:unique_integer([positive]),
    iolist_to_binary([<<"kura_cursor_">>, integer_to_binary(Ref)]).
