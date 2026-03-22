-module(kura_query_cache).
-moduledoc """
ETS-based cache for compiled query results.

Caches `{SQL, Params}` tuples keyed by query record hash.
Identical queries (same structure and values) skip recompilation.
""".

-export([init/0, get/1, put/2]).

-define(TABLE, kura_query_cache).

-doc "Initialize the query cache ETS table. Safe to call multiple times.".
-spec init() -> ok.
init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            _ = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
            ok;
        _ ->
            ok
    end.

-doc "Look up a cached compiled query by key.".
-spec get(term()) -> {ok, {iodata(), [term()]}} | miss.
get(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{_, Result}] -> {ok, Result};
        [] -> miss
    end.

-doc "Store a compiled query result for a key.".
-spec put(term(), {iodata(), [term()]}) -> ok.
put(Key, Result) ->
    ets:insert(?TABLE, {Key, Result}),
    ok.
