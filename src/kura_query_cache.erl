-module(kura_query_cache).
-moduledoc """
ETS-based cache for compiled query results.

Caches `{SQL, Params}` tuples keyed by `{RepoMod, QueryHash}` so two
repos with different dialects don't share entries. Identical queries
through the same repo skip recompilation.

The ETS table is owned by `kura_query_cache_owner` (a gen_server under
`kura_sup`), so the table survives any caller exiting.
""".
-behaviour(gen_server).

-export([init/0, get/1, put/2]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).

-define(TABLE, kura_query_cache).

-doc """
Initialize the query cache ETS table. No-op when the cache owner is
already running (the normal app-startup path). Used by tests that
exercise the cache without starting the kura app.
""".
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
    case ets:whereis(?TABLE) of
        undefined ->
            miss;
        _ ->
            case ets:lookup(?TABLE, Key) of
                [{_, Result}] -> {ok, Result};
                [] -> miss
            end
    end.

-doc "Store a compiled query result for a key.".
-spec put(term(), {iodata(), [term()]}) -> ok.
put(Key, Result) ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            ets:insert(?TABLE, {Key, Result}),
            ok
    end.

%%======================================================================
%% gen_server: owns the ETS table for the lifetime of kura_sup.
%%======================================================================

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init([]) -> {ok, []}.
init([]) ->
    init(),
    {ok, []}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
