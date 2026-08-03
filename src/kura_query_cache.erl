-module(kura_query_cache).
-moduledoc """
ETS-based cache for compiled query results.

Caches `{SQL, Params}` tuples keyed by `{RepoMod, Query}` so two repos
with different dialects don't share entries. Identical queries through
the same repo skip recompilation.

The key carries the whole query term rather than a hash of it: an entry
holds bound parameters as well as SQL, so serving the wrong entry would
hand a caller another query's rows rather than merely a slower path. ETS
hashes the term itself, so hashing the key by hand only reintroduces a
collision class the table had already eliminated.

Bound parameters are part of both key and value, so a parameterised
query interns a fresh entry per distinct value set and the table only
ever grows. `query_cache_max_size` caps that: once the table exceeds it
the whole cache is dropped and refills. See kura#163 for the shape-only
cache that removes the need for a cap.

The ETS table is owned by `kura_query_cache_owner` (a gen_server under
`kura_sup`), so the table survives any caller exiting.
""".
-behaviour(gen_server).

-export([init/0, get/1, put/2, flush/0]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).

-define(TABLE, kura_query_cache).
-define(DEFAULT_MAX_SIZE, 10000).

-doc """
Initialize the query cache ETS table. No-op when the cache owner is
already running (the normal app-startup path). Used by tests that
exercise the cache without starting the kura app.
""".
-spec init() -> ok.
init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            %% `set' compares keys with `=:='. `ordered_set' would compare
            %% with `==', collapsing `where id = 1' and `where id = 1.0'
            %% onto one entry whose bound parameters differ. Do not change.
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

-doc """
Store a compiled query result for a key. Drops the whole cache first
when it has grown past `query_cache_max_size`, so a workload of
parameterised queries cannot grow the table without bound.
""".
-spec put(term(), {iodata(), [term()]}) -> ok.
put(Key, Result) ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            _ =
                case ets:info(?TABLE, size) >= max_size() of
                    true -> ets:delete_all_objects(?TABLE);
                    false -> ok
                end,
            ets:insert(?TABLE, {Key, Result}),
            ok
    end.

-doc "Drop every cached entry. Used on config reload and by tests.".
-spec flush() -> ok.
flush() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            ets:delete_all_objects(?TABLE),
            ok
    end.

-spec max_size() -> pos_integer().
max_size() ->
    case application:get_env(kura, query_cache_max_size) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_MAX_SIZE
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
