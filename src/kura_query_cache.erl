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

Bound parameters are part of both key and value, so a parameterised query
interns a fresh entry per distinct value set and the table only ever
grows. Two limits bound it, both counted in words and both configurable:

- `query_cache_max_memory` (default 8000000 words, ~64 MB on 64-bit) -
  once the table exceeds it the whole cache is dropped and refills.
- `query_cache_max_entry_size` (default 4096 words) - a single result
  bigger than this is never interned.

The per-entry ceiling is the load-bearing one. A `where {id, in, List}`
built from a caller-supplied array stores that list in both key and
value, so capping entry *count* alone would still let a few thousand
oversized entries exhaust the node.

See kura#163 for the shape-only cache that removes the need for either.

The ETS table is owned by `kura_query_cache_owner` (a gen_server under
`kura_sup`), so the table survives any caller exiting.
""".
-behaviour(gen_server).

-export([init/0, get/1, put/2, flush/0]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).

-define(TABLE, kura_query_cache).
-define(DEFAULT_MAX_MEMORY_WORDS, 8000000).
-define(DEFAULT_MAX_ENTRY_WORDS, 4096).

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
Store a compiled query result for a key. Skips entries larger than
`query_cache_max_entry_size`, and drops the whole cache first when it has
grown past `query_cache_max_memory`, so a workload of parameterised
queries cannot grow the table without bound.

Dropping is not atomic with the insert, so concurrent putters can
transiently overshoot the limit or wipe each other's entry. Both cost a
recompile on the next lookup; neither can serve a wrong entry.
""".
-spec put(term(), {iodata(), [term()]}) -> ok.
put(Key, Result) ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            maybe_insert(Key, Result)
    end.

maybe_insert(Key, Result) ->
    Words = erts_debug:flat_size(Key) + erts_debug:flat_size(Result),
    case Words > max_entry_words() of
        true ->
            ok;
        false ->
            _ =
                case ets:info(?TABLE, memory) >= max_memory_words() of
                    true -> ets:delete_all_objects(?TABLE);
                    false -> ok
                end,
            ets:insert(?TABLE, {Key, Result}),
            ok
    end.

-doc """
Drop every cached entry. Required after swapping a repo's dialect at
runtime, since the dialect is not part of the cache key. Also used by
tests.
""".
-spec flush() -> ok.
flush() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            ets:delete_all_objects(?TABLE),
            ok
    end.

-spec max_memory_words() -> pos_integer().
max_memory_words() ->
    env_pos_integer(query_cache_max_memory, ?DEFAULT_MAX_MEMORY_WORDS).

-spec max_entry_words() -> pos_integer().
max_entry_words() ->
    env_pos_integer(query_cache_max_entry_size, ?DEFAULT_MAX_ENTRY_WORDS).

-spec env_pos_integer(atom(), pos_integer()) -> pos_integer().
env_pos_integer(Key, Default) ->
    case application:get_env(kura, Key) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> Default
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
