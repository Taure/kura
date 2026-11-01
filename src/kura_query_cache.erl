-module(kura_query_cache).
-moduledoc """
ETS-based cache for compiled query results.

Caches compiled query shapes for `kura_query_compiler:to_sql_cached/2`,
keyed by `{RepoMod, Tenant, Shape}` so two repos with different dialects
don't share entries. A shape is a query with its bound values replaced by
markers (see `kura_query_shape`), so queries that differ only in their
values share one entry.

The key carries the whole shape term rather than a hash of it: an entry
holds SQL and a parameter template, so serving the wrong entry would hand
a caller another query's rows rather than merely a slower path. ETS
hashes the term itself, so hashing the key by hand only reintroduces a
collision class the table had already eliminated.

The table is bounded by the number of distinct shapes, plus one entry per
value set for any condition `kura_query_shape` leaves in the shape. Two
limits bound it regardless, both counted in words and both configurable:

- `query_cache_max_memory` (default 8000000 words, ~64 MB on 64-bit) -
  once the table exceeds it the whole cache is dropped and refills.
- `query_cache_max_entry_size` (default 4096 words) - a single entry
  bigger than this is never interned.

Both limits count the bytes of every off-heap binary an entry holds,
such as its SQL text, which neither `erts_debug:flat_size/1` nor
`ets:info(Tab, memory)` include.

An `in` list is one shape per list length, so a caller-supplied array
of arbitrary length still interns an entry per length. The per-entry
ceiling keeps those entries small.

The ETS table is owned by `kura_query_cache_owner` (a gen_server under
`kura_sup`), so the table survives any caller exiting.
""".
-behaviour(gen_server).

-export([init/0, get/1, put/2, flush/0]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).
-export_type([entry/0]).

-doc "A compiled shape's SQL and parameter template, or a shape that cannot be cached.".
-type entry() :: {binary(), [term()]} | uncacheable.

-define(TABLE, kura_query_cache).
-define(DEFAULT_MAX_MEMORY_WORDS, 8000000).
-define(DEFAULT_MAX_ENTRY_WORDS, 4096).
-define(BINARY_TABLE, kura_query_cache_binaries).
-define(BINARY_WORDS, binary_words).
-define(HEAP_BINARY_LIMIT, 64).

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
            _ = ets:new(?BINARY_TABLE, [named_table, public, set]),
            ok;
        _ ->
            ok
    end.

-doc false.
-spec get(term()) -> {ok, entry()} | miss.
get(Key) ->
    case ets:whereis(?TABLE) of
        undefined ->
            miss;
        _ ->
            case ets:lookup(?TABLE, Key) of
                [{_, uncacheable}] ->
                    {ok, uncacheable};
                [{_, {SQL, Template}}] when is_binary(SQL), is_list(Template) ->
                    {ok, {SQL, Template}};
                _ ->
                    miss
            end
    end.

-doc false.
-spec put(term(), entry()) -> ok.
put(Key, Result) ->
    case ets:whereis(?TABLE) of
        undefined ->
            ok;
        _ ->
            maybe_insert(Key, Result)
    end.

maybe_insert(Key, Result) ->
    BinaryWords = binary_words(Key) + binary_words(Result),
    Words = erts_debug:flat_size(Key) + erts_debug:flat_size(Result) + BinaryWords,
    case Words > max_entry_words() of
        true ->
            ok;
        false ->
            _ =
                case table_words() >= max_memory_words() of
                    true -> drop_all();
                    false -> ok
                end,
            ets:insert(?TABLE, {Key, Result}),
            _ = ets:update_counter(?BINARY_TABLE, ?BINARY_WORDS, BinaryWords, {?BINARY_WORDS, 0}),
            ok
    end.

drop_all() ->
    ets:delete_all_objects(?TABLE),
    ets:delete_all_objects(?BINARY_TABLE),
    ok.

-spec table_words() -> non_neg_integer().
table_words() ->
    Binaries =
        case ets:lookup(?BINARY_TABLE, ?BINARY_WORDS) of
            [{_, N}] when is_integer(N) -> N;
            _ -> 0
        end,
    case ets:info(?TABLE, memory) of
        Words when is_integer(Words) -> Words + Binaries;
        _ -> Binaries
    end.

%% A binary over the heap limit lives outside the term, so its bytes are
%% missing from flat_size and from the table's memory. A sub-binary keeps
%% its whole parent alive, so the parent's size is what counts.
-spec binary_words(term()) -> non_neg_integer().
binary_words(B) when is_binary(B) ->
    case binary:referenced_byte_size(B) of
        Size when Size > ?HEAP_BINARY_LIMIT -> Size div erlang:system_info(wordsize) + 1;
        _ -> 0
    end;
binary_words(T) when is_tuple(T) ->
    binary_words(tuple_to_list(T));
binary_words([H | T]) ->
    binary_words(H) + binary_words(T);
binary_words(M) when is_map(M) ->
    binary_words(maps:to_list(M));
binary_words(_) ->
    0.

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
            drop_all()
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
