-module(kura_bench_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

bench_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"sequential insert throughput", {timeout, 60, fun t_sequential_insert_throughput/0}},
            {"concurrent insert throughput", {timeout, 60, fun t_concurrent_insert_throughput/0}},
            {"read latency distribution (p50/p95/p99)",
                {timeout, 60, fun t_read_latency_distribution/0}},
            {"bulk insert_all at 1k/5k/10k rows", {timeout, 120, fun t_insert_all_bulk/0}},
            {"preload batching avoids N+1", {timeout, 60, fun t_preload_batching/0}},
            {"stream large dataset in batches", {timeout, 120, fun t_stream_large_dataset/0}}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_stress_repo:start(),
    {ok, _} = kura_stress_repo:query(
        "CREATE TABLE IF NOT EXISTS stress_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  counter INTEGER DEFAULT 0,"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_stress_repo:query(
        "CREATE TABLE IF NOT EXISTS stress_posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  user_id BIGINT NOT NULL REFERENCES stress_users(id) ON DELETE CASCADE,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_stress_repo:query("DROP TABLE IF EXISTS stress_posts CASCADE", []),
    kura_stress_repo:query("DROP TABLE IF EXISTS stress_users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

clean_tables() ->
    kura_stress_repo:query("TRUNCATE stress_posts, stress_users RESTART IDENTITY CASCADE", []).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_sequential_insert_throughput() ->
    clean_tables(),
    N = 500,
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(
        fun(I) ->
            % eqwalizer:fixme - I is integer from lists:seq
            Name = iolist_to_binary([<<"seq_">>, integer_to_binary(I)]),
            Email = iolist_to_binary([Name, <<"@bench.com">>]),
            CS = kura_changeset:cast(
                kura_stress_schema, #{}, #{<<"name">> => Name, <<"email">> => Email}, [name, email]
            ),
            CS1 = kura_changeset:validate_required(CS, [name, email]),
            {ok, _} = kura_stress_repo:insert(CS1)
        end,
        lists:seq(1, N)
    ),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    ElapsedMs = round(Elapsed / 1000),
    Rate = round(N / (Elapsed / 1_000_000)),
    ?debugFmt("~nSequential: ~p inserts/sec (~p ms for ~p rows)", [Rate, ElapsedMs, N]),
    ?assert(Rate > 100).

t_concurrent_insert_throughput() ->
    clean_tables(),
    N = 500,
    Workers = 20,
    PerWorker = N div Workers,
    Self = self(),
    Start = erlang:monotonic_time(microsecond),
    _Pids = [
        spawn_link(fun() ->
            lists:foreach(
                fun(J) ->
                    % eqwalizer:fixme - J is integer from lists:seq
                    Idx = (W - 1) * PerWorker + J,
                    Name = iolist_to_binary([<<"con_">>, integer_to_binary(Idx)]),
                    Email = iolist_to_binary([Name, <<"@bench.com">>]),
                    CS = kura_changeset:cast(
                        kura_stress_schema,
                        #{},
                        #{<<"name">> => Name, <<"email">> => Email},
                        [name, email]
                    ),
                    CS1 = kura_changeset:validate_required(CS, [name, email]),
                    {ok, _} = kura_stress_repo:insert(CS1)
                end,
                lists:seq(1, PerWorker)
            ),
            Self ! {worker_done, W}
        end)
     || W <- lists:seq(1, Workers)
    ],
    lists:foreach(
        fun(W) ->
            receive
                {worker_done, W} -> ok
            after 30000 -> error({worker_timeout, W})
            end
        end,
        lists:seq(1, Workers)
    ),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    ElapsedMs = round(Elapsed / 1000),
    Rate = round(N / (Elapsed / 1_000_000)),
    ?debugFmt("~nConcurrent: ~p inserts/sec (~p ms for ~p rows, ~p workers)", [
        Rate, ElapsedMs, N, Workers
    ]),
    ?assert(Rate > 100).

t_read_latency_distribution() ->
    clean_tables(),
    %% Seed 100 rows
    Entries = [
        #{
            name => iolist_to_binary([<<"lat_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"lat_">>, integer_to_binary(I), <<"@bench.com">>])
        }
     || I <- lists:seq(1, 100)
    ],
    {ok, 100} = kura_stress_repo:insert_all(kura_stress_schema, Entries),
    %% Run 500 LIMIT 1 queries
    N = 500,
    Latencies = lists:map(
        fun(_) ->
            T1 = erlang:monotonic_time(microsecond),
            Q = kura_query:limit(kura_query:from(kura_stress_schema), 1),
            {ok, [_]} = kura_stress_repo:all(Q),
            erlang:monotonic_time(microsecond) - T1
        end,
        lists:seq(1, N)
    ),
    Sorted = lists:sort(Latencies),
    P50 = lists:nth(round(N * 0.50), Sorted),
    P95 = lists:nth(round(N * 0.95), Sorted),
    P99 = lists:nth(round(N * 0.99), Sorted),
    ?debugFmt("~nRead latency (us): p50=~p p95=~p p99=~p", [P50, P95, P99]),
    %% p99 should be under 50ms
    ?assert(P99 < 50_000).

t_insert_all_bulk() ->
    clean_tables(),
    lists:foreach(
        % eqwalizer:fixme - Size is integer from list
        fun(Size) ->
            clean_tables(),
            Entries = [
                #{
                    name => iolist_to_binary([<<"bulk_">>, integer_to_binary(I)]),
                    email => iolist_to_binary([<<"bulk_">>, integer_to_binary(I), <<"@bench.com">>])
                }
             || I <- lists:seq(1, Size)
            ],
            T1 = erlang:monotonic_time(microsecond),
            {ok, Count} = kura_stress_repo:insert_all(kura_stress_schema, Entries),
            Elapsed = erlang:monotonic_time(microsecond) - T1,
            Rate = round(Count / (Elapsed / 1_000_000)),
            ?debugFmt("~ninsert_all ~pk: ~p rows/sec (~p ms)", [
                Size div 1000, Rate, round(Elapsed / 1000)
            ]),
            ?assertEqual(Size, Count)
        end,
        [1000, 5000, 10000]
    ).

t_preload_batching() ->
    clean_tables(),
    %% Insert 50 users
    UserEntries = [
        #{
            name => iolist_to_binary([<<"pre_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"pre_">>, integer_to_binary(I), <<"@bench.com">>])
        }
     || I <- lists:seq(1, 50)
    ],
    {ok, 50} = kura_stress_repo:insert_all(kura_stress_schema, UserEntries),
    %% Get all user IDs
    Q = kura_query:from(kura_stress_schema),
    {ok, Users} = kura_stress_repo:all(Q),
    %% Insert 5 posts per user
    PostEntries = lists:flatmap(
        fun(User) ->
            % eqwalizer:fixme - User is map from repo:all
            Uid = maps:get(id, User),
            [
                #{
                    title => iolist_to_binary([<<"post_">>, integer_to_binary(J)]),
                    user_id => Uid
                }
             || J <- lists:seq(1, 5)
            ]
        end,
        Users
    ),
    {ok, 250} = kura_stress_repo:insert_all(kura_stress_post_schema, PostEntries),
    %% Fetch all users and preload posts
    {ok, AllUsers} = kura_stress_repo:all(Q),
    Loaded = kura_stress_repo:preload(kura_stress_schema, AllUsers, [posts]),
    ?assertEqual(50, length(Loaded)),
    %% Each user should have 5 posts
    lists:foreach(
        fun(U) ->
            % eqwalizer:fixme - U is map from preload
            Posts = maps:get(posts, U),
            ?assertEqual(5, length(Posts))
        end,
        Loaded
    ).

t_stream_large_dataset() ->
    clean_tables(),
    %% Insert 5000 rows via insert_all in batches of 1000
    lists:foreach(
        % eqwalizer:fixme - Batch is integer from lists:seq
        fun(Batch) ->
            Offset = (Batch - 1) * 1000,
            Entries = [
                #{
                    name => iolist_to_binary([<<"str_">>, integer_to_binary(Offset + I)]),
                    email => iolist_to_binary([
                        <<"str_">>, integer_to_binary(Offset + I), <<"@bench.com">>
                    ])
                }
             || I <- lists:seq(1, 1000)
            ],
            {ok, 1000} = kura_stress_repo:insert_all(kura_stress_schema, Entries)
        end,
        lists:seq(1, 5)
    ),
    %% Stream with batch_size=100
    RowCount = atomics:new(1, [{signed, false}]),
    BatchCount = atomics:new(1, [{signed, false}]),
    Q = kura_query:from(kura_stress_schema),
    ok = kura_stream:stream(
        kura_stress_repo,
        Q,
        fun(Batch) ->
            atomics:add(RowCount, 1, length(Batch)),
            atomics:add(BatchCount, 1, 1),
            ok
        end,
        #{batch_size => 100}
    ),
    TotalRows = atomics:get(RowCount, 1),
    TotalBatches = atomics:get(BatchCount, 1),
    ?debugFmt("~nStream: ~p rows in ~p batches", [TotalRows, TotalBatches]),
    ?assertEqual(5000, TotalRows),
    ?assertEqual(50, TotalBatches).
