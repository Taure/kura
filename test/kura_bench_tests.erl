-module(kura_bench_tests).

-include_lib("eunit/include/eunit.hrl").

-define(RECORDED_REPO, kura_bench_recorded_repo).

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
            {"stream large dataset in batches", {timeout, 120, fun t_stream_large_dataset/0}},
            {"cost of loading one row", {timeout, 60, fun t_load_row_cost/0}},
            {"cost of one statement's telemetry metadata",
                {timeout, 60, fun t_telemetry_metadata_cost/0}},
            {"insert, update_all and all as a job queue runs them",
                {timeout, 120, fun t_job_queue_loop/0}}
        ]
    end}.

%% These run kura_repo_worker against a driver that answers at once, so the
%% time is kura's own: compiling, scoping, redacting, loading and telemetry.
recorded_bench_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(telemetry),
            ok = kura_query_cache:init(),
            kura_test_recording_driver:configure(?RECORDED_REPO),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush()
        end,
        fun(_) ->
            [
                {"all/2 on a cached shape", {timeout, 120, fun t_cached_all_cost/0}},
                {"all/2 of a join on a cached shape", {timeout, 120, fun t_cached_join_all_cost/0}},
                {"insert, update_all and all without a database",
                    {timeout, 120, fun t_recorded_job_queue_cost/0}}
            ]
        end}.

setup() ->
    application:ensure_all_started(minato),
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
            Posts = maps:get(posts, U),
            ?assertEqual(5, length(Posts))
        end,
        Loaded
    ).

t_stream_large_dataset() ->
    clean_tables(),
    %% Insert 5000 rows via insert_all in batches of 1000
    lists:foreach(
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
    T0 = erlang:monotonic_time(millisecond),
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
    ?debugFmt("~nStream: ~p rows in ~p batches (~p ms)", [
        TotalRows, TotalBatches, erlang:monotonic_time(millisecond) - T0
    ]),
    ?assertEqual(5000, TotalRows),
    ?assertEqual(50, TotalBatches).

t_load_row_cost() ->
    Row = #{
        id => 1,
        name => ~"bench",
        email => ~"bench@bench.com",
        counter => 0,
        lock_version => 0,
        inserted_at => {{2026, 1, 1}, {0, 0, 0}},
        updated_at => {{2026, 1, 1}, {0, 0, 0}}
    },
    N = 300000,
    _ = kura_db:load_row(kura_stress_schema, Row),
    {Us, ok} = timer:tc(fun() -> load_row_times(N, Row) end),
    ?debugFmt("~nload_row: ~p ns/row (~p rows)", [Us * 1000 div N, N]).

load_row_times(0, _Row) ->
    ok;
load_row_times(N, Row) ->
    _ = kura_db:load_row(kura_stress_schema, Row),
    load_row_times(N - 1, Row).

t_telemetry_metadata_cost() ->
    SQL = [
        ~"SELECT \"id\", \"name\", \"email\", \"counter\" FROM \"stress_users\"",
        ~" WHERE \"email\" = $1 ORDER BY \"id\" LIMIT $2"
    ],
    Result = #{command => select, rows => [#{id => 1}], num_rows => 1},
    N = 300000,
    {Us, ok} = timer:tc(fun() -> telemetry_metadata_times(N, SQL, Result) end),
    ?debugFmt("~nbuild_telemetry_metadata: ~p ns/statement (~p statements)", [
        Us * 1000 div N, N
    ]).

telemetry_metadata_times(0, _SQL, _Result) ->
    ok;
telemetry_metadata_times(N, SQL, Result) ->
    _ = kura_db:build_telemetry_metadata(kura_stress_repo, SQL, [~"bench@bench.com", 1], Result),
    telemetry_metadata_times(N - 1, SQL, Result).

t_cached_all_cost() ->
    Q = kura_query:limit(
        kura_query:where(kura_query:from(kura_stress_schema), {email, '=', ~"bench@bench.com"}), 1
    ),
    N = 200000,
    ok = recorded_all_times(1000, Q),
    {Us, ok} = timer:tc(fun() -> recorded_all_times(N, Q) end),
    ?debugFmt("~nall/2, cached shape: ~p ns/call (~p calls)", [Us * 1000 div N, N]).

t_cached_join_all_cost() ->
    Q = kura_query:where(
        kura_query:join(
            kura_query:from(kura_stress_schema),
            inner,
            kura_stress_post_schema,
            {id, user_id}
        ),
        {email, '=', ~"bench@bench.com"}
    ),
    N = 200000,
    ok = recorded_all_times(1000, Q),
    {Us, ok} = timer:tc(fun() -> recorded_all_times(N, Q) end),
    ?debugFmt("~nall/2 of a join, cached shape: ~p ns/call (~p calls)", [Us * 1000 div N, N]).

recorded_all_times(0, _Q) ->
    _ = kura_test_recording_driver:statements(),
    ok;
recorded_all_times(N, Q) ->
    {ok, []} = kura_repo_worker:all(?RECORDED_REPO, Q),
    _ = drained(N),
    recorded_all_times(N - 1, Q).

drained(N) when N rem 500 =:= 0 -> kura_test_recording_driver:statements();
drained(_N) -> [].

t_recorded_job_queue_cost() ->
    N = 50000,
    ok = recorded_job_times(500),
    {Us, ok} = timer:tc(fun() -> recorded_job_times(N) end),
    ?debugFmt("~njob queue loop, no database: ~p ns/iteration (~p iterations)", [
        Us * 1000 div N, N
    ]).

recorded_job_times(0) ->
    _ = kura_test_recording_driver:statements(),
    ok;
recorded_job_times(N) ->
    ok = job_iteration(?RECORDED_REPO, N),
    _ = drained(N),
    recorded_job_times(N - 1).

t_job_queue_loop() ->
    clean_tables(),
    N = 500,
    ok = job_times(kura_stress_repo, 50),
    {Us, ok} = timer:tc(fun() -> job_times(kura_stress_repo, N) end),
    ?debugFmt("~njob queue loop, PostgreSQL: ~p us/iteration (~p iterations)", [Us div N, N]).

job_times(_Repo, 0) ->
    ok;
job_times(Repo, N) ->
    ok = job_iteration(Repo, N),
    job_times(Repo, N - 1).

%% Enqueue a row, claim what is available, read the claimed rows back.
job_iteration(Repo, N) ->
    Name = <<"job_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    CS = kura_changeset:cast(
        kura_stress_schema,
        #{},
        #{~"name" => Name, ~"email" => <<Name/binary, "@bench.com">>},
        [name, email]
    ),
    {ok, _} = kura_repo_worker:insert(Repo, CS),
    Available = kura_query:where(kura_query:from(kura_stress_schema), {counter, '=', 0}),
    {ok, _} = kura_repo_worker:update_all(Repo, Available, #{counter => N}),
    Claimed = kura_query:limit(
        kura_query:order_by(
            kura_query:where(kura_query:from(kura_stress_schema), {counter, '=', N}),
            [{id, asc}]
        ),
        10
    ),
    {ok, _} = kura_repo_worker:all(Repo, Claimed),
    ok.
