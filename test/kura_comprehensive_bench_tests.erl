-module(kura_comprehensive_bench_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, drop_tables/0}).
-eqwalizer({nowarn_function, seed_posts/2}).
-eqwalizer({nowarn_function, seed_comments/2}).
-eqwalizer({nowarn_function, seed_post_tags/2}).
-eqwalizer({nowarn_function, bench/3}).
-eqwalizer({nowarn_function, bench_concurrent/4}).
-eqwalizer({nowarn_function, t_insert_all_scaling/0}).
-eqwalizer({nowarn_function, t_where_in_clause/0}).
-eqwalizer({nowarn_function, t_pagination/0}).
-eqwalizer({nowarn_function, t_keyset_vs_offset/0}).
-eqwalizer({nowarn_function, t_changeset_overhead/0}).
-eqwalizer({nowarn_function, t_stream_batch_sizes/0}).

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

comprehensive_bench_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% --- CRUD Basics ---
            {"single insert latency (p50/p95/p99)", {timeout, 60, fun t_single_insert_latency/0}},
            {"single update latency (p50/p95/p99)", {timeout, 60, fun t_single_update_latency/0}},
            {"single delete latency (p50/p95/p99)", {timeout, 60, fun t_single_delete_latency/0}},
            {"get by primary key latency (p50/p95/p99)", {timeout, 60, fun t_get_by_pk_latency/0}},
            {"get_by secondary index latency", {timeout, 60, fun t_get_by_index_latency/0}},

            %% --- Bulk operations ---
            {"insert_all scaling (100/500/1k/5k/10k)", {timeout, 180, fun t_insert_all_scaling/0}},
            {"update_all bulk", {timeout, 60, fun t_update_all_bulk/0}},
            {"delete_all bulk", {timeout, 60, fun t_delete_all_bulk/0}},

            %% --- WHERE clause complexity ---
            {"simple equality WHERE", {timeout, 60, fun t_where_simple_equality/0}},
            {"compound AND/OR WHERE", {timeout, 60, fun t_where_compound/0}},
            {"IN clause with varying sizes", {timeout, 60, fun t_where_in_clause/0}},
            {"LIKE/ILIKE pattern matching", {timeout, 60, fun t_where_like/0}},
            {"BETWEEN range queries", {timeout, 60, fun t_where_between/0}},
            {"IS NULL / IS NOT NULL", {timeout, 60, fun t_where_null_checks/0}},
            {"nested boolean logic (AND within OR)", {timeout, 60, fun t_where_nested_boolean/0}},

            %% --- JOINs ---
            {"INNER JOIN two tables", {timeout, 60, fun t_inner_join_two/0}},
            {"INNER JOIN three tables", {timeout, 60, fun t_inner_join_three/0}},
            {"LEFT JOIN (sparse data)", {timeout, 60, fun t_left_join_sparse/0}},
            {"LEFT JOIN (dense data)", {timeout, 60, fun t_left_join_dense/0}},
            {"RIGHT JOIN", {timeout, 60, fun t_right_join/0}},
            {"FULL OUTER JOIN", {timeout, 60, fun t_full_outer_join/0}},
            {"JOIN with WHERE filter", {timeout, 60, fun t_join_with_where/0}},
            {"JOIN with ORDER BY and LIMIT", {timeout, 60, fun t_join_with_order_limit/0}},
            {"self-referencing pattern via JOIN", {timeout, 60, fun t_self_join_pattern/0}},
            {"many-to-many through join table", {timeout, 60, fun t_many_to_many_join/0}},

            %% --- Aggregations ---
            {"COUNT with GROUP BY", {timeout, 60, fun t_count_group_by/0}},
            {"SUM/AVG/MIN/MAX aggregates", {timeout, 60, fun t_aggregates/0}},
            {"GROUP BY with HAVING", {timeout, 60, fun t_group_by_having/0}},
            {"COUNT with JOIN", {timeout, 60, fun t_count_with_join/0}},

            %% --- Ordering and pagination ---
            {"ORDER BY single column", {timeout, 60, fun t_order_by_single/0}},
            {"ORDER BY multiple columns", {timeout, 60, fun t_order_by_multiple/0}},
            {"LIMIT/OFFSET pagination", {timeout, 60, fun t_pagination/0}},
            {"keyset pagination vs OFFSET", {timeout, 60, fun t_keyset_vs_offset/0}},

            %% --- DISTINCT ---
            {"DISTINCT on single column", {timeout, 60, fun t_distinct_single/0}},

            %% --- Subqueries & CTEs ---
            {"WHERE IN subquery", {timeout, 60, fun t_where_in_subquery/0}},
            {"CTE (WITH clause)", {timeout, 60, fun t_cte/0}},

            %% --- Set operations ---
            {"UNION", {timeout, 60, fun t_union/0}},
            {"UNION ALL", {timeout, 60, fun t_union_all/0}},
            {"INTERSECT", {timeout, 60, fun t_intersect/0}},
            {"EXCEPT", {timeout, 60, fun t_except/0}},

            %% --- Preloading ---
            {"preload has_many (50 parents, 5 children each)",
                {timeout, 60, fun t_preload_has_many/0}},
            {"preload belongs_to (250 children)", {timeout, 60, fun t_preload_belongs_to/0}},
            {"nested preload (users -> posts -> comments)", {timeout, 60, fun t_preload_nested/0}},

            %% --- Transactions ---
            {"transaction single insert", {timeout, 60, fun t_transaction_single/0}},
            {"transaction multi-step (kura_multi)", {timeout, 60, fun t_transaction_multi/0}},

            %% --- Concurrency ---
            {"concurrent reads (50 workers)", {timeout, 60, fun t_concurrent_reads/0}},
            {"concurrent writes (50 workers)", {timeout, 60, fun t_concurrent_writes/0}},
            {"mixed read/write (25+25 workers)", {timeout, 60, fun t_concurrent_mixed/0}},

            %% --- Changeset overhead ---
            {"changeset cast + validate (no DB)", {timeout, 60, fun t_changeset_overhead/0}},

            %% --- Streaming ---
            {"stream 10k rows (batch_size 100 vs 500 vs 1000)",
                {timeout, 120, fun t_stream_batch_sizes/0}},

            %% --- Complex real-world queries ---
            {"dashboard query (JOIN + GROUP BY + HAVING + ORDER)",
                {timeout, 60, fun t_dashboard_query/0}},
            {"search query (ILIKE + JOIN + pagination)", {timeout, 60, fun t_search_query/0}},
            {"leaderboard (aggregate + JOIN + ORDER + LIMIT)",
                {timeout, 60, fun t_leaderboard_query/0}},
            {"activity feed (multi-JOIN + ORDER + LIMIT)",
                {timeout, 60, fun t_activity_feed_query/0}}
        ]
    end}.

%%----------------------------------------------------------------------
%% Setup / Teardown
%%----------------------------------------------------------------------

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_stress_repo:start(),
    create_tables(),
    ok.

teardown(_) ->
    drop_tables(),
    ok.

create_tables() ->
    Stmts = [
        "CREATE TABLE IF NOT EXISTS bench_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  score DOUBLE PRECISION DEFAULT 0.0,"
        "  role VARCHAR(50) DEFAULT 'user',"
        "  metadata JSONB,"
        "  inserted_at TIMESTAMPTZ DEFAULT NOW(),"
        "  updated_at TIMESTAMPTZ DEFAULT NOW()"
        ")",
        "CREATE TABLE IF NOT EXISTS bench_posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  body TEXT,"
        "  published BOOLEAN DEFAULT false,"
        "  view_count INTEGER DEFAULT 0,"
        "  rating DOUBLE PRECISION DEFAULT 0.0,"
        "  user_id BIGINT NOT NULL REFERENCES bench_users(id) ON DELETE CASCADE,"
        "  inserted_at TIMESTAMPTZ DEFAULT NOW(),"
        "  updated_at TIMESTAMPTZ DEFAULT NOW()"
        ")",
        "CREATE TABLE IF NOT EXISTS bench_comments ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  body TEXT NOT NULL,"
        "  post_id BIGINT NOT NULL REFERENCES bench_posts(id) ON DELETE CASCADE,"
        "  user_id BIGINT NOT NULL REFERENCES bench_users(id) ON DELETE CASCADE,"
        "  inserted_at TIMESTAMPTZ DEFAULT NOW()"
        ")",
        "CREATE TABLE IF NOT EXISTS bench_tags ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL UNIQUE"
        ")",
        "CREATE TABLE IF NOT EXISTS bench_post_tags ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  post_id BIGINT NOT NULL REFERENCES bench_posts(id) ON DELETE CASCADE,"
        "  tag_id BIGINT NOT NULL REFERENCES bench_tags(id) ON DELETE CASCADE,"
        "  UNIQUE(post_id, tag_id)"
        ")",
        "CREATE INDEX IF NOT EXISTS idx_bench_users_email ON bench_users(email)",
        "CREATE INDEX IF NOT EXISTS idx_bench_users_active ON bench_users(active)",
        "CREATE INDEX IF NOT EXISTS idx_bench_users_age ON bench_users(age)",
        "CREATE INDEX IF NOT EXISTS idx_bench_posts_user_id ON bench_posts(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_bench_posts_published ON bench_posts(published)",
        "CREATE INDEX IF NOT EXISTS idx_bench_comments_post_id ON bench_comments(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_bench_comments_user_id ON bench_comments(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_bench_post_tags_post_id ON bench_post_tags(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_bench_post_tags_tag_id ON bench_post_tags(tag_id)"
    ],
    lists:foreach(
        fun(SQL) -> {ok, _} = kura_stress_repo:query(SQL, []) end,
        Stmts
    ).

drop_tables() ->
    lists:foreach(
        fun(T) ->
            kura_stress_repo:query(
                iolist_to_binary(["DROP TABLE IF EXISTS ", T, " CASCADE"]), []
            )
        end,
        [
            <<"bench_post_tags">>,
            <<"bench_comments">>,
            <<"bench_posts">>,
            <<"bench_tags">>,
            <<"bench_users">>
        ]
    ).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

clean() ->
    kura_stress_repo:query(
        "TRUNCATE bench_users, bench_posts, bench_comments, bench_tags, bench_post_tags "
        "RESTART IDENTITY CASCADE",
        []
    ).

seed_users(N) ->
    Entries = [
        #{
            name => iolist_to_binary([<<"user_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"user_">>, integer_to_binary(I), <<"@bench.com">>]),
            age => 18 + (I rem 50),
            active => (I rem 3 =/= 0),
            score => I * 1.5,
            role => lists:nth((I rem 3) + 1, [<<"admin">>, <<"user">>, <<"moderator">>])
        }
     || I <- lists:seq(1, N)
    ],
    {ok, N} = kura_stress_repo:insert_all(kura_bench_schema_user, Entries),
    ok.

seed_posts(UsersCount, PostsPerUser) ->
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    lists:foreach(
        fun(User) ->
            Uid = maps:get(id, User),
            Entries = [
                #{
                    title => iolist_to_binary([
                        <<"post_">>, integer_to_binary(Uid), <<"_">>, integer_to_binary(J)
                    ]),
                    body => iolist_to_binary([<<"Body for post ">>, integer_to_binary(J)]),
                    published => (J rem 2 =:= 0),
                    view_count => J * 10,
                    rating => J * 0.5,
                    user_id => Uid
                }
             || J <- lists:seq(1, PostsPerUser)
            ],
            {ok, PostsPerUser} = kura_stress_repo:insert_all(kura_bench_schema_post, Entries)
        end,
        lists:sublist(Users, UsersCount)
    ),
    ok.

seed_comments(PostLimit, CommentsPerPost) ->
    Q = kura_query:limit(kura_query:from(kura_bench_schema_post), PostLimit),
    {ok, Posts} = kura_stress_repo:all(Q),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    UserIds = [maps:get(id, U) || U <- Users],
    NumUsers = length(UserIds),
    lists:foreach(
        fun(Post) ->
            Pid = maps:get(id, Post),
            Entries = [
                #{
                    body => iolist_to_binary([
                        <<"Comment ">>,
                        integer_to_binary(J),
                        <<" on post ">>,
                        integer_to_binary(Pid)
                    ]),
                    post_id => Pid,
                    user_id => lists:nth((J rem NumUsers) + 1, UserIds)
                }
             || J <- lists:seq(1, CommentsPerPost)
            ],
            {ok, CommentsPerPost} = kura_stress_repo:insert_all(kura_bench_schema_comment, Entries)
        end,
        Posts
    ),
    ok.

seed_tags(N) ->
    Entries = [
        #{name => iolist_to_binary([<<"tag_">>, integer_to_binary(I)])}
     || I <- lists:seq(1, N)
    ],
    {ok, N} = kura_stress_repo:insert_all(kura_bench_schema_tag, Entries),
    ok.

seed_post_tags(PostLimit, TagsPerPost) ->
    Q = kura_query:limit(kura_query:from(kura_bench_schema_post), PostLimit),
    {ok, Posts} = kura_stress_repo:all(Q),
    {ok, Tags} = kura_stress_repo:all(kura_query:from(kura_bench_schema_tag)),
    TagIds = [maps:get(id, T) || T <- Tags],
    NumTags = length(TagIds),
    lists:foreach(
        fun(Post) ->
            Pid = maps:get(id, Post),
            Entries = [
                #{
                    post_id => Pid,
                    tag_id => lists:nth(((J - 1) rem NumTags) + 1, TagIds)
                }
             || J <- lists:seq(1, erlang:min(TagsPerPost, NumTags))
            ],
            {ok, _} = kura_stress_repo:insert_all(kura_bench_schema_post_tag, Entries)
        end,
        Posts
    ),
    ok.

seed_full(UserCount, PostsPerUser, CommentsPerPost, TagCount, TagsPerPost) ->
    seed_users(UserCount),
    seed_posts(UserCount, PostsPerUser),
    seed_comments(UserCount * PostsPerUser, CommentsPerPost),
    seed_tags(TagCount),
    seed_post_tags(UserCount * PostsPerUser, TagsPerPost).

bench(Name, N, Fun) ->
    %% Warmup
    lists:foreach(fun(_) -> Fun() end, lists:seq(1, erlang:min(5, N))),
    %% Measure
    Latencies = lists:map(
        fun(_) ->
            T1 = erlang:monotonic_time(microsecond),
            Fun(),
            erlang:monotonic_time(microsecond) - T1
        end,
        lists:seq(1, N)
    ),
    Sorted = lists:sort(Latencies),
    P50 = lists:nth(erlang:max(1, round(N * 0.50)), Sorted),
    P95 = lists:nth(erlang:max(1, round(N * 0.95)), Sorted),
    P99 = lists:nth(erlang:max(1, round(N * 0.99)), Sorted),
    Avg = round(lists:sum(Latencies) / N),
    Min = hd(Sorted),
    Max = lists:last(Sorted),
    Total = lists:sum(Latencies),
    Rate = round(N / (Total / 1_000_000)),
    ?debugFmt(
        "~n  ~s (~p iterations)~n"
        "    p50=~pus  p95=~pus  p99=~pus~n"
        "    avg=~pus  min=~pus  max=~pus~n"
        "    throughput=~p ops/sec",
        [Name, N, P50, P95, P99, Avg, Min, Max, Rate]
    ),
    {P50, P95, P99, Avg, Rate}.

bench_concurrent(Name, Workers, ItersPerWorker, Fun) ->
    Self = self(),
    Start = erlang:monotonic_time(microsecond),
    _Pids = [
        spawn_link(fun() ->
            Lats = lists:map(
                fun(_) ->
                    T1 = erlang:monotonic_time(microsecond),
                    Fun(),
                    erlang:monotonic_time(microsecond) - T1
                end,
                lists:seq(1, ItersPerWorker)
            ),
            Self ! {worker_done, W, Lats}
        end)
     || W <- lists:seq(1, Workers)
    ],
    AllLats = lists:flatmap(
        fun(W) ->
            receive
                {worker_done, W, Lats} -> Lats
            after 30000 -> error({timeout, W})
            end
        end,
        lists:seq(1, Workers)
    ),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    Total = Workers * ItersPerWorker,
    Sorted = lists:sort(AllLats),
    P50 = lists:nth(erlang:max(1, round(Total * 0.50)), Sorted),
    P95 = lists:nth(erlang:max(1, round(Total * 0.95)), Sorted),
    P99 = lists:nth(erlang:max(1, round(Total * 0.99)), Sorted),
    Rate = round(Total / (Elapsed / 1_000_000)),
    ?debugFmt(
        "~n  ~s (~p workers x ~p iters = ~p total)~n"
        "    p50=~pus  p95=~pus  p99=~pus~n"
        "    wall_time=~pms  throughput=~p ops/sec",
        [Name, Workers, ItersPerWorker, Total, P50, P95, P99, round(Elapsed / 1000), Rate]
    ),
    {P50, P95, P99, Rate}.

%%======================================================================
%% CRUD Basics
%%======================================================================

t_single_insert_latency() ->
    clean(),
    Counter = atomics:new(1, [{signed, false}]),
    {_P50, _P95, P99, _Avg, _Rate} = bench("single insert", 500, fun() ->
        I = atomics:add_get(Counter, 1, 1),
        Name = iolist_to_binary([<<"ins_">>, integer_to_binary(I)]),
        Email = iolist_to_binary([Name, <<"@bench.com">>]),
        CS = kura_changeset:cast(
            kura_bench_schema_user, #{}, #{<<"name">> => Name, <<"email">> => Email}, [name, email]
        ),
        CS1 = kura_changeset:validate_required(CS, [name, email]),
        {ok, _} = kura_stress_repo:insert(CS1)
    end),
    ?assert(P99 < 50_000).

t_single_update_latency() ->
    clean(),
    seed_users(100),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    Ids = [maps:get(id, U) || U <- Users],
    NumIds = length(Ids),
    Counter = atomics:new(1, [{signed, false}]),
    {_P50, _P95, P99, _Avg, _Rate} = bench("single update", 500, fun() ->
        I = atomics:add_get(Counter, 1, 1),
        Id = lists:nth((I rem NumIds) + 1, Ids),
        {ok, Row} = kura_stress_repo:get(kura_bench_schema_user, Id),
        CS = kura_changeset:cast(
            kura_bench_schema_user,
            Row,
            #{<<"name">> => iolist_to_binary([<<"upd_">>, integer_to_binary(I)])},
            [name]
        ),
        {ok, _} = kura_stress_repo:update(CS)
    end),
    ?assert(P99 < 50_000).

t_single_delete_latency() ->
    clean(),
    N = 300,
    Warmup = 5,
    seed_users(N + Warmup),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    Ref = atomics:new(1, [{signed, false}]),
    UserArr = list_to_tuple(Users),
    {_P50, _P95, P99, _Avg, _Rate} = bench("single delete", N, fun() ->
        I = atomics:add_get(Ref, 1, 1),
        Row = element(I, UserArr),
        CS = kura_changeset:cast(kura_bench_schema_user, Row, #{}, []),
        {ok, _} = kura_stress_repo:delete(CS)
    end),
    ?assert(P99 < 50_000).

t_get_by_pk_latency() ->
    clean(),
    seed_users(100),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    Ids = [maps:get(id, U) || U <- Users],
    NumIds = length(Ids),
    {_P50, _P95, P99, _Avg, _Rate} = bench("get by PK", 1000, fun() ->
        Id = lists:nth(rand:uniform(NumIds), Ids),
        {ok, _} = kura_stress_repo:get(kura_bench_schema_user, Id)
    end),
    ?assert(P99 < 10_000).

t_get_by_index_latency() ->
    clean(),
    seed_users(100),
    {_P50, _P95, P99, _Avg, _Rate} = bench("get_by email index", 500, fun() ->
        I = rand:uniform(100),
        Email = iolist_to_binary([<<"user_">>, integer_to_binary(I), <<"@bench.com">>]),
        {ok, _} = kura_stress_repo:get_by(kura_bench_schema_user, [{email, Email}])
    end),
    ?assert(P99 < 10_000).

%%======================================================================
%% Bulk operations
%%======================================================================

t_insert_all_scaling() ->
    lists:foreach(
        fun(Size) ->
            clean(),
            Entries = [
                #{
                    name => iolist_to_binary([<<"bulk_">>, integer_to_binary(I)]),
                    email => iolist_to_binary([<<"bulk_">>, integer_to_binary(I), <<"@bench.com">>])
                }
             || I <- lists:seq(1, Size)
            ],
            T1 = erlang:monotonic_time(microsecond),
            {ok, Count} = kura_stress_repo:insert_all(kura_bench_schema_user, Entries),
            Elapsed = erlang:monotonic_time(microsecond) - T1,
            Rate = round(Count / (Elapsed / 1_000_000)),
            ?debugFmt("~n  insert_all ~p rows: ~p rows/sec (~pms)", [
                Size, Rate, round(Elapsed / 1000)
            ]),
            ?assertEqual(Size, Count)
        end,
        [100, 500, 1000, 5000, 10000]
    ).

t_update_all_bulk() ->
    clean(),
    seed_users(1000),
    lists:foreach(
        fun({Label, Where}) ->
            T1 = erlang:monotonic_time(microsecond),
            Q = kura_query:where(kura_query:from(kura_bench_schema_user), Where),
            {ok, Count} = kura_stress_repo:update_all(Q, #{score => 99.9}),
            Elapsed = erlang:monotonic_time(microsecond) - T1,
            ?debugFmt("~n  update_all ~s: ~p rows in ~pms", [
                Label, Count, round(Elapsed / 1000)
            ]),
            ?assert(Count > 0)
        end,
        [
            {"all rows", {active, is_not_nil}},
            {"filtered (active=true)", {active, true}},
            {"filtered (age > 40)", {age, '>', 40}}
        ]
    ).

t_delete_all_bulk() ->
    clean(),
    seed_users(1000),
    T1 = erlang:monotonic_time(microsecond),
    Q = kura_query:where(kura_query:from(kura_bench_schema_user), {active, false}),
    {ok, Count} = kura_stress_repo:delete_all(Q),
    Elapsed = erlang:monotonic_time(microsecond) - T1,
    ?debugFmt("~n  delete_all (active=false): ~p rows in ~pms", [
        Count, round(Elapsed / 1000)
    ]),
    ?assert(Count > 0).

%%======================================================================
%% WHERE clause complexity
%%======================================================================

t_where_simple_equality() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE active = true", 500, fun() ->
        Q = kura_query:where(kura_query:from(kura_bench_schema_user), {active, true}),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

t_where_compound() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE AND/OR compound", 500, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_user),
            {'or', [
                {'and', [{active, true}, {age, '>', 30}]},
                {role, <<"admin">>}
            ]}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

t_where_in_clause() ->
    clean(),
    seed_users(1000),
    lists:foreach(
        fun(Size) ->
            Ids = lists:seq(1, Size),
            {_P50, _P95, _P99, _Avg, _Rate} = bench(
                io_lib:format("WHERE id IN (~p items)", [Size]), 200, fun() ->
                    Q = kura_query:where(
                        kura_query:from(kura_bench_schema_user), {id, in, Ids}
                    ),
                    {ok, _} = kura_stress_repo:all(Q)
                end
            )
        end,
        [10, 50, 200, 500]
    ).

t_where_like() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE name ILIKE", 500, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_user), {name, ilike, <<"user_1%">>}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

t_where_between() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE age BETWEEN", 500, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_user), {age, between, {25, 45}}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

t_where_null_checks() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE age IS NOT NULL", 500, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_user), {age, is_not_nil}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

t_where_nested_boolean() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE nested boolean", 500, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_user),
            {'and', [
                {'or', [{age, '>', 30}, {role, <<"admin">>}]},
                {'or', [{active, true}, {score, '>', 100.0}]}
            ]}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 50_000).

%%======================================================================
%% JOINs
%%======================================================================

t_inner_join_two() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("INNER JOIN users<->posts", 500, fun() ->
        Q = kura_query:join(
            kura_query:from(kura_bench_schema_user),
            inner,
            kura_bench_schema_post,
            {id, user_id}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_inner_join_three() ->
    clean(),
    seed_users(50),
    seed_posts(50, 5),
    seed_comments(250, 3),
    {_P50, _P95, P99, _Avg, _Rate} = bench("INNER JOIN 3 tables", 300, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:join(Q1, inner, kura_bench_schema_comment, {id, post_id}),
        {ok, _} = kura_stress_repo:all(Q2)
    end),
    ?assert(P99 < 200_000).

t_left_join_sparse() ->
    clean(),
    seed_users(200),
    %% Only 20 users get posts — 180 will have NULL on the right
    seed_posts(20, 3),
    {_P50, _P95, P99, _Avg, _Rate} = bench("LEFT JOIN (sparse, 10% have posts)", 500, fun() ->
        Q = kura_query:join(
            kura_query:from(kura_bench_schema_user),
            left,
            kura_bench_schema_post,
            {id, user_id}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_left_join_dense() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("LEFT JOIN (dense, 100% have posts)", 500, fun() ->
        Q = kura_query:join(
            kura_query:from(kura_bench_schema_user),
            left,
            kura_bench_schema_post,
            {id, user_id}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_right_join() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("RIGHT JOIN users->posts", 500, fun() ->
        Q = kura_query:join(
            kura_query:from(kura_bench_schema_user),
            right,
            kura_bench_schema_post,
            {id, user_id}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_full_outer_join() ->
    clean(),
    seed_users(200),
    seed_posts(100, 3),
    {_P50, _P95, P99, _Avg, _Rate} = bench("FULL OUTER JOIN", 500, fun() ->
        Q = kura_query:join(
            kura_query:from(kura_bench_schema_user),
            full,
            kura_bench_schema_post,
            {id, user_id}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_join_with_where() ->
    clean(),
    seed_users(100),
    seed_posts(100, 10),
    {_P50, _P95, P99, _Avg, _Rate} = bench("INNER JOIN + WHERE filter", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:where(Q1, {active, true}),
        Q3 = kura_query:where(Q2, {published, true}),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 100_000).

t_join_with_order_limit() ->
    clean(),
    seed_users(100),
    seed_posts(100, 10),
    {_P50, _P95, P99, _Avg, _Rate} = bench("JOIN + ORDER BY + LIMIT", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:order_by(Q1, [{view_count, desc}]),
        Q3 = kura_query:limit(Q2, 20),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 50_000).

t_self_join_pattern() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    %% Find posts by same user — simulates self-referencing via join on same FK
    {_P50, _P95, P99, _Avg, _Rate} = bench("self-join pattern (posts by same user)", 300, fun() ->
        Q0 = kura_query:from(kura_bench_schema_post),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {user_id, user_id}, p2),
        Q2 = kura_query:limit(Q1, 50),
        {ok, _} = kura_stress_repo:all(Q2)
    end),
    ?assert(P99 < 100_000).

t_many_to_many_join() ->
    clean(),
    seed_users(50),
    seed_posts(50, 5),
    seed_tags(20),
    seed_post_tags(250, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench(
        "many-to-many (posts<->tags via join table)", 300, fun() ->
            Q0 = kura_query:from(kura_bench_schema_post),
            Q1 = kura_query:join(Q0, inner, kura_bench_schema_post_tag, {id, post_id}),
            Q2 = kura_query:join(Q1, inner, kura_bench_schema_tag, {tag_id, id}),
            Q3 = kura_query:limit(Q2, 50),
            {ok, _} = kura_stress_repo:all(Q3)
        end
    ),
    ?assert(P99 < 100_000).

%%======================================================================
%% Aggregations
%%======================================================================

t_count_group_by() ->
    clean(),
    seed_users(500),
    {_P50, _P95, P99, _Avg, _Rate} = bench("COUNT + GROUP BY role", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:select(Q0, [role, {count, '*'}]),
        Q2 = kura_query:group_by(Q1, [role]),
        {ok, _} = kura_stress_repo:all(Q2)
    end),
    ?assert(P99 < 50_000).

t_aggregates() ->
    clean(),
    seed_users(100),
    seed_posts(100, 10),
    lists:foreach(
        fun({AggName, AggFun}) when is_function(AggFun, 1) ->
            {_P50, _P95, _P99, _Avg, _Rate} = bench(
                io_lib:format("~s(view_count)", [AggName]), 300, fun() ->
                    Q = AggFun(kura_query:from(kura_bench_schema_post)),
                    {ok, _} = kura_stress_repo:all(Q)
                end
            )
        end,
        [
            {"SUM", fun(Q) -> kura_query:sum(Q, view_count) end},
            {"AVG", fun(Q) -> kura_query:avg(Q, view_count) end},
            {"MIN", fun(Q) -> kura_query:min(Q, view_count) end},
            {"MAX", fun(Q) -> kura_query:max(Q, view_count) end},
            {"COUNT", fun kura_query:count/1}
        ]
    ).

t_group_by_having() ->
    clean(),
    seed_users(100),
    seed_posts(100, 10),
    {_P50, _P95, P99, _Avg, _Rate} = bench("GROUP BY + HAVING", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_post),
        Q1 = kura_query:select(Q0, [user_id, {count, '*'}, {sum, view_count}]),
        Q2 = kura_query:group_by(Q1, [user_id]),
        Q3 = kura_query:having(Q2, {fragment, <<"count(*) > ?">>, [3]}),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 50_000).

t_count_with_join() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("COUNT with JOIN", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:select(Q1, [name, {count, '*'}]),
        Q3 = kura_query:group_by(Q2, [name]),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 50_000).

%%======================================================================
%% Ordering and pagination
%%======================================================================

t_order_by_single() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("ORDER BY name ASC", 500, fun() ->
        Q = kura_query:order_by(kura_query:from(kura_bench_schema_user), [{name, asc}]),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_order_by_multiple() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("ORDER BY role ASC, score DESC", 500, fun() ->
        Q = kura_query:order_by(
            kura_query:from(kura_bench_schema_user),
            [{role, asc}, {score, desc}]
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_pagination() ->
    clean(),
    seed_users(1000),
    PageSize = 20,
    Pages = [0, 5, 10, 25, 49],
    lists:foreach(
        fun(Page) ->
            {_P50, _P95, _P99, _Avg, _Rate} = bench(
                io_lib:format("OFFSET ~p LIMIT ~p", [Page * PageSize, PageSize]), 200, fun() ->
                    Q0 = kura_query:from(kura_bench_schema_user),
                    Q1 = kura_query:order_by(Q0, [{id, asc}]),
                    Q2 = kura_query:limit(Q1, PageSize),
                    Q3 = kura_query:offset(Q2, Page * PageSize),
                    {ok, _} = kura_stress_repo:all(Q3)
                end
            )
        end,
        Pages
    ).

t_keyset_vs_offset() ->
    clean(),
    seed_users(5000),
    PageSize = 20,
    %% OFFSET-based at page 200 (offset 4000)
    {_P50Off, _P95Off, _P99Off, AvgOff, _RateOff} = bench(
        "OFFSET 4000 LIMIT 20", 200, fun() ->
            Q0 = kura_query:from(kura_bench_schema_user),
            Q1 = kura_query:order_by(Q0, [{id, asc}]),
            Q2 = kura_query:limit(Q1, PageSize),
            Q3 = kura_query:offset(Q2, 4000),
            {ok, _} = kura_stress_repo:all(Q3)
        end
    ),
    %% Keyset-based (WHERE id > last_id LIMIT 20)
    {_P50Key, _P95Key, _P99Key, AvgKey, _RateKey} = bench(
        "WHERE id > 4000 LIMIT 20 (keyset)", 200, fun() ->
            Q0 = kura_query:from(kura_bench_schema_user),
            Q1 = kura_query:where(Q0, {id, '>', 4000}),
            Q2 = kura_query:order_by(Q1, [{id, asc}]),
            Q3 = kura_query:limit(Q2, PageSize),
            {ok, _} = kura_stress_repo:all(Q3)
        end
    ),
    ?debugFmt("~n  Keyset vs OFFSET speedup: ~.1fx", [AvgOff / erlang:max(1, AvgKey)]).

%%======================================================================
%% DISTINCT
%%======================================================================

t_distinct_single() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Avg, _Rate} = bench("DISTINCT role", 500, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:select(Q0, [role]),
        Q2 = kura_query:distinct(Q1),
        {ok, _} = kura_stress_repo:all(Q2)
    end),
    ?assert(P99 < 50_000).

%%======================================================================
%% Subqueries & CTEs
%%======================================================================

t_where_in_subquery() ->
    clean(),
    seed_users(100),
    seed_posts(100, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("WHERE user_id IN (fragment subquery)", 300, fun() ->
        Q = kura_query:where(
            kura_query:from(kura_bench_schema_post),
            {fragment, <<"user_id IN (SELECT id FROM bench_users WHERE active = true)">>, []}
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_cte() ->
    clean(),
    seed_users(100),
    seed_posts(100, 10),
    {_P50, _P95, P99, _Avg, _Rate} = bench("CTE (WITH via raw SQL)", 300, fun() ->
        {ok, _} = kura_stress_repo:query(
            "WITH active_users AS ("
            "  SELECT id FROM bench_users WHERE active = true"
            ") "
            "SELECT p.* FROM bench_posts p "
            "INNER JOIN active_users au ON au.id = p.user_id",
            []
        )
    end),
    ?assert(P99 < 100_000).

%%======================================================================
%% Set operations
%%======================================================================

t_union() ->
    clean(),
    seed_users(500),
    {_P50, _P95, P99, _Avg, _Rate} = bench("UNION", 300, fun() ->
        Q1 = kura_query:where(kura_query:from(kura_bench_schema_user), {active, true}),
        Q2 = kura_query:where(kura_query:from(kura_bench_schema_user), {role, <<"admin">>}),
        Q3 = kura_query:union(Q1, Q2),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 100_000).

t_union_all() ->
    clean(),
    seed_users(500),
    {_P50, _P95, P99, _Avg, _Rate} = bench("UNION ALL", 300, fun() ->
        Q1 = kura_query:where(kura_query:from(kura_bench_schema_user), {active, true}),
        Q2 = kura_query:where(kura_query:from(kura_bench_schema_user), {role, <<"admin">>}),
        Q3 = kura_query:union_all(Q1, Q2),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 100_000).

t_intersect() ->
    clean(),
    seed_users(500),
    {_P50, _P95, P99, _Avg, _Rate} = bench("INTERSECT", 300, fun() ->
        Q1 = kura_query:where(kura_query:from(kura_bench_schema_user), {active, true}),
        Q2 = kura_query:where(kura_query:from(kura_bench_schema_user), {age, '>', 30}),
        Q3 = kura_query:intersect(Q1, Q2),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 100_000).

t_except() ->
    clean(),
    seed_users(500),
    {_P50, _P95, P99, _Avg, _Rate} = bench("EXCEPT", 300, fun() ->
        Q1 = kura_query:from(kura_bench_schema_user),
        Q2 = kura_query:where(kura_query:from(kura_bench_schema_user), {role, <<"admin">>}),
        Q3 = kura_query:except(Q1, Q2),
        {ok, _} = kura_stress_repo:all(Q3)
    end),
    ?assert(P99 < 100_000).

%%======================================================================
%% Preloading
%%======================================================================

t_preload_has_many() ->
    clean(),
    seed_users(50),
    seed_posts(50, 5),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    {_P50, _P95, P99, _Avg, _Rate} = bench(
        "preload has_many (50 users, 5 posts each)", 200, fun() ->
            _Loaded = kura_stress_repo:preload(kura_bench_schema_user, Users, [posts]),
            ok
        end
    ),
    ?assert(P99 < 100_000).

t_preload_belongs_to() ->
    clean(),
    seed_users(50),
    seed_posts(50, 5),
    {ok, Posts} = kura_stress_repo:all(kura_query:from(kura_bench_schema_post)),
    {_P50, _P95, P99, _Avg, _Rate} = bench("preload belongs_to (250 posts)", 200, fun() ->
        _Loaded = kura_stress_repo:preload(kura_bench_schema_post, Posts, [user]),
        ok
    end),
    ?assert(P99 < 100_000).

t_preload_nested() ->
    clean(),
    seed_users(30),
    seed_posts(30, 4),
    seed_comments(120, 3),
    {ok, Users} = kura_stress_repo:all(kura_query:from(kura_bench_schema_user)),
    {_P50, _P95, P99, _Avg, _Rate} = bench("nested preload (users->posts->comments)", 100, fun() ->
        _Loaded = kura_stress_repo:preload(
            kura_bench_schema_user, Users, [{posts, [comments]}]
        ),
        ok
    end),
    ?assert(P99 < 200_000).

%%======================================================================
%% Transactions
%%======================================================================

t_transaction_single() ->
    clean(),
    Counter = atomics:new(1, [{signed, false}]),
    {_P50, _P95, P99, _Avg, _Rate} = bench("transaction (single insert)", 300, fun() ->
        I = atomics:add_get(Counter, 1, 1),
        kura_stress_repo:transaction(fun() ->
            Name = iolist_to_binary([<<"txn_">>, integer_to_binary(I)]),
            Email = iolist_to_binary([Name, <<"@bench.com">>]),
            CS = kura_changeset:cast(
                kura_bench_schema_user,
                #{},
                #{<<"name">> => Name, <<"email">> => Email},
                [name, email]
            ),
            CS1 = kura_changeset:validate_required(CS, [name, email]),
            {ok, _} = kura_stress_repo:insert(CS1)
        end)
    end),
    ?assert(P99 < 50_000).

t_transaction_multi() ->
    clean(),
    Counter = atomics:new(1, [{signed, false}]),
    {_P50, _P95, P99, _Avg, _Rate} = bench("kura_multi (insert user + post)", 200, fun() ->
        I = atomics:add_get(Counter, 1, 1),
        Name = iolist_to_binary([<<"multi_">>, integer_to_binary(I)]),
        Email = iolist_to_binary([Name, <<"@bench.com">>]),
        UserCS = kura_changeset:cast(
            kura_bench_schema_user,
            #{},
            #{<<"name">> => Name, <<"email">> => Email},
            [name, email]
        ),
        UserCS1 = kura_changeset:validate_required(UserCS, [name, email]),
        M = kura_multi:new(),
        M1 = kura_multi:insert(M, create_user, UserCS1),
        M2 = kura_multi:run(M1, create_post, fun(#{create_user := User}) ->
            Uid = maps:get(id, User),
            PostCS = kura_changeset:cast(
                kura_bench_schema_post,
                #{},
                #{<<"title">> => <<"Multi post">>, <<"user_id">> => Uid},
                [title, user_id]
            ),
            PostCS1 = kura_changeset:validate_required(PostCS, [title, user_id]),
            kura_stress_repo:insert(PostCS1)
        end),
        {ok, _} = kura_stress_repo:multi(M2)
    end),
    ?assert(P99 < 100_000).

%%======================================================================
%% Concurrency
%%======================================================================

t_concurrent_reads() ->
    clean(),
    seed_users(1000),
    {_P50, _P95, P99, _Rate} = bench_concurrent("concurrent reads", 50, 20, fun() ->
        Q = kura_query:limit(
            kura_query:where(kura_query:from(kura_bench_schema_user), {active, true}),
            10
        ),
        {ok, _} = kura_stress_repo:all(Q)
    end),
    ?assert(P99 < 100_000).

t_concurrent_writes() ->
    clean(),
    Counter = atomics:new(1, [{signed, false}]),
    {_P50, _P95, P99, _Rate} = bench_concurrent("concurrent writes", 50, 10, fun() ->
        I = atomics:add_get(Counter, 1, 1),
        Name = iolist_to_binary([<<"cw_">>, integer_to_binary(I)]),
        Email = iolist_to_binary([Name, <<"@bench.com">>]),
        CS = kura_changeset:cast(
            kura_bench_schema_user,
            #{},
            #{<<"name">> => Name, <<"email">> => Email},
            [name, email]
        ),
        CS1 = kura_changeset:validate_required(CS, [name, email]),
        {ok, _} = kura_stress_repo:insert(CS1)
    end),
    ?assert(P99 < 100_000).

t_concurrent_mixed() ->
    clean(),
    seed_users(500),
    Counter = atomics:new(1, [{signed, false}]),
    Self = self(),
    Start = erlang:monotonic_time(microsecond),
    %% 25 readers
    ReaderPids = [
        spawn_link(fun() ->
            Lats = lists:map(
                fun(_) ->
                    T1 = erlang:monotonic_time(microsecond),
                    Q = kura_query:limit(kura_query:from(kura_bench_schema_user), 10),
                    {ok, _} = kura_stress_repo:all(Q),
                    erlang:monotonic_time(microsecond) - T1
                end,
                lists:seq(1, 20)
            ),
            Self ! {reader, W, Lats}
        end)
     || W <- lists:seq(1, 25)
    ],
    %% 25 writers
    WriterPids = [
        spawn_link(fun() ->
            Lats = lists:map(
                fun(_) ->
                    T1 = erlang:monotonic_time(microsecond),
                    I = atomics:add_get(Counter, 1, 1),
                    Name = iolist_to_binary([<<"mx_">>, integer_to_binary(I)]),
                    Email = iolist_to_binary([Name, <<"@bench.com">>]),
                    CS = kura_changeset:cast(
                        kura_bench_schema_user,
                        #{},
                        #{<<"name">> => Name, <<"email">> => Email},
                        [name, email]
                    ),
                    CS1 = kura_changeset:validate_required(CS, [name, email]),
                    {ok, _} = kura_stress_repo:insert(CS1),
                    erlang:monotonic_time(microsecond) - T1
                end,
                lists:seq(1, 10)
            ),
            Self ! {writer, W, Lats}
        end)
     || W <- lists:seq(1, 25)
    ],
    ReadLats = lists:flatmap(
        fun(W) ->
            receive
                {reader, W, L} -> L
            after 30000 -> error({timeout, reader, W})
            end
        end,
        lists:seq(1, 25)
    ),
    WriteLats = lists:flatmap(
        fun(W) ->
            receive
                {writer, W, L} -> L
            after 30000 -> error({timeout, writer, W})
            end
        end,
        lists:seq(1, 25)
    ),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    TotalOps = length(ReadLats) + length(WriteLats),
    SortedR = lists:sort(ReadLats),
    SortedW = lists:sort(WriteLats),
    P99R = lists:nth(round(length(SortedR) * 0.99), SortedR),
    P99W = lists:nth(round(length(SortedW) * 0.99), SortedW),
    ?debugFmt(
        "~n  mixed read/write (25+25 workers)~n"
        "    reads:  p99=~pus  writes: p99=~pus~n"
        "    total=~p ops in ~pms (~p ops/sec)",
        [P99R, P99W, TotalOps, round(Elapsed / 1000), round(TotalOps / (Elapsed / 1_000_000))]
    ),
    ?assertEqual(25, length(ReaderPids)),
    ?assertEqual(25, length(WriterPids)),
    ?assert(P99R < 100_000),
    ?assert(P99W < 100_000).

%%======================================================================
%% Changeset overhead
%%======================================================================

t_changeset_overhead() ->
    %% Pure in-memory, no DB — measures changeset construction cost
    N = 10000,
    T1 = erlang:monotonic_time(microsecond),
    lists:foreach(
        fun(I) ->
            Params = #{
                <<"name">> => iolist_to_binary([<<"u">>, integer_to_binary(I)]),
                <<"email">> => iolist_to_binary([<<"u">>, integer_to_binary(I), <<"@x.com">>]),
                <<"age">> => I rem 100,
                <<"active">> => true,
                <<"score">> => I * 1.1
            },
            CS = kura_changeset:cast(
                kura_bench_schema_user, #{}, Params, [name, email, age, active, score]
            ),
            CS1 = kura_changeset:validate_required(CS, [name, email]),
            CS2 = kura_changeset:validate_length(CS1, name, [{min, 1}, {max, 255}]),
            _CS3 = kura_changeset:validate_number(CS2, age, [{greater_than, 0}, {less_than, 200}])
        end,
        lists:seq(1, N)
    ),
    Elapsed = erlang:monotonic_time(microsecond) - T1,
    Rate = round(N / (Elapsed / 1_000_000)),
    Avg = round(Elapsed / N),
    ?debugFmt("~n  changeset overhead (~p iterations): ~p/sec, avg=~pus", [N, Rate, Avg]),
    ?assert(Rate > 10000).

%%======================================================================
%% Streaming
%%======================================================================

t_stream_batch_sizes() ->
    clean(),
    %% Insert 10k rows
    lists:foreach(
        fun(Batch) ->
            Offset = (Batch - 1) * 1000,
            Entries = [
                #{
                    name => iolist_to_binary([<<"s_">>, integer_to_binary(Offset + I)]),
                    email => iolist_to_binary([
                        <<"s_">>, integer_to_binary(Offset + I), <<"@bench.com">>
                    ])
                }
             || I <- lists:seq(1, 1000)
            ],
            {ok, 1000} = kura_stress_repo:insert_all(kura_bench_schema_user, Entries)
        end,
        lists:seq(1, 10)
    ),
    Q = kura_query:from(kura_bench_schema_user),
    lists:foreach(
        fun(BatchSize) ->
            RowCount = atomics:new(1, [{signed, false}]),
            BatchCount = atomics:new(1, [{signed, false}]),
            T1 = erlang:monotonic_time(microsecond),
            ok = kura_stream:stream(
                kura_stress_repo,
                Q,
                fun(Rows) ->
                    atomics:add(RowCount, 1, length(Rows)),
                    atomics:add(BatchCount, 1, 1),
                    ok
                end,
                #{batch_size => BatchSize}
            ),
            Elapsed = erlang:monotonic_time(microsecond) - T1,
            Total = atomics:get(RowCount, 1),
            Batches = atomics:get(BatchCount, 1),
            Rate = round(Total / (Elapsed / 1_000_000)),
            ?debugFmt("~n  stream batch_size=~p: ~p rows in ~p batches, ~p rows/sec (~pms)", [
                BatchSize, Total, Batches, Rate, round(Elapsed / 1000)
            ]),
            ?assertEqual(10000, Total)
        end,
        [100, 500, 1000]
    ).

%%======================================================================
%% Complex real-world queries
%%======================================================================

t_dashboard_query() ->
    clean(),
    seed_full(100, 10, 3, 20, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("dashboard (JOIN+GROUP BY+HAVING+ORDER)", 200, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:select(Q1, [name, {count, '*'}, {sum, view_count}]),
        Q3 = kura_query:group_by(Q2, [name]),
        Q4 = kura_query:having(Q3, {fragment, <<"count(*) > ?">>, [3]}),
        Q5 = kura_query:order_by(Q4, [{count, desc}]),
        Q6 = kura_query:limit(Q5, 20),
        {ok, _} = kura_stress_repo:all(Q6)
    end),
    ?assert(P99 < 100_000).

t_search_query() ->
    clean(),
    seed_full(100, 10, 3, 20, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("search (ILIKE+JOIN+pagination)", 200, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:where(Q1, {title, ilike, <<"post_1%">>}),
        Q3 = kura_query:where(Q2, {published, true}),
        Q4 = kura_query:order_by(Q3, [{view_count, desc}]),
        Q5 = kura_query:limit(Q4, 20),
        Q6 = kura_query:offset(Q5, 0),
        {ok, _} = kura_stress_repo:all(Q6)
    end),
    ?assert(P99 < 50_000).

t_leaderboard_query() ->
    clean(),
    seed_full(100, 10, 3, 20, 5),
    {_P50, _P95, P99, _Avg, _Rate} = bench("leaderboard (agg+JOIN+ORDER+LIMIT)", 200, fun() ->
        Q0 = kura_query:from(kura_bench_schema_user),
        Q1 = kura_query:join(Q0, left, kura_bench_schema_post, {id, user_id}),
        Q2 = kura_query:select(Q1, [name, {count, '*'}, {sum, view_count}, {avg, rating}]),
        Q3 = kura_query:group_by(Q2, [name]),
        Q4 = kura_query:order_by(Q3, [{sum, desc}]),
        Q5 = kura_query:limit(Q4, 10),
        {ok, _} = kura_stress_repo:all(Q5)
    end),
    ?assert(P99 < 100_000).

t_activity_feed_query() ->
    clean(),
    seed_full(50, 5, 3, 10, 3),
    {_P50, _P95, P99, _Avg, _Rate} = bench(
        "activity feed (multi-JOIN+ORDER+LIMIT)", 200, fun() ->
            Q0 = kura_query:from(kura_bench_schema_user),
            Q1 = kura_query:join(Q0, inner, kura_bench_schema_post, {id, user_id}),
            Q2 = kura_query:join(Q1, inner, kura_bench_schema_comment, {id, post_id}),
            Q3 = kura_query:where(Q2, {published, true}),
            Q4 = kura_query:select(Q3, [name, email]),
            Q5 = kura_query:order_by(Q4, [{name, desc}]),
            Q6 = kura_query:limit(Q5, 50),
            {ok, _} = kura_stress_repo:all(Q6)
        end
    ),
    ?assert(P99 < 100_000).
