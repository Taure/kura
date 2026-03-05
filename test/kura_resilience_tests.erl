-module(kura_resilience_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixtures — non-docker tests run first, docker tests last
%%----------------------------------------------------------------------

resilience_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"large result set (10k rows)", {timeout, 60, fun t_large_result_set/0}},
            {"cursor cleanup on process crash",
                {timeout, 60, fun t_cursor_cleanup_on_process_crash/0}}
        ]
    end}.

%% Docker tests are separate so pool death doesn't affect other tests
docker_resilience_test_() ->
    {setup, fun setup/0, fun docker_teardown/1, fun(_) ->
        [
            {"query during PG downtime returns error",
                {timeout, 120, fun t_query_during_pg_downtime/0}},
            {"recovery after restart preserves data",
                {timeout, 120, fun t_recovery_after_restart/0}}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    ensure_pg_running(),
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

docker_teardown(_) ->
    ensure_pg_running(),
    wait_for_pg(60),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

docker_compose_path() ->
    {ok, Cwd} = file:get_cwd(),
    filename:join(Cwd, "docker-compose.yml").

docker_compose_cmd() ->
    "docker compose -f " ++ binary_to_list(iolist_to_binary(docker_compose_path())).

stop_pg() ->
    os:cmd(docker_compose_cmd() ++ " stop -t 5 postgres").

start_pg() ->
    os:cmd(docker_compose_cmd() ++ " start postgres").

ensure_pg_running() ->
    start_pg().

wait_for_pg(MaxRetries) ->
    wait_for_pg(MaxRetries, 0).

wait_for_pg(MaxRetries, Attempt) when Attempt >= MaxRetries ->
    error(pg_not_ready);
wait_for_pg(MaxRetries, Attempt) ->
    Cmd = docker_compose_cmd() ++ " exec -T postgres pg_isready -U postgres",
    case string:find(os:cmd(Cmd), "accepting connections") of
        nomatch ->
            timer:sleep(1000),
            wait_for_pg(MaxRetries, Attempt + 1);
        _ ->
            ok
    end.

retry(Fun, MaxRetries, DelayMs) ->
    retry(Fun, MaxRetries, DelayMs, 0, undefined).

retry(_Fun, MaxRetries, _DelayMs, Attempt, LastErr) when Attempt >= MaxRetries ->
    {error, {max_retries, LastErr}};
retry(Fun, MaxRetries, DelayMs, Attempt, _LastErr) ->
    try Fun() of
        {ok, _} = Ok ->
            Ok;
        {error, _} = Err ->
            timer:sleep(DelayMs * (Attempt + 1)),
            retry(Fun, MaxRetries, DelayMs, Attempt + 1, Err)
    catch
        _:Reason ->
            timer:sleep(DelayMs * (Attempt + 1)),
            retry(Fun, MaxRetries, DelayMs, Attempt + 1, Reason)
    end.

clean_tables() ->
    kura_stress_repo:query("TRUNCATE stress_posts, stress_users RESTART IDENTITY CASCADE", []).

%%----------------------------------------------------------------------
%% Non-docker tests
%%----------------------------------------------------------------------

t_large_result_set() ->
    clean_tables(),
    %% Insert 10k rows in batches
    lists:foreach(
        fun(Batch) ->
            Offset = (Batch - 1) * 1000,
            Entries = [
                #{
                    name => iolist_to_binary([<<"big_">>, integer_to_binary(Offset + I)]),
                    email => iolist_to_binary([
                        <<"big_">>, integer_to_binary(Offset + I), <<"@test.com">>
                    ])
                }
             || I <- lists:seq(1, 1000)
            ],
            {ok, 1000} = kura_stress_repo:insert_all(kura_stress_schema, Entries)
        end,
        lists:seq(1, 10)
    ),
    %% Fetch all at once
    Q = kura_query:from(kura_stress_schema),
    {ok, All} = kura_stress_repo:all(Q),
    ?assertEqual(10000, length(All)).

t_cursor_cleanup_on_process_crash() ->
    clean_tables(),
    %% Insert 100 rows
    Entries = [
        #{
            name => iolist_to_binary([<<"cur_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"cur_">>, integer_to_binary(I), <<"@test.com">>])
        }
     || I <- lists:seq(1, 100)
    ],
    {ok, 100} = kura_stress_repo:insert_all(kura_stress_schema, Entries),
    %% Spawn a process that starts streaming then crashes
    Pid = spawn(fun() ->
        try
            Q = kura_query:from(kura_stress_schema),
            kura_stream:stream(
                kura_stress_repo,
                Q,
                fun(_Batch) ->
                    exit(deliberate_crash)
                end,
                #{batch_size => 10}
            )
        catch
            _:_ -> ok
        end
    end),
    MRef = monitor(process, Pid),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 10000 -> error(stream_timeout)
    end,
    timer:sleep(500),
    %% Check for leaked cursors
    Pool = maps:get(pool, kura_stress_repo:config()),
    #{rows := Cursors} = pgo:query(
        <<"SELECT name FROM pg_cursors WHERE name LIKE 'kura_cursor_%'">>,
        [],
        #{pool => Pool}
    ),
    ?assertEqual(0, length(Cursors)).

%%----------------------------------------------------------------------
%% Docker stop/start tests
%%----------------------------------------------------------------------

t_query_during_pg_downtime() ->
    clean_tables(),
    stop_pg(),
    timer:sleep(3000),
    %% Query should fail — try multiple times since pool may have cached connections
    Results = [kura_stress_repo:query("SELECT 1", []) || _ <- lists:seq(1, 5)],
    HasError = lists:any(
        fun
            ({error, _}) -> true;
            (_) -> false
        end,
        Results
    ),
    ?assert(HasError),
    %% Bring PG back
    start_pg(),
    wait_for_pg(60).

t_recovery_after_restart() ->
    %% Ensure PG is up and pool is good — wait for pgo to reconnect from previous test
    ensure_pg_running(),
    wait_for_pg(60),
    try kura_stress_repo:start() catch _:_ -> ok end,
    %% Wait for pgo connection backoff to reconnect (max backoff is 10s)
    timer:sleep(12000),
    %% Create tables if they don't exist
    {ok, _} = retry(
        fun() ->
            kura_stress_repo:query(
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
            )
        end,
        10,
        1000
    ),
    clean_tables(),
    %% Insert data before shutdown — retry in case pool still recovering
    CS = kura_changeset:cast(
        kura_stress_schema,
        #{},
        #{<<"name">> => <<"Survivor">>, <<"email">> => <<"survivor@test.com">>},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    {ok, _} = retry(fun() -> kura_stress_repo:insert(CS1) end, 10, 1000),
    %% Stop and restart PG
    stop_pg(),
    timer:sleep(3000),
    start_pg(),
    wait_for_pg(60),
    %% Pool may need to reconnect — restart pool
    try kura_stress_repo:start() catch _:_ -> ok end,
    timer:sleep(2000),
    %% Retry query with backoff
    {ok, Found} = retry(
        fun() ->
            kura_stress_repo:get_by(kura_stress_schema, [{email, <<"survivor@test.com">>}])
        end,
        20,
        500
    ),
    ?assertEqual(<<"Survivor">>, maps:get(name, Found)).
