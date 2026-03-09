-module(kura_stress_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

stress_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"pool saturation: 50 concurrent inserts on pool_size=20",
                {timeout, 30, fun t_pool_saturation/0}},
            {"concurrent reads under write load",
                {timeout, 30, fun t_concurrent_reads_under_write_load/0}},
            {"long transaction doesn't block others",
                {timeout, 30, fun t_long_transaction_doesnt_block_others/0}},
            {"optimistic lock race between two processes",
                {timeout, 30, fun t_optimistic_lock_race/0}},
            {"unique constraint race among 20 processes",
                {timeout, 30, fun t_unique_constraint_race/0}},
            {"multi atomicity on failure rolls back all steps",
                {timeout, 30, fun t_multi_atomicity_on_failure/0}},
            {"transaction rollback discards inserted row",
                {timeout, 30, fun t_transaction_rollback/0}}
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

insert_stress_user(Name, Email) ->
    CS = kura_changeset:cast(
        kura_stress_schema, #{}, #{<<"name">> => Name, <<"email">> => Email}, [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_stress_repo:insert(CS1).

clean_tables() ->
    kura_stress_repo:query("TRUNCATE stress_posts, stress_users RESTART IDENTITY CASCADE", []).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_pool_saturation() ->
    clean_tables(),
    Self = self(),
    N = 50,
    Pids = [
        spawn_link(fun() ->
            Name = iolist_to_binary([<<"sat_">>, integer_to_binary(I)]),
            Email = iolist_to_binary([Name, <<"@test.com">>]),
            Result = insert_stress_user(Name, Email),
            Self ! {done, I, Result}
        end)
     || I <- lists:seq(1, N)
    ],
    Results = [
        receive
            {done, I, R} -> {I, R}
        after 20000 -> error({timeout, I})
        end
     || I <- lists:seq(1, N)
    ],
    ?assertEqual(N, length(Pids)),
    lists:foreach(
        fun({I, R}) ->
            ?assertMatch({ok, _}, R, io_lib:format("Insert ~p failed: ~p", [I, R]))
        end,
        Results
    ).

t_concurrent_reads_under_write_load() ->
    clean_tables(),
    %% Seed 100 rows
    Entries = [
        #{
            name => iolist_to_binary([<<"rw_">>, integer_to_binary(I)]),
            email => iolist_to_binary([<<"rw_">>, integer_to_binary(I), <<"@test.com">>])
        }
     || I <- lists:seq(1, 100)
    ],
    {ok, 100} = kura_stress_repo:insert_all(kura_stress_schema, Entries),
    Self = self(),
    %% 20 writers
    WriterPids = [
        spawn_link(fun() ->
            Name = iolist_to_binary([<<"crw_">>, integer_to_binary(I)]),
            Email = iolist_to_binary([Name, <<"@test.com">>]),
            R = insert_stress_user(Name, Email),
            Self ! {writer, I, R}
        end)
     || I <- lists:seq(1, 20)
    ],
    %% 20 readers
    ReaderPids = [
        spawn_link(fun() ->
            Q = kura_query:limit(kura_query:from(kura_stress_schema), 10),
            R = kura_stress_repo:all(Q),
            Self ! {reader, I, R}
        end)
     || I <- lists:seq(1, 20)
    ],
    %% Collect all
    lists:foreach(
        fun(I) ->
            receive
                {writer, I, R} -> ?assertMatch({ok, _}, R)
            after 20000 -> error({writer_timeout, I})
            end
        end,
        lists:seq(1, 20)
    ),
    lists:foreach(
        fun(I) ->
            receive
                {reader, I, R} -> ?assertMatch({ok, _}, R)
            after 20000 -> error({reader_timeout, I})
            end
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual(20, length(WriterPids)),
    ?assertEqual(20, length(ReaderPids)).

t_long_transaction_doesnt_block_others() ->
    clean_tables(),
    Self = self(),
    Pool = maps:get(pool, kura_repo:config(kura_stress_repo)),
    %% Spawn a process that holds a transaction open for 2 seconds
    spawn_link(fun() ->
        pgo:transaction(
            fun() ->
                pgo:query(<<"SELECT 1 FROM pg_sleep(2)">>, [], #{pool => Pool}),
                ok
            end,
            #{pool => Pool}
        ),
        Self ! long_txn_done
    end),
    %% Give the long txn a moment to start
    timer:sleep(200),
    %% A simple query should complete quickly on a different connection
    Start = erlang:monotonic_time(millisecond),
    {ok, _} = kura_stress_repo:query("SELECT 1", []),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ?assert(Elapsed < 1500),
    %% Wait for the long txn to finish
    receive
        long_txn_done -> ok
    after 10000 -> error(long_txn_timeout)
    end.

t_optimistic_lock_race() ->
    clean_tables(),
    {ok, User} = insert_stress_user(<<"LockRace">>, <<"lockrace@test.com">>),
    Id = maps:get(id, User),
    Self = self(),
    %% Two processes both read the same row, then try to update
    Do = fun(Tag) ->
        spawn_link(fun() ->
            {ok, Row} = kura_stress_repo:get(kura_stress_schema, Id),
            CS = kura_changeset:cast(
                kura_stress_schema, Row, #{<<"name">> => <<"Updated">>}, [name]
            ),
            CS1 = kura_changeset:optimistic_lock(CS, lock_version),
            R = kura_stress_repo:update(CS1),
            Self ! {Tag, R}
        end)
    end,
    Do(proc1),
    Do(proc2),
    R1 =
        receive
            {proc1, V1} -> V1
        after 10000 -> error(timeout)
        end,
    R2 =
        receive
            {proc2, V2} -> V2
        after 10000 -> error(timeout)
        end,
    %% One should succeed, one should get stale
    Results = lists:sort([element(1, R1), element(1, R2)]),
    ?assertEqual([error, ok], Results).

t_unique_constraint_race() ->
    clean_tables(),
    Self = self(),
    N = 20,
    _Pids = [
        spawn_link(fun() ->
            CS = kura_changeset:cast(
                kura_stress_schema,
                #{},
                #{<<"name">> => <<"RaceUser">>, <<"email">> => <<"race@test.com">>},
                [name, email]
            ),
            CS1 = kura_changeset:validate_required(CS, [name, email]),
            CS2 = kura_changeset:unique_constraint(CS1, email),
            R = kura_stress_repo:insert(CS2),
            Self ! {unique_race, I, R}
        end)
     || I <- lists:seq(1, N)
    ],
    Results = [
        receive
            {unique_race, I, R} -> R
        after 20000 -> error({timeout, I})
        end
     || I <- lists:seq(1, N)
    ],
    Successes = [R || {ok, _} = R <- Results],
    Failures = [R || {error, _} = R <- Results],
    ?assertEqual(1, length(Successes)),
    ?assertEqual(N - 1, length(Failures)).

t_multi_atomicity_on_failure() ->
    clean_tables(),
    M = kura_multi:new(),
    CS = kura_changeset:cast(
        kura_stress_schema,
        #{},
        #{<<"name">> => <<"MultiAtom">>, <<"email">> => <<"multiatom@test.com">>},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    M1 = kura_multi:insert(M, step1, CS1),
    M2 = kura_multi:run(M1, step2, fun(_) -> {error, boom} end),
    {error, step2, boom, _} = kura_stress_repo:multi(M2),
    %% The user from step1 should NOT exist (rolled back)
    ?assertEqual(
        {error, not_found},
        kura_stress_repo:get_by(kura_stress_schema, [{email, <<"multiatom@test.com">>}])
    ).

t_transaction_rollback() ->
    clean_tables(),
    ?assertError(
        rollback_test,
        kura_stress_repo:transaction(fun() ->
            {ok, _} = insert_stress_user(<<"TxnUser">>, <<"txn@test.com">>),
            error(rollback_test)
        end)
    ),
    ?assertEqual(
        {error, not_found},
        kura_stress_repo:get_by(kura_stress_schema, [{email, <<"txn@test.com">>}])
    ).
