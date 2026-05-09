-module(kura_pool_pgo_tests).
-include_lib("eunit/include/eunit.hrl").

-define(POOL, kura_pool_pgo_test).
-define(CONFIG, #{
    host => "localhost",
    port => 5555,
    database => "kura_test",
    user => "postgres",
    password => "root",
    pool_size => 2
}).

with_pool_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"behaviour declares kura_pool", fun behaviour_declared/0},
            {"checkout returns a usable pgo conn", fun checkout_returns_conn/0},
            {"with_conn runs the fun and checks back in", fun with_conn_roundtrip/0},
            {"sequential checkouts reuse a connection from the pool", fun sequential_checkouts/0},
            {"give_away transfers ownership to another process", fun give_away_transfers/0},
            {"start_pool is idempotent on a running pool", fun start_pool_idempotent/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    case kura_pool_pgo:start_pool(?POOL, ?CONFIG) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end,
    ok = wait_for_ready(?POOL, 5000).

teardown(_) ->
    _ = kura_pool_pgo:stop_pool(?POOL),
    ok.

behaviour_declared() ->
    Attrs = kura_pool_pgo:module_info(attributes),
    Behaviours = lists:append([V || {behaviour, V} <- Attrs] ++ [V || {behavior, V} <- Attrs]),
    ?assert(lists:member(kura_pool, Behaviours)).

checkout_returns_conn() ->
    {ok, Conn, Token} = kura_pool_pgo:checkout(?POOL, #{}),
    try
        Result = pgo_handler:extended_query(
            Conn, ~"SELECT 1::integer AS one", [], [], #{queue_time => undefined}
        ),
        ?assertMatch(#{rows := [_]}, Result)
    after
        ok = kura_pool_pgo:checkin(?POOL, Token)
    end.

with_conn_roundtrip() ->
    Result = kura_pool:with_conn(kura_pool_pgo, ?POOL, fun(Conn) ->
        pgo_handler:extended_query(
            Conn, ~"SELECT 42::integer AS answer", [], [], #{queue_time => undefined}
        )
    end),
    ?assertMatch(#{rows := [_]}, Result),
    #{rows := [Row]} = Result,
    ?assertEqual([42], tuple_to_list(Row)).

sequential_checkouts() ->
    %% Two back-to-back checkouts must both succeed (proves checkin returned the
    %% conn to the pool, not that we just got two distinct workers).
    F = fun() ->
        {ok, Conn, Token} = kura_pool_pgo:checkout(?POOL, #{}),
        try
            #{rows := [_]} = pgo_handler:extended_query(
                Conn, ~"SELECT 1", [], [], #{queue_time => undefined}
            ),
            ok
        after
            ok = kura_pool_pgo:checkin(?POOL, Token)
        end
    end,
    ?assertEqual(ok, F()),
    ?assertEqual(ok, F()),
    ?assertEqual(ok, F()).

give_away_transfers() ->
    %% Setup process checks out a conn, gives it away to a child process
    %% that does the work and checks it back in. Mirrors the sandbox pattern.
    {ok, Conn, Token} = kura_pool_pgo:checkout(?POOL, #{}),
    Self = self(),
    Child = spawn_link(fun() ->
        receive
            {gift, T, C} ->
                Result = pgo_handler:extended_query(
                    C, ~"SELECT 7::integer", [], [], #{queue_time => undefined}
                ),
                ok = kura_pool_pgo:checkin(?POOL, T),
                Self ! {done, Result}
        after 5000 ->
            Self ! {done, timeout}
        end
    end),
    ok = kura_pool_pgo:give_away(Token, Child, none),
    %% Hand the token + conn to the child for the actual checkin call;
    %% give_away/3 transferred ETS ownership but the {Ref, Conn} term
    %% needs to travel via the message too.
    Child ! {gift, Token, Conn},
    receive
        {done, Result} ->
            ?assertMatch(#{rows := [_]}, Result)
    after 5000 ->
        ?assert(false)
    end.

start_pool_idempotent() ->
    %% Already started in setup. Re-calling must return ok with the existing pid.
    ?assertMatch({ok, _Pid}, kura_pool_pgo:start_pool(?POOL, ?CONFIG)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

wait_for_ready(Pool, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_ready_loop(Pool, Deadline).

wait_for_ready_loop(Pool, Deadline) ->
    case kura_pool_pgo:checkout(Pool, #{}) of
        {ok, _Conn, Token} ->
            ok = kura_pool_pgo:checkin(Pool, Token),
            ok;
        {error, _} ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    {error, pool_not_ready};
                false ->
                    timer:sleep(50),
                    wait_for_ready_loop(Pool, Deadline)
            end
    end.
