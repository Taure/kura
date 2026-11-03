-module(kura_sandbox_ended_tests).
-moduledoc """
A statement that ends the sandbox's transaction, with a driver that reports it.

This module is the repo (`otp_app/0`, `init/1`) and its driver (`query/5`,
`query_on/4`, `transaction/4`). The driver logs every statement and raises
`{?MODULE, transaction_ended_by_statement}` for a `COMMIT` marked
`transaction => inside`, as a driver that checks does.
""".
-include_lib("eunit/include/eunit.hrl").

-export([otp_app/0, init/1]).
-export([query/5, query_on/4, transaction/4]).

-define(POOL, kura_sandbox_ended_pool).
-define(LOG, kura_sandbox_ended_log).
-define(ENDED, {?MODULE, transaction_ended_by_statement}).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

ended_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"the sandbox begins with transaction => starts and runs statements inside",
            fun statements_are_marked/0},
        {"a COMMIT raises, and the next statement raises without running",
            fun commit_then_statement/0},
        {"another process on a shared connection gets the same label",
            fun shared_process_after_commit/0},
        {"an allowed process gets the same label", fun allowed_process_after_commit/0},
        {"a transaction whose function caught the COMMIT raises ended, not rolled back",
            fun transaction_caught_commit/0},
        {"a transaction whose function raised ended re-raises it unchanged",
            fun transaction_raised_ended/0},
        {"a transaction opened after the COMMIT raises before running its function",
            fun transaction_after_commit/0},
        {"checkin clears it for the next checkout", fun checkin_clears/0},
        {"other driver errors still read as the savepoint failing", fun other_failures_unchanged/0}
    ]}.

statements_are_marked() ->
    ok = kura_sandbox:checkout(?MODULE),
    _ = kura_db:query(?MODULE, ~"SELECT 1", []),
    ?assertMatch(
        [{~"BEGIN", #{transaction := starts}}, {~"SELECT 1", #{transaction := inside}}],
        logged()
    ).

commit_then_statement() ->
    ok = kura_sandbox:checkout(?MODULE),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"SELECT 1", [])),
    ?assertEqual([~"BEGIN", ~"COMMIT"], logged_sql()).

shared_process_after_commit() ->
    ok = kura_sandbox:checkout(?MODULE, #{shared => true}),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
    ?assertEqual({error, ?ENDED}, elsewhere(fun() -> kura_db:query(?MODULE, ~"SELECT 1", []) end)),
    ?assertEqual([~"BEGIN", ~"COMMIT"], logged_sql()).

allowed_process_after_commit() ->
    ok = kura_sandbox:checkout(?MODULE),
    Self = self(),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
    Answer = elsewhere(
        fun() ->
            receive
                go -> kura_db:query(?MODULE, ~"SELECT 1", [])
            end
        end,
        fun(Pid) -> kura_sandbox:allow(?MODULE, Self, Pid) end
    ),
    ?assertEqual({error, ?ENDED}, Answer),
    ?assertEqual([~"BEGIN", ~"COMMIT"], logged_sql()).

transaction_caught_commit() ->
    ok = kura_sandbox:checkout(?MODULE),
    ?assertError(
        ?ENDED,
        kura_db:transaction(?MODULE, fun() ->
            ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
            done
        end)
    ),
    ?assertMatch([~"BEGIN", <<"SAVEPOINT ", _/binary>>, ~"COMMIT"], logged_sql()).

transaction_raised_ended() ->
    ok = kura_sandbox:checkout(?MODULE),
    ?assertError(
        ?ENDED,
        kura_db:transaction(?MODULE, fun() -> kura_db:query(?MODULE, ~"COMMIT", []) end)
    ),
    ?assertMatch([~"BEGIN", <<"SAVEPOINT ", _/binary>>, ~"COMMIT"], logged_sql()).

transaction_after_commit() ->
    ok = kura_sandbox:checkout(?MODULE),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
    ?assertError(?ENDED, kura_db:transaction(?MODULE, fun() -> error(ran) end)),
    ?assertEqual([~"BEGIN", ~"COMMIT"], logged_sql()).

checkin_clears() ->
    ok = kura_sandbox:checkout(?MODULE),
    ?assertError(?ENDED, kura_db:query(?MODULE, ~"COMMIT", [])),
    ok = kura_sandbox:checkin(?MODULE),
    ok = kura_sandbox:checkout(?MODULE),
    ?assertMatch(#{}, kura_db:query(?MODULE, ~"SELECT 1", [])),
    ?assertEqual(
        [~"BEGIN", ~"COMMIT", ~"ROLLBACK", ~"BEGIN", ~"SELECT 1"], logged_sql()
    ).

other_failures_unchanged() ->
    ok = kura_sandbox:checkout(?MODULE),
    persistent_term:put({?MODULE, raise_on_savepoint}, true),
    try
        ?assertError(
            {?MODULE, {savepoint_failed, {raised, error}}},
            kura_db:transaction(?MODULE, fun() -> error(ran) end)
        ),
        ?assertMatch(#{}, kura_db:query(?MODULE, ~"SELECT 1", []))
    after
        persistent_term:erase({?MODULE, raise_on_savepoint})
    end.

%%----------------------------------------------------------------------
%% Fixture
%%----------------------------------------------------------------------

setup() ->
    persistent_term:put({?MODULE, config}, #{
        pool => ?POOL, pool_module => kura_pool_ets, driver_module => ?MODULE
    }),
    ok = kura_sandbox:start(),
    {ok, _} = kura_pool_ets:start_pool(?POOL, #{pool_size => 1}),
    _ = ets:new(?LOG, [named_table, public, ordered_set]),
    ok.

teardown(_) ->
    ok = kura_sandbox:checkin(?MODULE),
    ok = kura_pool_ets:stop_pool(?POOL),
    true = ets:delete(?LOG),
    persistent_term:erase({?MODULE, config}),
    ok.

logged() ->
    [{SQL, Opts} || {_Seq, SQL, Opts} <- ets:tab2list(?LOG)].

logged_sql() ->
    [SQL || {SQL, _Opts} <- logged()].

elsewhere(Fun) ->
    elsewhere(Fun, fun(_Pid) -> ok end).

elsewhere(Fun, Before) ->
    Self = self(),
    {Pid, Monitor} = spawn_monitor(fun() ->
        Self !
            {
                self(),
                try
                    {ok, Fun()}
                catch
                    error:Reason -> {error, Reason}
                end
            }
    end),
    ok = Before(Pid),
    Pid ! go,
    receive
        {Pid, Answer} ->
            erlang:demonitor(Monitor, [flush]),
            Answer;
        {'DOWN', Monitor, process, Pid, Reason} ->
            error({died, Reason})
    after 5000 ->
        error(timeout)
    end.

%%----------------------------------------------------------------------
%% Repo
%%----------------------------------------------------------------------

otp_app() -> kura.

init(_Config) ->
    persistent_term:get({?MODULE, config}).

%%----------------------------------------------------------------------
%% Driver
%%----------------------------------------------------------------------

query(_PoolMod, _Pool, _SQL, _Params, _Opts) ->
    error(not_sandboxed).

query_on(_Conn, SQL, _Params, Opts) ->
    Statement = iolist_to_binary(SQL),
    true = ets:insert(?LOG, {erlang:unique_integer([monotonic]), Statement, Opts}),
    answer(Statement, maps:get(transaction, Opts, none)).

transaction(_PoolMod, _Pool, _Fun, _Opts) ->
    error(not_sandboxed).

answer(~"COMMIT", inside) ->
    error(?ENDED);
answer(<<"SAVEPOINT ", _/binary>>, inside) ->
    case persistent_term:get({?MODULE, raise_on_savepoint}, false) of
        true -> error(refused);
        false -> #{command => savepoint, num_rows => 0, rows => []}
    end;
answer(_Statement, _Transaction) ->
    #{command => ok, num_rows => 0, rows => []}.
