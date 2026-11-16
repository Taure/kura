-module(kura_postgres_driver_tests).
-moduledoc """
kura's transaction, sandbox and pool-start paths through its public API,
against the released kura_postgres driver and PostgreSQL.

This module is also a repo whose pool is configured without a user.
""".

-include_lib("eunit/include/eunit.hrl").

-export([otp_app/0]).

-define(REPO, kura_test_repo).
-define(ENDED, {kura_driver_minato, transaction_ended_by_statement}).

otp_app() -> kura.

%%----------------------------------------------------------------------
%% Fixture
%%----------------------------------------------------------------------

driver_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"a raw COMMIT inside a transaction raises and keeps what it committed",
                fun t_commit_inside_transaction/0},
            {"every spelling that ends the transaction is caught", fun t_ending_statements/0},
            {"a function that catches the error cannot go on in the transaction",
                fun t_caught_ending_statement/0},
            {"a nested transaction that fails rolls back to its savepoint only",
                fun t_nested_rolls_back_to_its_savepoint/0},
            {"a refused savepoint rollback rolls the whole transaction back",
                fun t_refused_savepoint_rollback/0},
            {"a sandbox runs statements in a transaction it rolls back",
                fun t_sandbox_rolls_back/0},
            {"a transaction in a sandbox is a savepoint", fun t_sandbox_transaction/0},
            {"a COMMIT in a sandbox raises until it is checked in",
                fun t_sandbox_ending_statement/0}
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    ?REPO:start(),
    {ok, _} = ?REPO:query(~"DROP TABLE IF EXISTS driver_marks", []),
    {ok, _} = ?REPO:query(
        ~"CREATE TABLE driver_marks (id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL)", []
    ),
    ok.

teardown(_) ->
    {ok, _} = ?REPO:query(~"DROP TABLE IF EXISTS driver_marks", []),
    ok.

mark(Label) ->
    {ok, _} = ?REPO:query(~"INSERT INTO driver_marks (label) VALUES ($1)", [Label]),
    ok.

marks() ->
    {ok, Rows} = ?REPO:query(~"SELECT label FROM driver_marks ORDER BY id", []),
    [Label || #{label := Label} <- Rows].

clear() ->
    {ok, _} = ?REPO:query(~"DELETE FROM driver_marks", []),
    ok.

%%----------------------------------------------------------------------
%% A statement that ends the transaction
%%----------------------------------------------------------------------

t_commit_inside_transaction() ->
    clear(),
    ?assertError(
        ?ENDED,
        ?REPO:transaction(fun() ->
            ok = mark(~"before commit"),
            ?REPO:query(~"COMMIT", [])
        end)
    ),
    ?assertEqual([~"before commit"], marks()).

t_ending_statements() ->
    lists:foreach(
        fun(SQL) ->
            clear(),
            ?assertError(
                ?ENDED,
                ?REPO:transaction(fun() ->
                    ok = mark(SQL),
                    ?REPO:query(SQL, [])
                end),
                SQL
            ),
            ?assertEqual(ok, ?REPO:transaction(fun() -> mark(~"after") end), SQL)
        end,
        [
            ~"ROLLBACK",
            ~"END",
            ~"commit and chain",
            ~"ROLLBACK AND CHAIN",
            ~"ABORT AND CHAIN",
            ~"/* still a commit */ COMMIT"
        ]
    ).

t_caught_ending_statement() ->
    clear(),
    ?assertError(
        ?ENDED,
        ?REPO:transaction(fun() ->
            ?assertError(?ENDED, ?REPO:query(~"COMMIT", [])),
            ?assertError(?ENDED, mark(~"outside the transaction")),
            ?assertError(?ENDED, ?REPO:transaction(fun() -> mark(~"nested") end)),
            ok
        end)
    ),
    ?assertEqual([], marks()).

%%----------------------------------------------------------------------
%% Nested transactions
%%----------------------------------------------------------------------

t_nested_rolls_back_to_its_savepoint() ->
    clear(),
    ok = ?REPO:transaction(fun() ->
        ok = mark(~"outer"),
        ?assertError(
            boom,
            ?REPO:transaction(fun() ->
                ok = mark(~"inner"),
                error(boom)
            end)
        ),
        ok = ?REPO:transaction(fun() -> mark(~"second inner") end)
    end),
    ?assertEqual([~"outer", ~"second inner"], marks()).

t_refused_savepoint_rollback() ->
    clear(),
    ?assertError(
        {kura_driver_minato, {rollback_to_failed, {pgsql_error, #{code := ~"3B001"}}}},
        ?REPO:transaction(fun() ->
            ok = mark(~"outer"),
            {ok, _} = ?REPO:query(~"SAVEPOINT before_inner", []),
            ?REPO:transaction(fun() ->
                {ok, _} = ?REPO:query(~"ROLLBACK TO SAVEPOINT before_inner", []),
                error(boom)
            end)
        end)
    ),
    ?assertEqual([], marks()),
    ?assertEqual(ok, ?REPO:transaction(fun() -> mark(~"after") end)).

%%----------------------------------------------------------------------
%% Sandbox
%%----------------------------------------------------------------------

t_sandbox_rolls_back() ->
    clear(),
    ok = kura_sandbox:checkout(?REPO),
    try
        ok = mark(~"sandboxed"),
        ?assertEqual([~"sandboxed"], marks())
    after
        ok = kura_sandbox:checkin(?REPO)
    end,
    ?assertEqual([], marks()).

t_sandbox_transaction() ->
    clear(),
    ok = kura_sandbox:checkout(?REPO),
    try
        ok = ?REPO:transaction(fun() -> mark(~"kept") end),
        ?assertError(
            boom,
            ?REPO:transaction(fun() ->
                ok = mark(~"dropped"),
                error(boom)
            end)
        ),
        ?assertEqual([~"kept"], marks())
    after
        ok = kura_sandbox:checkin(?REPO)
    end,
    ?assertEqual([], marks()).

t_sandbox_ending_statement() ->
    clear(),
    ok = kura_sandbox:checkout(?REPO),
    try
        ok = mark(~"committed by the test"),
        ?assertError(?ENDED, ?REPO:query(~"COMMIT", [])),
        ?assertError(?ENDED, mark(~"after the commit")),
        ?assertError(?ENDED, ?REPO:transaction(fun() -> ok end))
    after
        ok = kura_sandbox:checkin(?REPO)
    end,
    ?assertEqual([~"committed by the test"], marks()),
    ok = kura_sandbox:checkout(?REPO),
    try
        ok = mark(~"next test"),
        ?assertEqual([~"committed by the test", ~"next test"], marks())
    after
        ok = kura_sandbox:checkin(?REPO)
    end,
    ?assertEqual([~"committed by the test"], marks()).

%%----------------------------------------------------------------------
%% Pool start
%%----------------------------------------------------------------------

pool_without_a_user_is_not_started_test() ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    application:set_env(kura, ?MODULE, #{
        pool => kura_postgres_driver_userless_pool,
        pool_module => kura_pool_minato,
        driver_module => kura_driver_minato,
        dialect => kura_dialect_pg,
        database => ~"kura_test",
        hostname => ~"localhost",
        port => 5555,
        password => ~"root"
    }),
    try
        ?assertEqual({error, {missing_option, user}}, kura_repo_worker:start(?MODULE)),
        ?assertEqual(undefined, whereis(kura_postgres_driver_userless_pool))
    after
        application:unset_env(kura, ?MODULE)
    end.
