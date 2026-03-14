-module(kura_sandbox_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

sandbox_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"insert is rolled back on checkin", fun t_rollback/0},
            {"data visible within sandbox", fun t_visible_within/0},
            {"nested transaction works in sandbox", fun t_nested_transaction/0},
            {"allow lets child process use sandbox", fun t_allow/0},
            {"shared mode lets any process use sandbox", fun t_shared_mode/0},
            {"multiple checkouts for different repos", fun t_multiple_repos/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    kura_sandbox:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => Name, email => Email}, [name, email]),
    kura_repo_worker:insert(kura_test_repo, CS).

count_users() ->
    Q = kura_query:count(kura_query:from(kura_test_schema)),
    {ok, [#{count := N}]} = kura_repo_worker:all(kura_test_repo, Q),
    N.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_rollback() ->
    %% Count before
    Before = count_users(),

    %% Checkout, insert, checkin
    kura_sandbox:checkout(kura_test_repo),
    {ok, _} = insert_user(<<"Sandbox User">>, <<"sandbox@test.com">>),
    %% Visible inside sandbox
    ?assertEqual(Before + 1, count_users()),
    kura_sandbox:checkin(kura_test_repo),

    %% After checkin, the insert was rolled back
    ?assertEqual(Before, count_users()).

t_visible_within() ->
    kura_sandbox:checkout(kura_test_repo),
    {ok, User} = insert_user(<<"Visible">>, <<"visible@test.com">>),
    Id = maps:get(id, User),
    {ok, Found} = kura_repo_worker:get(kura_test_repo, kura_test_schema, Id),
    ?assertEqual(<<"Visible">>, maps:get(name, Found)),
    kura_sandbox:checkin(kura_test_repo).

t_nested_transaction() ->
    kura_sandbox:checkout(kura_test_repo),
    Before = count_users(),
    %% Transaction inside sandbox should work (becomes a no-op wrapper)
    Result = kura_repo_worker:transaction(kura_test_repo, fun() ->
        {ok, U} = insert_user(<<"TxUser">>, <<"tx@test.com">>),
        {ok, U}
    end),
    ?assertMatch({ok, _}, Result),
    ?assertEqual(Before + 1, count_users()),
    kura_sandbox:checkin(kura_test_repo),
    %% Still rolled back
    ?assertEqual(Before, count_users()).

t_allow() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo),

    Self = self(),
    Child = spawn_link(fun() ->
        receive
            go -> ok
        end,
        %% This process was allowed, so it uses the sandbox connection
        {ok, _} = insert_user(<<"Child">>, <<"child@test.com">>),
        N = count_users(),
        Self ! {done, N}
    end),
    kura_sandbox:allow(kura_test_repo, self(), Child),
    Child ! go,
    ChildCount =
        receive
            {done, N} -> N
        after 5000 -> error(timeout)
        end,

    %% Child's insert is visible (same sandbox transaction)
    ?assertEqual(Before + 1, ChildCount),

    kura_sandbox:checkin(kura_test_repo),
    %% Rolled back
    ?assertEqual(Before, count_users()).

t_shared_mode() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo, #{shared => true}),

    Self = self(),
    Child = spawn_link(fun() ->
        receive
            go -> ok
        end,
        %% No explicit allow needed — shared mode
        {ok, _} = insert_user(<<"Shared">>, <<"shared@test.com">>),
        N = count_users(),
        Self ! {done, N}
    end),
    Child ! go,
    ChildCount =
        receive
            {done, N} -> N
        after 5000 -> error(timeout)
        end,
    ?assertEqual(Before + 1, ChildCount),

    kura_sandbox:checkin(kura_test_repo),
    ?assertEqual(Before, count_users()).

t_multiple_repos() ->
    %% Verifies checkout/checkin for the same repo is idempotent
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo),
    {ok, _} = insert_user(<<"Multi1">>, <<"multi1@test.com">>),
    kura_sandbox:checkin(kura_test_repo),
    ?assertEqual(Before, count_users()),

    %% Second checkout works fine
    kura_sandbox:checkout(kura_test_repo),
    {ok, _} = insert_user(<<"Multi2">>, <<"multi2@test.com">>),
    ?assertEqual(Before + 1, count_users()),
    kura_sandbox:checkin(kura_test_repo),
    ?assertEqual(Before, count_users()).
