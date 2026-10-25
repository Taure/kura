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
            {"a raise in a sandboxed transaction undoes only its work",
                fun t_raised_transaction_undoes_its_work/0},
            {"a failed statement in a sandboxed transaction leaves the sandbox usable",
                fun t_failed_statement_keeps_sandbox/0},
            {"a raise two transactions down undoes only the innermost work",
                fun t_doubly_nested_raise/0},
            {"a savepoint refused in an aborted sandbox raises a reduced reason",
                fun t_refused_savepoint_is_reduced/0},
            {"a transaction left open by a dead process does not block the next",
                fun t_dead_holder_does_not_block/0},
            {"overlapping transactions on a shared sandbox raise",
                fun t_overlapping_shared_transactions_raise/0},
            {"overlapping transactions on an allowed sandbox raise",
                fun t_overlapping_allowed_transactions_raise/0},
            {"allow lets child process use sandbox", fun t_allow/0},
            {"shared mode lets any process use sandbox", fun t_shared_mode/0},
            {"multiple checkouts for different repos", fun t_multiple_repos/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(minato),
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
        %% No explicit allow needed - shared mode
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

t_raised_transaction_undoes_its_work() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo),
    try
        {ok, _} = insert_user(<<"Kept">>, <<"kept@test.com">>),
        ?assertError(
            undone,
            kura_repo_worker:transaction(kura_test_repo, fun() ->
                {ok, _} = insert_user(<<"Undone">>, <<"undone@test.com">>),
                error(undone)
            end)
        ),
        ?assertEqual(Before + 1, count_users())
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end,
    ?assertEqual(Before, count_users()).

t_failed_statement_keeps_sandbox() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo),
    try
        {ok, _} = insert_user(<<"First">>, <<"first@test.com">>),
        ?assertError(
            transaction_rolled_back,
            kura_repo_worker:transaction(kura_test_repo, fun() ->
                {ok, _} = insert_user(<<"Second">>, <<"second@test.com">>),
                {error, _} = kura_repo_worker:query(
                    kura_test_repo,
                    ~"INSERT INTO users (name, email) VALUES ('Dup', 'first@test.com')",
                    []
                ),
                ok
            end)
        ),
        ?assertEqual(Before + 1, count_users()),
        {ok, _} = insert_user(<<"Third">>, <<"third@test.com">>),
        ?assertEqual(Before + 2, count_users())
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end,
    ?assertEqual(Before, count_users()).

t_doubly_nested_raise() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo),
    try
        Result = kura_repo_worker:transaction(kura_test_repo, fun() ->
            {ok, _} = insert_user(<<"Outer">>, <<"outer@test.com">>),
            try
                kura_repo_worker:transaction(kura_test_repo, fun() ->
                    {ok, _} = insert_user(<<"Inner">>, <<"inner@test.com">>),
                    error(inner)
                end)
            catch
                error:inner -> caught
            end
        end),
        ?assertEqual(caught, Result),
        ?assertEqual(Before + 1, count_users())
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end,
    ?assertEqual(Before, count_users()).

t_overlapping_shared_transactions_raise() ->
    Before = count_users(),
    kura_sandbox:checkout(kura_test_repo, #{shared => true}),
    try
        Self = self(),
        Result = kura_repo_worker:transaction(kura_test_repo, fun() ->
            {ok, _} = insert_user(<<"Holder">>, <<"holder@test.com">>),
            {Child, Ref} = spawn_monitor(fun() ->
                Self ! {overlapped, transaction_outcome()}
            end),
            Outcome =
                receive
                    {overlapped, O} -> O
                after 5000 -> error(timeout)
                end,
            receive
                {'DOWN', Ref, process, Child, _} -> ok
            end,
            Outcome
        end),
        ?assertEqual({error, {kura_sandbox, overlapping_transactions}}, Result),
        ?assertEqual(Before + 1, count_users()),
        Sequential = spawn_link(fun() -> Self ! {sequential, transaction_outcome()} end),
        receive
            {sequential, Outcome} ->
                ?assertEqual({ok, inserted}, Outcome),
                ?assert(is_pid(Sequential))
        after 5000 -> error(timeout)
        end,
        ?assertEqual(Before + 2, count_users())
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end,
    ?assertEqual(Before, count_users()).

t_overlapping_allowed_transactions_raise() ->
    kura_sandbox:checkout(kura_test_repo),
    try
        Self = self(),
        Child = spawn_link(fun() ->
            receive
                go -> Self ! {overlapped, transaction_outcome()}
            end
        end),
        kura_sandbox:allow(kura_test_repo, self(), Child),
        Result = kura_repo_worker:transaction(kura_test_repo, fun() ->
            Child ! go,
            receive
                {overlapped, O} -> O
            after 5000 -> error(timeout)
            end
        end),
        ?assertEqual({error, {kura_sandbox, overlapping_transactions}}, Result)
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end.

t_refused_savepoint_is_reduced() ->
    kura_sandbox:checkout(kura_test_repo),
    try
        {error, _} = kura_repo_worker:query(
            kura_test_repo, ~"SELECT no_such_column FROM users", []
        ),
        try kura_repo_worker:transaction(kura_test_repo, fun() -> ran end) of
            Ran -> error({expected_a_raise, Ran})
        catch
            error:{kura_driver_minato, {savepoint_failed, {pgsql_error, Fields}}} ->
                ?assertEqual(~"25P02", maps:get(code, Fields)),
                ?assertEqual(
                    [], maps:keys(Fields) -- [code, constraint, severity, severity_non_localised]
                )
        end
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end.

t_dead_holder_does_not_block() ->
    kura_sandbox:checkout(kura_test_repo, #{shared => true}),
    try
        {ok, Conn} = kura_sandbox:get_conn(kura_test_repo),
        {Holder, Ref} = spawn_monitor(fun() -> ok = kura_sandbox:enter_transaction(Conn) end),
        receive
            {'DOWN', Ref, process, Holder, normal} -> ok
        end,
        ?assertEqual({ok, inserted}, transaction_outcome())
    after
        ok = kura_sandbox:checkin(kura_test_repo)
    end.

transaction_outcome() ->
    try
        kura_repo_worker:transaction(kura_test_repo, fun() ->
            {ok, _} = insert_user(<<"Second holder">>, <<"second_holder@test.com">>),
            {ok, inserted}
        end)
    catch
        error:Reason -> {error, Reason}
    end.
