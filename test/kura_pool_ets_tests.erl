-module(kura_pool_ets_tests).
-include_lib("eunit/include/eunit.hrl").

%% give_away_rejects_non_pid_test deliberately calls with an atom to
%% exercise the error path; the kura_pool spec types NewOwner as pid().
-eqwalizer({nowarn_function, give_away_rejects_non_pid_test/0}).

%%----------------------------------------------------------------------
%% start_pool / stop_pool
%%----------------------------------------------------------------------

start_pool_creates_ets_table_test() ->
    Name = pool_for_create_test,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 2}),
    try
        ?assertNotEqual(undefined, ets:whereis(Name)),
        ?assertMatch([{available, [_, _]}], ets:lookup(Name, available)),
        ?assertMatch([{checked_out, M}] when map_size(M) =:= 0, ets:lookup(Name, checked_out))
    after
        kura_pool_ets:stop_pool(Name)
    end.

start_pool_default_size_is_one_test() ->
    Name = pool_for_default_size,
    {ok, _} = kura_pool_ets:start_pool(Name, #{}),
    try
        ?assertMatch([{available, [_]}], ets:lookup(Name, available))
    after
        kura_pool_ets:stop_pool(Name)
    end.

start_pool_already_started_returns_error_test() ->
    Name = pool_for_dup_test,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        ?assertMatch({error, {already_started, _}}, kura_pool_ets:start_pool(Name, #{}))
    after
        kura_pool_ets:stop_pool(Name)
    end.

stop_pool_idempotent_test() ->
    Name = pool_for_stop_test,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    ?assertEqual(ok, kura_pool_ets:stop_pool(Name)),
    ?assertEqual(ok, kura_pool_ets:stop_pool(Name)).

%%----------------------------------------------------------------------
%% checkout / checkin
%%----------------------------------------------------------------------

checkout_returns_a_conn_and_token_test() ->
    Name = pool_for_checkout,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        {ok, Conn, Token} = kura_pool_ets:checkout(Name, #{}),
        ?assert(is_reference(Conn)),
        ?assertEqual(Conn, Token)
    after
        kura_pool_ets:stop_pool(Name)
    end.

checkout_drains_pool_then_returns_no_conns_test() ->
    Name = pool_for_drain,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 2}),
    try
        {ok, _, _} = kura_pool_ets:checkout(Name, #{}),
        {ok, _, _} = kura_pool_ets:checkout(Name, #{}),
        ?assertEqual({error, no_conns}, kura_pool_ets:checkout(Name, #{}))
    after
        kura_pool_ets:stop_pool(Name)
    end.

checkout_on_missing_pool_returns_no_pool_test() ->
    ?assertEqual({error, no_pool}, kura_pool_ets:checkout(pool_does_not_exist, #{})).

checkin_makes_conn_available_again_test() ->
    Name = pool_for_checkin,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        {ok, _Conn, Token} = kura_pool_ets:checkout(Name, #{}),
        ?assertEqual({error, no_conns}, kura_pool_ets:checkout(Name, #{})),
        ok = kura_pool_ets:checkin(Name, Token),
        {ok, _, _} = kura_pool_ets:checkout(Name, #{})
    after
        kura_pool_ets:stop_pool(Name)
    end.

checkin_double_is_idempotent_test() ->
    Name = pool_for_double_checkin,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        {ok, _, Token} = kura_pool_ets:checkout(Name, #{}),
        ok = kura_pool_ets:checkin(Name, Token),
        ?assertEqual(ok, kura_pool_ets:checkin(Name, Token))
    after
        kura_pool_ets:stop_pool(Name)
    end.

checkin_on_missing_pool_is_ok_test() ->
    %% Mirrors kura_pool_pgo's permissive checkin so with_conn-style
    %% finalizers do not crash when a pool was already torn down.
    ?assertEqual(ok, kura_pool_ets:checkin(pool_gone, erlang:make_ref())).

%%----------------------------------------------------------------------
%% give_away
%%----------------------------------------------------------------------

give_away_accepts_pid_test() ->
    ?assertEqual(ok, kura_pool_ets:give_away(erlang:make_ref(), self(), gift)).

give_away_rejects_non_pid_test() ->
    ?assertEqual({error, badarg}, kura_pool_ets:give_away(erlang:make_ref(), not_a_pid, gift)).

%%----------------------------------------------------------------------
%% resize
%%----------------------------------------------------------------------

resize_grows_the_pool_test() ->
    Name = pool_for_grow,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 2}),
    try
        ok = kura_pool_ets:resize(Name, 5),
        [{available, Avail}] = ets:lookup(Name, available),
        ?assertEqual(5, length(Avail))
    after
        kura_pool_ets:stop_pool(Name)
    end.

resize_shrinks_the_pool_test() ->
    Name = pool_for_shrink,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 5}),
    try
        ok = kura_pool_ets:resize(Name, 2),
        [{available, Avail}] = ets:lookup(Name, available),
        ?assertEqual(2, length(Avail))
    after
        kura_pool_ets:stop_pool(Name)
    end.

resize_on_missing_pool_returns_no_pool_test() ->
    ?assertEqual({error, no_pool}, kura_pool_ets:resize(pool_does_not_exist, 3)).

resize_accounts_for_checked_out_conns_test() ->
    %% With 2 checked-out + 1 available (current = 3), resizing to 5
    %% should add 2 fresh refs to keep the total at 5. If accounting
    %% used subtraction instead of addition, total would drift.
    Name = pool_for_resize_with_outs,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 3}),
    try
        {ok, _, _} = kura_pool_ets:checkout(Name, #{}),
        {ok, _, _} = kura_pool_ets:checkout(Name, #{}),
        ok = kura_pool_ets:resize(Name, 5),
        [{available, Avail}] = ets:lookup(Name, available),
        [{checked_out, Out}] = ets:lookup(Name, checked_out),
        ?assertEqual(5, length(Avail) + map_size(Out))
    after
        kura_pool_ets:stop_pool(Name)
    end.

%%----------------------------------------------------------------------
%% with_conn integration via kura_pool
%%----------------------------------------------------------------------

with_conn_runs_fun_with_a_real_ets_pool_test() ->
    Name = pool_for_with_conn,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        Result = kura_pool:with_conn(kura_pool_ets, Name, fun(Conn) -> {used, Conn} end),
        ?assertMatch({used, _Ref}, Result),
        %% Conn should be checked back in afterward.
        {ok, _, _} = kura_pool_ets:checkout(Name, #{})
    after
        kura_pool_ets:stop_pool(Name)
    end.

with_conn_checks_in_on_throw_test() ->
    Name = pool_for_with_conn_throw,
    {ok, _} = kura_pool_ets:start_pool(Name, #{pool_size => 1}),
    try
        ?assertException(
            throw,
            boom,
            kura_pool:with_conn(kura_pool_ets, Name, fun(_) -> throw(boom) end)
        ),
        %% Pool should still be drainable after the throw.
        {ok, _, _} = kura_pool_ets:checkout(Name, #{})
    after
        kura_pool_ets:stop_pool(Name)
    end.

%%----------------------------------------------------------------------
%% kura_capabilities integration
%%----------------------------------------------------------------------

declares_kura_capabilities_behaviour_test() ->
    Attrs = kura_pool_ets:module_info(attributes),
    Behaviours = lists:append([V || {behaviour, V} <- Attrs] ++ [V || {behavior, V} <- Attrs]),
    ?assert(lists:member(kura_capabilities, Behaviours)).

capabilities_is_empty_test() ->
    %% This pool runs no SQL and supports no DB features.
    ?assertEqual([], kura_capabilities:supported(kura_pool_ets)).

require_any_capability_returns_missing_test() ->
    %% Consumers that need any feature should refuse to start on this pool.
    ?assertEqual(
        {error, {missing_capabilities, [returning]}},
        kura_capabilities:require(kura_pool_ets, [returning])
    ).
