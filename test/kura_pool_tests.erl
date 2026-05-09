-module(kura_pool_tests).
-include_lib("eunit/include/eunit.hrl").

%% A trivial fake pool that lets us test kura_pool:with_conn/3,4 without PG.

-export([checkout/2, checkin/2]).

with_conn_returns_fun_result_test() ->
    register(fake, self()),
    try
        Result = kura_pool:with_conn(?MODULE, fake, fun(Conn) -> {got, Conn} end),
        ?assertEqual({got, fake_conn}, Result),
        assert_checkin(fake, fake_token)
    after
        unregister(fake)
    end.

with_conn_calls_checkin_on_throw_test() ->
    register(fake, self()),
    try
        ?assertException(
            throw,
            boom,
            kura_pool:with_conn(?MODULE, fake, fun(_) -> throw(boom) end)
        ),
        assert_checkin(fake, fake_token)
    after
        unregister(fake)
    end.

with_conn_propagates_checkout_error_test() ->
    register(fake_err, self()),
    try
        ?assertEqual(
            {error, no_conns},
            kura_pool:with_conn(?MODULE, fake_err, fun(_) -> ok end)
        )
    after
        unregister(fake_err)
    end.

%%----------------------------------------------------------------------
%% Fake kura_pool implementation used by the tests above
%%----------------------------------------------------------------------

%% kura_pool callbacks (only the ones with_conn touches)

checkout(fake, _Opts) ->
    {ok, fake_conn, fake_token};
checkout(fake_err, _Opts) ->
    {error, no_conns}.

checkin(Name, Token) ->
    Name ! {checkin, Name, Token},
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

assert_checkin(Name, Token) ->
    receive
        {checkin, Name, Token} -> ok
    after 1000 ->
        ?assert(false)
    end.
