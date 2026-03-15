-module(kura_app_tests).
-include_lib("eunit/include/eunit.hrl").

stop_returns_ok_test() ->
    ?assertEqual(ok, kura_app:stop(undefined)).

stop_with_any_state_test() ->
    ?assertEqual(ok, kura_app:stop(some_state)).
