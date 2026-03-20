-module(kura_stream_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

narrow_transaction_result_ok_test() ->
    ?assertEqual(ok, kura_stream:narrow_transaction_result(ok)).

narrow_transaction_result_error_test() ->
    ?assertEqual({error, timeout}, kura_stream:narrow_transaction_result({error, timeout})).

narrow_transaction_result_error_tuple_test() ->
    ?assertEqual(
        {error, {pgsql_error, <<"oops">>}},
        kura_stream:narrow_transaction_result({error, {pgsql_error, <<"oops">>}})
    ).
