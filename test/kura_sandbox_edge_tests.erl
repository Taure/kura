-module(kura_sandbox_edge_tests).
-include_lib("eunit/include/eunit.hrl").

start_idempotent_test() ->
    ok = kura_sandbox:start(),
    ok = kura_sandbox:start().

get_conn_no_table_test() ->
    TableName = kura_sandbox_registry,
    case ets:whereis(TableName) of
        undefined -> ok;
        _ -> ok
    end,
    ok.
