-module(kura_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

build_log_event_select_test() ->
    Result = #{command => select, rows => [#{id => 1}, #{id => 2}]},
    Event = kura_repo_worker:build_log_event(my_repo, <<"SELECT * FROM users">>, [], Result, 1500),
    ?assertEqual(<<"SELECT * FROM users">>, maps:get(query, Event)),
    ?assertEqual([], maps:get(params, Event)),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(2, maps:get(num_rows, Event)),
    ?assertEqual(1500, maps:get(duration_us, Event)),
    ?assertEqual(my_repo, maps:get(repo, Event)).

build_log_event_insert_test() ->
    Result = #{command => insert, num_rows => 1, rows => [#{id => 1}]},
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"INSERT INTO users">>, [<<"Alice">>], Result, 500
    ),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(1, maps:get(num_rows, Event)),
    ?assertEqual([<<"Alice">>], maps:get(params, Event)).

build_log_event_error_test() ->
    Result = {error, #{code => <<"23505">>}},
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"INSERT INTO users">>, [<<"Alice">>], Result, 200
    ),
    ?assertEqual(error, maps:get(result, Event)),
    ?assertEqual(0, maps:get(num_rows, Event)).

build_log_event_keys_test() ->
    Result = #{command => select, rows => []},
    Event = kura_repo_worker:build_log_event(my_repo, <<"SELECT 1">>, [], Result, 100),
    ExpectedKeys = lists:sort([query, params, result, num_rows, duration_us, repo]),
    ?assertEqual(ExpectedKeys, lists:sort(maps:keys(Event))).

default_logger_test() ->
    LogFun = kura_repo_worker:default_logger(),
    ?assert(is_function(LogFun, 1)).
