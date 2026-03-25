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

%%----------------------------------------------------------------------
%% Telemetry metadata tests
%%----------------------------------------------------------------------

build_telemetry_metadata_select_test() ->
    Result = #{command => select, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT * FROM \"users\" WHERE id = $1">>, [1], Result
    ),
    ?assertEqual(<<"SELECT * FROM \"users\" WHERE id = $1">>, maps:get(query, Meta)),
    ?assertEqual([1], maps:get(params, Meta)),
    ?assertEqual(my_repo, maps:get(repo, Meta)),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(1, maps:get(num_rows, Meta)),
    ?assertEqual(<<"users">>, maps:get(source, Meta)),
    ?assertEqual(undefined, maps:get(tenant, Meta)),
    ?assertEqual(undefined, maps:get(error_reason, Meta)).

build_telemetry_metadata_insert_test() ->
    Result = #{command => insert, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"INSERT INTO \"posts\" (title) VALUES ($1)">>, [<<"Hello">>], Result
    ),
    ?assertEqual(<<"posts">>, maps:get(source, Meta)),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(undefined, maps:get(error_reason, Meta)).

build_telemetry_metadata_update_test() ->
    Result = #{command => update, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"UPDATE \"users\" SET name = $1">>, [<<"Bob">>], Result
    ),
    ?assertEqual(<<"users">>, maps:get(source, Meta)).

build_telemetry_metadata_error_test() ->
    Result = {error, #{code => <<"23505">>}},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"INSERT INTO \"users\"">>, [], Result
    ),
    ?assertEqual(error, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)),
    ?assertEqual(#{code => <<"23505">>}, maps:get(error_reason, Meta)).

build_telemetry_metadata_no_source_test() ->
    Result = #{command => select, rows => []},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT 1">>, [], Result
    ),
    ?assertEqual(undefined, maps:get(source, Meta)).

extract_source_test() ->
    ?assertEqual(<<"users">>, kura_repo_worker:extract_source(<<"SELECT * FROM \"users\"">>)),
    ?assertEqual(<<"posts">>, kura_repo_worker:extract_source(<<"INSERT INTO \"posts\"">>)),
    ?assertEqual(<<"users">>, kura_repo_worker:extract_source(<<"UPDATE \"users\" SET">>)),
    ?assertEqual(<<"t">>, kura_repo_worker:extract_source(<<"CREATE TABLE \"t\"">>)),
    ?assertEqual(undefined, kura_repo_worker:extract_source(<<"SELECT 1">>)).
