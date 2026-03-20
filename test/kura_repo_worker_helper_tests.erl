-module(kura_repo_worker_helper_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% narrow_ok_error
%%----------------------------------------------------------------------

narrow_ok_error_ok_test() ->
    ?assertEqual({ok, 42}, kura_repo_worker:narrow_ok_error({ok, 42})).

narrow_ok_error_error_test() ->
    ?assertEqual({error, reason}, kura_repo_worker:narrow_ok_error({error, reason})).

narrow_ok_error_ok_complex_test() ->
    ?assertEqual({ok, #{a => 1}}, kura_repo_worker:narrow_ok_error({ok, #{a => 1}})).

%%----------------------------------------------------------------------
%% extract_result_status
%%----------------------------------------------------------------------

extract_result_status_error_test() ->
    ?assertEqual(error, kura_repo_worker:extract_result_status({error, oops})).

extract_result_status_ok_map_test() ->
    ?assertEqual(ok, kura_repo_worker:extract_result_status(#{rows => []})).

extract_result_status_ok_atom_test() ->
    ?assertEqual(ok, kura_repo_worker:extract_result_status(ok)).

extract_result_status_ok_other_test() ->
    ?assertEqual(ok, kura_repo_worker:extract_result_status(42)).

%%----------------------------------------------------------------------
%% extract_num_rows
%%----------------------------------------------------------------------

extract_num_rows_from_num_rows_test() ->
    ?assertEqual(5, kura_repo_worker:extract_num_rows(#{num_rows => 5})).

extract_num_rows_from_rows_test() ->
    ?assertEqual(3, kura_repo_worker:extract_num_rows(#{rows => [a, b, c]})).

extract_num_rows_default_test() ->
    ?assertEqual(0, kura_repo_worker:extract_num_rows(something_else)).

extract_num_rows_error_default_test() ->
    ?assertEqual(0, kura_repo_worker:extract_num_rows({error, oops})).

extract_num_rows_empty_rows_test() ->
    ?assertEqual(0, kura_repo_worker:extract_num_rows(#{rows => []})).

%%----------------------------------------------------------------------
%% build_log_event
%%----------------------------------------------------------------------

build_log_event_ok_test() ->
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"SELECT 1">>, [], #{rows => [#{a => 1}]}, 1000
    ),
    ?assertEqual(<<"SELECT 1">>, maps:get(query, Event)),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(1, maps:get(num_rows, Event)),
    ?assertEqual(1000, maps:get(duration_us, Event)),
    ?assertEqual(my_repo, maps:get(repo, Event)).

build_log_event_error_test() ->
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"INSERT INTO t">>, [1], {error, oops}, 500
    ),
    ?assertEqual(error, maps:get(result, Event)),
    ?assertEqual(0, maps:get(num_rows, Event)).

%%----------------------------------------------------------------------
%% build_telemetry_metadata
%%----------------------------------------------------------------------

build_telemetry_metadata_ok_test() ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT * FROM users">>, [], #{rows => [#{id => 1}]}
    ),
    ?assertEqual(<<"SELECT * FROM users">>, maps:get(query, Meta)),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(1, maps:get(num_rows, Meta)),
    ?assertEqual(<<"users">>, maps:get(source, Meta)).

build_telemetry_metadata_error_test() ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT 1">>, [], {error, timeout}
    ),
    ?assertEqual(error, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)).

%%----------------------------------------------------------------------
%% extract_source
%%----------------------------------------------------------------------

extract_source_from_test() ->
    ?assertEqual(<<"users">>, kura_repo_worker:extract_source(<<"SELECT * FROM users">>)).

extract_source_insert_test() ->
    ?assertEqual(
        <<"posts">>, kura_repo_worker:extract_source(<<"INSERT INTO posts (id) VALUES (1)">>)
    ).

extract_source_update_test() ->
    ?assertEqual(<<"comments">>, kura_repo_worker:extract_source(<<"UPDATE comments SET x = 1">>)).

extract_source_no_match_test() ->
    ?assertEqual(undefined, kura_repo_worker:extract_source(<<"SELECT 1">>)).

extract_source_quoted_test() ->
    ?assertEqual(
        <<"users">>, kura_repo_worker:extract_source(<<"SELECT * FROM \"users\" WHERE id = 1">>)
    ).
