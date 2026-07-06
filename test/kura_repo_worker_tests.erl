-module(kura_repo_worker_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% key_clauses (uniform key-spec addressing)
%%----------------------------------------------------------------------

key_clauses_single_bare_value_test() ->
    ?assertEqual(
        [{id, 42}],
        kura_repo_worker:key_clauses(kura_test_schema, 42)
    ).

key_clauses_single_map_test() ->
    ?assertEqual(
        [{id, 42}],
        kura_repo_worker:key_clauses(kura_test_schema, #{id => 42})
    ).

key_clauses_composite_map_ordered_test() ->
    ?assertEqual(
        [{org_id, <<"o">>}, {user_id, <<"u">>}],
        kura_repo_worker:key_clauses(kura_test_composite_schema, #{
            user_id => <<"u">>, org_id => <<"o">>
        })
    ).

key_clauses_composite_bare_value_rejected_test() ->
    ?assertError(
        {key_spec_required, kura_test_composite_schema, [org_id, user_id]},
        kura_repo_worker:key_clauses(kura_test_composite_schema, <<"x">>)
    ).

key_clauses_incomplete_key_rejected_test() ->
    ?assertError(
        {incomplete_key, kura_test_composite_schema, user_id},
        kura_repo_worker:key_clauses(kura_test_composite_schema, #{org_id => <<"o">>})
    ).

key_conds_composite_counter_test() ->
    %% The optimistic-lock UPDATE builder must continue placeholder
    %% numbering from the SET counter and hand the lock the next slot.
    {Conds, Params, Next} = kura_repo_worker:key_conds(
        [{org_id, <<"o">>}, {user_id, <<"u">>}], 2
    ),
    ?assertEqual(
        [<<"\"org_id\" = $2">>, <<"\"user_id\" = $3">>],
        [iolist_to_binary(C) || C <- Conds]
    ),
    ?assertEqual([<<"o">>, <<"u">>], Params),
    ?assertEqual(4, Next).

reload_composite_missing_key_col_rejected_test() ->
    %% user_id absent from the record: the key-completeness guard must
    %% reject before any repo round-trip, so the repo arg is never used.
    ?assertEqual(
        {error, no_primary_key},
        kura_repo_worker:reload(undefined, kura_test_composite_schema, #{org_id => <<"o">>})
    ).

%%----------------------------------------------------------------------
%% narrow_ok_error
%%----------------------------------------------------------------------

narrow_ok_error_ok_test() ->
    ?assertEqual({ok, 42}, kura_db:narrow_ok_error({ok, 42})).

narrow_ok_error_error_test() ->
    ?assertEqual({error, reason}, kura_db:narrow_ok_error({error, reason})).

narrow_ok_error_ok_complex_test() ->
    ?assertEqual({ok, #{a => 1}}, kura_db:narrow_ok_error({ok, #{a => 1}})).

%%----------------------------------------------------------------------
%% extract_result_status
%%----------------------------------------------------------------------

extract_result_status_error_test() ->
    ?assertEqual(error, kura_db:extract_result_status({error, oops})).

extract_result_status_ok_map_test() ->
    ?assertEqual(ok, kura_db:extract_result_status(#{rows => []})).

extract_result_status_ok_atom_test() ->
    ?assertEqual(ok, kura_db:extract_result_status(ok)).

extract_result_status_ok_other_test() ->
    ?assertEqual(ok, kura_db:extract_result_status(42)).

%%----------------------------------------------------------------------
%% extract_num_rows
%%----------------------------------------------------------------------

extract_num_rows_from_num_rows_test() ->
    ?assertEqual(5, kura_db:extract_num_rows(#{num_rows => 5})).

extract_num_rows_from_rows_test() ->
    ?assertEqual(3, kura_db:extract_num_rows(#{rows => [a, b, c]})).

extract_num_rows_default_test() ->
    ?assertEqual(0, kura_db:extract_num_rows(something_else)).

extract_num_rows_error_default_test() ->
    ?assertEqual(0, kura_db:extract_num_rows({error, oops})).

extract_num_rows_empty_rows_test() ->
    ?assertEqual(0, kura_db:extract_num_rows(#{rows => []})).

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
    ?assertEqual(<<"users">>, maps:get(source, Meta)),
    ?assertEqual(undefined, maps:get(tenant, Meta)),
    ?assertEqual(undefined, maps:get(error_reason, Meta)).

build_telemetry_metadata_error_test() ->
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT 1">>, [], {error, timeout}
    ),
    ?assertEqual(error, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)),
    ?assertEqual(timeout, maps:get(error_reason, Meta)).

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

%%----------------------------------------------------------------------
%% read_only / replica routing
%%----------------------------------------------------------------------

with_repos(Map, Fun) ->
    application:set_env(kura, repos, Map),
    try
        Fun()
    after
        application:unset_env(kura, repos)
    end.

read_only_defaults_false_test() ->
    with_repos(#{ro_default => #{}}, fun() ->
        ?assertEqual(false, kura_repo:read_only(ro_default))
    end).

read_only_true_test() ->
    with_repos(#{ro_true => #{read_only => true}}, fun() ->
        ?assertEqual(true, kura_repo:read_only(ro_true))
    end).

replica_none_returns_self_test() ->
    with_repos(#{primary => #{}}, fun() ->
        ?assertEqual(primary, kura_repo:replica(primary))
    end).

replica_single_test() ->
    with_repos(#{primary => #{replicas => [primary_ro]}}, fun() ->
        ?assertEqual(primary_ro, kura_repo:replica(primary))
    end).

replica_multi_member_test() ->
    with_repos(#{primary => #{replicas => [r1, r2, r3]}}, fun() ->
        ?assert(lists:member(kura_repo:replica(primary), [r1, r2, r3]))
    end).

read_only_repo_rejects_insert_test() ->
    with_repos(#{ro_repo => #{read_only => true}}, fun() ->
        CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"x">>}, [name]),
        ?assertEqual({error, read_only}, kura_repo_worker:insert(ro_repo, CS))
    end).

read_only_repo_rejects_delete_all_test() ->
    with_repos(#{ro_repo => #{read_only => true}}, fun() ->
        ?assertEqual(
            {error, read_only},
            kura_repo_worker:delete_all(ro_repo, kura_query:from(kura_test_schema))
        )
    end).

read_only_repo_rejects_update_test() ->
    with_repos(#{ro_repo => #{read_only => true}}, fun() ->
        CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"x">>}, [name]),
        ?assertEqual({error, read_only}, kura_repo_worker:update(ro_repo, CS))
    end).

read_only_repo_rejects_soft_delete_test() ->
    with_repos(#{ro_repo => #{read_only => true}}, fun() ->
        CS = kura_changeset:cast(kura_test_soft_delete_schema, #{}, #{name => <<"x">>}, [name]),
        ?assertEqual({error, read_only}, kura_repo_worker:soft_delete(ro_repo, CS))
    end).
