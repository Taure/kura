-module(kura_schema_hook_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% before_insert hook
%%----------------------------------------------------------------------

before_insert_fires_on_hook_schema_test() ->
    CS = kura_changeset:cast(kura_test_hook_schema, #{}, #{name => <<"test">>}, [name]),
    {ok, CS1} = kura_schema:run_before_insert(kura_test_hook_schema, CS),
    ?assertEqual(<<"hook_inserted">>, kura_changeset:get_change(CS1, status)).

before_insert_default_on_plain_schema_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"test">>}, [name]),
    {ok, CS1} = kura_schema:run_before_insert(kura_test_schema, CS),
    ?assertEqual(CS, CS1).

before_insert_error_propagates_test() ->
    CS = kura_changeset:cast(kura_test_hook_schema, #{}, #{name => <<"reject_insert">>}, [name]),
    {error, ErrCS} = kura_schema:run_before_insert(kura_test_hook_schema, CS),
    ?assertNot(ErrCS#kura_changeset.valid).

%%----------------------------------------------------------------------
%% after_insert hook
%%----------------------------------------------------------------------

after_insert_fires_on_hook_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    {ok, R} = kura_schema:run_after_insert(kura_test_hook_schema, Record),
    ?assertEqual(Record, R).

after_insert_default_on_plain_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    {ok, R} = kura_schema:run_after_insert(kura_test_schema, Record),
    ?assertEqual(Record, R).

after_insert_error_propagates_test() ->
    Record = #{name => <<"fail_after_insert">>, id => 1},
    ?assertEqual(
        {error, after_insert_failed},
        kura_schema:run_after_insert(kura_test_hook_schema, Record)
    ).

%%----------------------------------------------------------------------
%% before_update hook
%%----------------------------------------------------------------------

before_update_fires_on_hook_schema_test() ->
    CS = kura_changeset:cast(kura_test_hook_schema, #{}, #{name => <<"test">>}, [name]),
    {ok, CS1} = kura_schema:run_before_update(kura_test_hook_schema, CS),
    ?assertEqual(CS, CS1).

before_update_default_on_plain_schema_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"test">>}, [name]),
    {ok, CS1} = kura_schema:run_before_update(kura_test_schema, CS),
    ?assertEqual(CS, CS1).

before_update_error_propagates_test() ->
    CS = kura_changeset:cast(kura_test_hook_schema, #{}, #{name => <<"reject_update">>}, [name]),
    {error, ErrCS} = kura_schema:run_before_update(kura_test_hook_schema, CS),
    ?assertNot(ErrCS#kura_changeset.valid).

%%----------------------------------------------------------------------
%% after_update hook
%%----------------------------------------------------------------------

after_update_fires_on_hook_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    {ok, R} = kura_schema:run_after_update(kura_test_hook_schema, Record),
    ?assertEqual(Record, R).

after_update_default_on_plain_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    {ok, R} = kura_schema:run_after_update(kura_test_schema, Record),
    ?assertEqual(Record, R).

after_update_error_propagates_test() ->
    Record = #{name => <<"fail_after_update">>, id => 1},
    ?assertEqual(
        {error, after_update_failed},
        kura_schema:run_after_update(kura_test_hook_schema, Record)
    ).

%%----------------------------------------------------------------------
%% before_delete hook
%%----------------------------------------------------------------------

before_delete_fires_on_hook_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    ?assertEqual(ok, kura_schema:run_before_delete(kura_test_hook_schema, Record)).

before_delete_default_on_plain_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    ?assertEqual(ok, kura_schema:run_before_delete(kura_test_schema, Record)).

before_delete_error_propagates_test() ->
    Record = #{name => <<"reject_delete">>, id => 1},
    ?assertEqual(
        {error, delete_rejected},
        kura_schema:run_before_delete(kura_test_hook_schema, Record)
    ).

%%----------------------------------------------------------------------
%% after_delete hook
%%----------------------------------------------------------------------

after_delete_fires_on_hook_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    ?assertEqual(ok, kura_schema:run_after_delete(kura_test_hook_schema, Record)).

after_delete_default_on_plain_schema_test() ->
    Record = #{name => <<"test">>, id => 1},
    ?assertEqual(ok, kura_schema:run_after_delete(kura_test_schema, Record)).

after_delete_error_propagates_test() ->
    Record = #{name => <<"fail_after_delete">>, id => 1},
    ?assertEqual(
        {error, after_delete_failed},
        kura_schema:run_after_delete(kura_test_hook_schema, Record)
    ).

%%----------------------------------------------------------------------
%% has_after_hook
%%----------------------------------------------------------------------

has_after_hook_insert_true_test() ->
    ?assert(kura_schema:has_after_hook(kura_test_hook_schema, insert)).

has_after_hook_update_true_test() ->
    ?assert(kura_schema:has_after_hook(kura_test_hook_schema, update)).

has_after_hook_delete_true_test() ->
    ?assert(kura_schema:has_after_hook(kura_test_hook_schema, delete)).

has_after_hook_insert_false_test() ->
    ?assertNot(kura_schema:has_after_hook(kura_test_schema, insert)).

has_after_hook_update_false_test() ->
    ?assertNot(kura_schema:has_after_hook(kura_test_schema, update)).

has_after_hook_delete_false_test() ->
    ?assertNot(kura_schema:has_after_hook(kura_test_schema, delete)).
