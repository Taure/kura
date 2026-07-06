-module(kura_schema_tests).
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

%%----------------------------------------------------------------------
%% key / key_fields (uniform identity)
%%----------------------------------------------------------------------

key_derived_from_primary_key_field_test() ->
    ?assertEqual([id], kura_schema:key(kura_test_schema)).

key_from_callback_composite_test() ->
    ?assertEqual([org_id, user_id], kura_schema:key(kura_test_composite_schema)).

key_fields_single_test() ->
    [Field] = kura_schema:key_fields(kura_test_schema),
    ?assertEqual(id, Field#kura_field.name).

key_fields_composite_preserves_order_test() ->
    Fields = kura_schema:key_fields(kura_test_composite_schema),
    ?assertEqual([org_id, user_id], [F#kura_field.name || F <- Fields]).

primary_key_single_test() ->
    ?assertEqual(id, kura_schema:primary_key(kura_test_schema)).

primary_key_composite_raises_named_error_test() ->
    ?assertError(
        {composite_primary_key, kura_test_composite_schema, [org_id, user_id]},
        kura_schema:primary_key(kura_test_composite_schema)
    ).

primary_key_field_single_test() ->
    Field = kura_schema:primary_key_field(kura_test_schema),
    ?assertEqual(id, Field#kura_field.name).

assoc_fields_from_legacy_foreign_key_test() ->
    A = #kura_assoc{name = user, type = belongs_to, schema = t, foreign_key = user_id},
    ?assertEqual([user_id], kura_schema:assoc_fields(A)).

assoc_fields_from_composite_ref_test() ->
    A = #kura_assoc{
        name = membership,
        type = belongs_to,
        ref = #kura_ref{fields = [org_id, user_id], target = t}
    },
    ?assertEqual([org_id, user_id], kura_schema:assoc_fields(A)).

assoc_fields_none_test() ->
    ?assertEqual([], kura_schema:assoc_fields(#kura_assoc{name = x, type = has_many, schema = t})).

assoc_fields_ref_wins_over_foreign_key_test() ->
    A = #kura_assoc{
        name = membership,
        type = belongs_to,
        foreign_key = legacy_id,
        ref = #kura_ref{fields = [org_id, user_id], target = t}
    },
    ?assertEqual([org_id, user_id], kura_schema:assoc_fields(A)).

assoc_fields_ref_without_fields_falls_back_to_foreign_key_test() ->
    A = #kura_assoc{
        name = membership,
        type = belongs_to,
        foreign_key = user_id,
        ref = #kura_ref{target = t}
    },
    ?assertEqual([user_id], kura_schema:assoc_fields(A)).

assoc_target_from_ref_test() ->
    A = #kura_assoc{name = m, type = belongs_to, schema = legacy_t, ref = #kura_ref{target = t}},
    ?assertEqual(t, kura_schema:assoc_target(A)).

assoc_target_from_legacy_schema_test() ->
    ?assertEqual(
        legacy_t,
        kura_schema:assoc_target(#kura_assoc{name = m, type = belongs_to, schema = legacy_t})
    ).

assoc_target_key_explicit_test() ->
    A = #kura_assoc{
        name = m, type = belongs_to, ref = #kura_ref{fields = [a], target = t, target_key = [b]}
    },
    ?assertEqual([b], kura_schema:assoc_target_key(A)).

assoc_target_key_defaults_undefined_test() ->
    ?assertEqual(
        undefined,
        kura_schema:assoc_target_key(#kura_assoc{name = m, type = belongs_to, foreign_key = fk})
    ).

primary_key_field_composite_raises_named_error_test() ->
    ?assertError(
        {composite_primary_key, kura_test_composite_schema, [org_id, user_id]},
        kura_schema:primary_key_field(kura_test_composite_schema)
    ).
