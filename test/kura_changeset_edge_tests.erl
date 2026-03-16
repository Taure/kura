-module(kura_changeset_edge_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

apply_changes_on_invalid_changeset_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"bad">>}, [age]),
    ?assertNot(CS#kura_changeset.valid),
    Result = kura_changeset:apply_changes(CS),
    ?assert(is_map(Result)).

apply_action_sets_action_on_error_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"bad">>}, [age]),
    {error, ErrCS} = kura_changeset:apply_action(CS, update),
    ?assertEqual(update, ErrCS#kura_changeset.action).

validate_format_non_binary_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 25}, [age]),
    CS2 = kura_changeset:validate_format(CS, age, <<"\\d+">>),
    ?assert(CS2#kura_changeset.valid).

validate_length_list_value_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"a">>, <<"b">>, <<"c">>]}, [tags]),
    CS2 = kura_changeset:validate_length(CS, tags, [{min, 2}, {max, 5}]),
    ?assert(CS2#kura_changeset.valid).

validate_length_list_too_short_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"a">>]}, [tags]),
    CS2 = kura_changeset:validate_length(CS, tags, [{min, 2}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_length_list_too_long_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"a">>, <<"b">>, <<"c">>]}, [tags]),
    CS2 = kura_changeset:validate_length(CS, tags, [{max, 2}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_length_list_is_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"a">>, <<"b">>]}, [tags]),
    CS2 = kura_changeset:validate_length(CS, tags, [{is, 3}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_length_non_string_non_list_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 25}, [age]),
    CS2 = kura_changeset:validate_length(CS, age, [{min, 1}]),
    ?assert(CS2#kura_changeset.valid).

validate_number_non_number_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:validate_number(CS, name, [{greater_than, 0}]),
    ?assert(CS2#kura_changeset.valid).

validate_number_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{greater_than, 0}]),
    ?assert(CS2#kura_changeset.valid).

validate_inclusion_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [role]),
    CS2 = kura_changeset:validate_inclusion(CS, role, [<<"admin">>]),
    ?assert(CS2#kura_changeset.valid).

get_field_with_default_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    ?assertEqual(<<"fallback">>, kura_changeset:get_field(CS, name, <<"fallback">>)).

get_field_prefers_changes_over_data_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{name => <<"Old">>}, #{name => <<"New">>}, [name]
    ),
    ?assertEqual(<<"New">>, kura_changeset:get_field(CS, name)).

put_change_overwrites_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:put_change(CS, name, <<"Bob">>),
    ?assertEqual(<<"Bob">>, kura_changeset:get_change(CS2, name)).

multiple_errors_on_same_field_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS1 = kura_changeset:add_error(CS, name, <<"error 1">>),
    CS2 = kura_changeset:add_error(CS1, name, <<"error 2">>),
    ?assertEqual(2, length(CS2#kura_changeset.errors)),
    ?assertNot(CS2#kura_changeset.valid).

cast_with_nil_value_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => null}, [name]),
    ?assert(CS#kura_changeset.valid).

cast_with_undefined_value_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => undefined}, [name]),
    ?assert(CS#kura_changeset.valid).

schemaless_unique_constraint_requires_name_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>}, [email]),
    ?assertError({schemaless_constraint, _}, kura_changeset:unique_constraint(CS, email)).

schemaless_unique_constraint_with_name_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>}, [email]),
    CS2 = kura_changeset:unique_constraint(CS, email, #{name => <<"my_idx">>}),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(<<"my_idx">>, C#kura_constraint.constraint).

schemaless_foreign_key_constraint_requires_name_test() ->
    Types = #{user_id => integer},
    CS = kura_changeset:cast(Types, #{}, #{user_id => 1}, [user_id]),
    ?assertError({schemaless_constraint, _}, kura_changeset:foreign_key_constraint(CS, user_id)).

schemaless_foreign_key_constraint_with_name_test() ->
    Types = #{user_id => integer},
    CS = kura_changeset:cast(Types, #{}, #{user_id => 1}, [user_id]),
    CS2 = kura_changeset:foreign_key_constraint(CS, user_id, #{name => <<"my_fk">>}),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(<<"my_fk">>, C#kura_constraint.constraint).

normalize_params_integer_key_test() ->
    Params = kura_changeset:normalize_params(#{42 => <<"val">>}),
    ?assertEqual(<<"val">>, maps:get(42, Params)).

cast_field_not_in_types_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{nonexistent_field_xyz => <<"val">>}, [
        nonexistent_field_xyz
    ]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(#{}, CS#kura_changeset.changes).

validate_required_with_existing_errors_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"bad">>}, [age]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertEqual(2, length(CS2#kura_changeset.errors)).

validate_length_is_exact_pass_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"ABC">>}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{is, 3}]),
    ?assert(CS2#kura_changeset.valid).

validate_length_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{min, 1}]),
    ?assert(CS2#kura_changeset.valid).

check_constraint_on_schema_changeset_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 10}, [age]),
    CS2 = kura_changeset:check_constraint(CS, <<"users_age_check">>, age, #{
        message => <<"must be positive">>
    }),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(check, C#kura_constraint.type),
    ?assertEqual(age, C#kura_constraint.field).

build_schema_constraints_check_test() ->
    CS = kura_changeset:cast(kura_test_participant_schema, #{}, #{}, []),
    Constraints = CS#kura_changeset.constraints,
    ?assert(length(Constraints) >= 1),
    [C] = Constraints,
    ?assertEqual(unique, C#kura_constraint.type).

cast_multiple_type_errors_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"bad">>, score => <<"bad">>}, [
        age, score
    ]),
    ?assertNot(CS#kura_changeset.valid),
    ?assertEqual(2, length(CS#kura_changeset.errors)).

validate_change_with_list_return_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"ok">>}, [name]),
    CS2 = kura_changeset:validate_change(CS, name, fun(_V) -> ok end),
    ?assert(CS2#kura_changeset.valid).

optimistic_lock_existing_value_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{lock_version => 5}, #{name => <<"A">>}, [name]),
    CS2 = kura_changeset:optimistic_lock(CS, lock_version),
    ?assertEqual(6, kura_changeset:get_change(CS2, lock_version)).
