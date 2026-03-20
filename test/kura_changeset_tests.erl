-module(kura_changeset_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, validate_change_ok_test/0}).
-eqwalizer({nowarn_function, validate_change_error_test/0}).
-eqwalizer({nowarn_function, build_table_constraints_skips_unknown_test/0}).

%%----------------------------------------------------------------------
%% cast
%%----------------------------------------------------------------------

cast_basic_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{}, #{name => <<"Alice">>, email => <<"a@b.com">>}, [name, email]
    ),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(<<"Alice">>, maps:get(name, CS#kura_changeset.changes)),
    ?assertEqual(<<"a@b.com">>, maps:get(email, CS#kura_changeset.changes)).

cast_binary_keys_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{<<"name">> => <<"Alice">>}, [name]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(<<"Alice">>, maps:get(name, CS#kura_changeset.changes)).

cast_ignores_unallowed_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>, age => 30}, [name]),
    ?assertNot(maps:is_key(age, CS#kura_changeset.changes)).

cast_skips_unchanged_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{name => <<"Alice">>}, #{name => <<"Alice">>}, [
        name
    ]),
    ?assertEqual(#{}, CS#kura_changeset.changes).

cast_records_change_when_different_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{name => <<"Alice">>}, #{name => <<"Bob">>}, [
        name
    ]),
    ?assertEqual(<<"Bob">>, maps:get(name, CS#kura_changeset.changes)).

cast_error_on_invalid_type_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"not_a_number">>}, [age]),
    ?assertNot(CS#kura_changeset.valid),
    ?assertMatch([{age, _}], CS#kura_changeset.errors).

cast_integer_field_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"25">>}, [age]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(25, maps:get(age, CS#kura_changeset.changes)).

%%----------------------------------------------------------------------
%% cast enum field
%%----------------------------------------------------------------------

cast_enum_valid_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{status => active}, [status]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(active, maps:get(status, CS#kura_changeset.changes)).

cast_enum_from_binary_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{status => <<"inactive">>}, [status]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(inactive, maps:get(status, CS#kura_changeset.changes)).

cast_enum_invalid_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{status => <<"unknown">>}, [status]),
    ?assertNot(CS#kura_changeset.valid),
    ?assertMatch([{status, _}], CS#kura_changeset.errors).

%%----------------------------------------------------------------------
%% validate_required
%%----------------------------------------------------------------------

validate_required_missing_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name, email]),
    CS2 = kura_changeset:validate_required(CS, [name, email]),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertEqual(2, length(CS2#kura_changeset.errors)).

validate_required_present_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>, email => <<"b">>}, [
        name, email
    ]),
    CS2 = kura_changeset:validate_required(CS, [name, email]),
    ?assert(CS2#kura_changeset.valid).

validate_required_blank_string_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<>>}, [name]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    ?assertNot(CS2#kura_changeset.valid).

validate_required_from_data_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{name => <<"Alice">>}, #{}, [name]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_format
%%----------------------------------------------------------------------

validate_format_match_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"a@b.com">>}, [email]),
    CS2 = kura_changeset:validate_format(CS, email, <<"@">>),
    ?assert(CS2#kura_changeset.valid).

validate_format_nomatch_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"invalid">>}, [email]),
    CS2 = kura_changeset:validate_format(CS, email, <<"@">>),
    ?assertNot(CS2#kura_changeset.valid).

validate_format_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [email]),
    CS2 = kura_changeset:validate_format(CS, email, <<"@">>),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_length
%%----------------------------------------------------------------------

validate_length_min_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"AB">>}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{min, 3}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_length_max_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"ABCDEF">>}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{max, 5}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_length_is_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"ABC">>}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{is, 3}]),
    ?assert(CS2#kura_changeset.valid).

validate_length_ok_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:validate_length(CS, name, [{min, 1}, {max, 100}]),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_number
%%----------------------------------------------------------------------

validate_number_greater_than_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 17}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{greater_than, 18}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_number_less_than_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 200}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{less_than, 150}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_number_ok_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 25}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{greater_than, 0}, {less_than, 150}]),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_inclusion
%%----------------------------------------------------------------------

validate_inclusion_ok_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{role => <<"admin">>}, [role]),
    CS2 = kura_changeset:validate_inclusion(CS, role, [<<"admin">>, <<"user">>, <<"moderator">>]),
    ?assert(CS2#kura_changeset.valid).

validate_inclusion_fail_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{role => <<"superuser">>}, [role]),
    CS2 = kura_changeset:validate_inclusion(CS, role, [<<"admin">>, <<"user">>]),
    ?assertNot(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% virtual fields
%%----------------------------------------------------------------------

cast_virtual_field_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{}, #{full_name => <<"Alice Smith">>}, [full_name]
    ),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(<<"Alice Smith">>, maps:get(full_name, CS#kura_changeset.changes)).

virtual_field_excluded_from_column_map_test() ->
    ColMap = kura_schema:column_map(kura_test_schema),
    ?assertNot(maps:is_key(full_name, ColMap)).

non_virtual_fields_test() ->
    NVF = kura_schema:non_virtual_fields(kura_test_schema),
    ?assertNot(lists:member(full_name, NVF)),
    ?assert(lists:member(name, NVF)).

%%----------------------------------------------------------------------
%% validate_change
%%----------------------------------------------------------------------

validate_change_ok_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:validate_change(CS, name, fun(V) ->
        case byte_size(V) >= 2 of
            true -> ok;
            false -> {error, <<"too short">>}
        end
    end),
    ?assert(CS2#kura_changeset.valid).

validate_change_error_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name]),
    CS2 = kura_changeset:validate_change(CS, name, fun(V) ->
        case byte_size(V) >= 2 of
            true -> ok;
            false -> {error, <<"too short">>}
        end
    end),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{name, <<"too short">>}], CS2#kura_changeset.errors).

validate_change_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name]),
    CS2 = kura_changeset:validate_change(CS, name, fun(_) -> {error, <<"should not run">>} end),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

add_error_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:add_error(CS, name, <<"custom error">>),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertEqual([{name, <<"custom error">>}], CS2#kura_changeset.errors).

get_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    ?assertEqual(<<"Alice">>, kura_changeset:get_change(CS, name)),
    ?assertEqual(undefined, kura_changeset:get_change(CS, email)),
    ?assertEqual(<<"default">>, kura_changeset:get_change(CS, email, <<"default">>)).

get_field_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{email => <<"old@b.com">>}, #{name => <<"Alice">>}, [name]
    ),
    ?assertEqual(<<"Alice">>, kura_changeset:get_field(CS, name)),
    ?assertEqual(<<"old@b.com">>, kura_changeset:get_field(CS, email)).

put_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:put_change(CS, name, <<"Bob">>),
    ?assertEqual(<<"Bob">>, kura_changeset:get_change(CS2, name)).

apply_changes_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{name => <<"Alice">>}, #{email => <<"a@b.com">>}, [email]
    ),
    Result = kura_changeset:apply_changes(CS),
    ?assertEqual(<<"Alice">>, maps:get(name, Result)),
    ?assertEqual(<<"a@b.com">>, maps:get(email, Result)).

apply_action_ok_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    {ok, Data} = kura_changeset:apply_action(CS, insert),
    ?assertEqual(<<"Alice">>, maps:get(name, Data)).

apply_action_error_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => <<"bad">>}, [age]),
    {error, ErrCS} = kura_changeset:apply_action(CS, insert),
    ?assertEqual(insert, ErrCS#kura_changeset.action).

%%----------------------------------------------------------------------
%% Constraint declarations
%%----------------------------------------------------------------------

unique_constraint_default_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"a@b.com">>}, [email]),
    CS2 = kura_changeset:unique_constraint(CS, email),
    ?assertEqual(1, length(CS2#kura_changeset.constraints)),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(<<"users_email_key">>, C#kura_constraint.constraint),
    ?assertEqual(email, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

unique_constraint_custom_opts_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"a@b.com">>}, [email]),
    CS2 = kura_changeset:unique_constraint(CS, email, #{
        name => <<"users_email_unique">>, message => <<"already exists">>
    }),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(<<"users_email_unique">>, C#kura_constraint.constraint),
    ?assertEqual(<<"already exists">>, C#kura_constraint.message).

foreign_key_constraint_default_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:foreign_key_constraint(CS, age),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(foreign_key, C#kura_constraint.type),
    ?assertEqual(<<"users_age_fkey">>, C#kura_constraint.constraint),
    ?assertEqual(age, C#kura_constraint.field),
    ?assertEqual(<<"does not exist">>, C#kura_constraint.message).

foreign_key_constraint_custom_opts_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:foreign_key_constraint(CS, age, #{
        name => <<"custom_fk">>, message => <<"ref missing">>
    }),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(<<"custom_fk">>, C#kura_constraint.constraint),
    ?assertEqual(<<"ref missing">>, C#kura_constraint.message).

check_constraint_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:check_constraint(CS, <<"users_age_positive">>, age),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(check, C#kura_constraint.type),
    ?assertEqual(<<"users_age_positive">>, C#kura_constraint.constraint),
    ?assertEqual(age, C#kura_constraint.field),
    ?assertEqual(<<"is invalid">>, C#kura_constraint.message).

check_constraint_custom_message_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    CS2 = kura_changeset:check_constraint(CS, <<"age_check">>, age, #{
        message => <<"must be positive">>
    }),
    [C] = CS2#kura_changeset.constraints,
    ?assertEqual(<<"must be positive">>, C#kura_constraint.message).

multiple_constraints_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{email => <<"a@b.com">>}, [email]),
    CS2 = kura_changeset:unique_constraint(CS, email),
    CS3 = kura_changeset:foreign_key_constraint(CS2, age),
    ?assertEqual(2, length(CS3#kura_changeset.constraints)).

%%----------------------------------------------------------------------
%% validate_confirmation
%%----------------------------------------------------------------------

validate_confirmation_match_test() ->
    CS = kura_changeset:cast(
        #{password => string},
        #{},
        #{password => <<"secret">>, password_confirmation => <<"secret">>},
        [password]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, password),
    ?assert(CS2#kura_changeset.valid).

validate_confirmation_mismatch_test() ->
    CS = kura_changeset:cast(
        #{password => string},
        #{},
        #{password => <<"secret">>, password_confirmation => <<"other">>},
        [password]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, password),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{password_confirmation, <<"does not match">>}], CS2#kura_changeset.errors).

validate_confirmation_missing_test() ->
    CS = kura_changeset:cast(
        #{password => string},
        #{},
        #{password => <<"secret">>},
        [password]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, password),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{password_confirmation, <<"does not match">>}], CS2#kura_changeset.errors).

validate_confirmation_no_change_test() ->
    CS = kura_changeset:cast(
        #{password => string},
        #{},
        #{},
        [password]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, password),
    ?assert(CS2#kura_changeset.valid).

validate_confirmation_custom_message_test() ->
    CS = kura_changeset:cast(
        #{password => string},
        #{},
        #{password => <<"secret">>, password_confirmation => <<"other">>},
        [password]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, password, #{
        message => <<"passwords don't match">>
    }),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{password_confirmation, <<"passwords don't match">>}], CS2#kura_changeset.errors).

validate_confirmation_schemaless_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(
        Types,
        #{},
        #{email => <<"a@b.com">>, email_confirmation => <<"a@b.com">>},
        [email]
    ),
    CS2 = kura_changeset:validate_confirmation(CS, email),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_exclusion
%%----------------------------------------------------------------------

validate_exclusion_rejected_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{role => <<"admin">>}, [role]),
    CS2 = kura_changeset:validate_exclusion(CS, role, [<<"admin">>, <<"root">>]),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{role, <<"is reserved">>}], CS2#kura_changeset.errors).

validate_exclusion_allowed_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{role => <<"user">>}, [role]),
    CS2 = kura_changeset:validate_exclusion(CS, role, [<<"admin">>, <<"root">>]),
    ?assert(CS2#kura_changeset.valid).

validate_exclusion_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [role]),
    CS2 = kura_changeset:validate_exclusion(CS, role, [<<"admin">>]),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_subset
%%----------------------------------------------------------------------

validate_subset_valid_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"erlang">>, <<"otp">>]}, [tags]),
    CS2 = kura_changeset:validate_subset(CS, tags, [<<"erlang">>, <<"otp">>, <<"beam">>]),
    ?assert(CS2#kura_changeset.valid).

validate_subset_invalid_entry_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{tags => [<<"erlang">>, <<"java">>]}, [tags]),
    CS2 = kura_changeset:validate_subset(CS, tags, [<<"erlang">>, <<"otp">>]),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{tags, <<"has an invalid entry">>}], CS2#kura_changeset.errors).

validate_subset_non_list_test() ->
    CS = kura_changeset:cast(#{tags => string}, #{}, #{tags => <<"not a list">>}, [tags]),
    CS2 = kura_changeset:validate_subset(CS, tags, [<<"a">>, <<"b">>]),
    ?assertNot(CS2#kura_changeset.valid),
    ?assertMatch([{tags, <<"is invalid">>}], CS2#kura_changeset.errors).

validate_subset_no_change_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [tags]),
    CS2 = kura_changeset:validate_subset(CS, tags, [<<"a">>]),
    ?assert(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% traverse_errors
%%----------------------------------------------------------------------

traverse_errors_single_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name, email]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    Result = kura_changeset:traverse_errors(CS2, fun(_Field, Msg) -> Msg end),
    ?assertEqual(#{name => [<<"can't be blank">>]}, Result).

traverse_errors_multiple_same_field_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<>>}, [name]),
    CS2 = kura_changeset:add_error(CS, name, <<"is blank">>),
    CS3 = kura_changeset:add_error(CS2, name, <<"is too short">>),
    Result = kura_changeset:traverse_errors(CS3, fun(_Field, Msg) -> Msg end),
    ?assertEqual(#{name => [<<"is blank">>, <<"is too short">>]}, Result).

traverse_errors_custom_formatter_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, [name]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    Result = kura_changeset:traverse_errors(CS2, fun(Field, Msg) ->
        <<(atom_to_binary(Field))/binary, " ", Msg/binary>>
    end),
    ?assertEqual(#{name => [<<"name can't be blank">>]}, Result).

traverse_errors_empty_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    Result = kura_changeset:traverse_errors(CS, fun(_Field, Msg) -> Msg end),
    ?assertEqual(#{}, Result).

%%----------------------------------------------------------------------
%% prepare_changes
%%----------------------------------------------------------------------

prepare_changes_single_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:prepare_changes(CS, fun(C) ->
        kura_changeset:put_change(C, email, <<"auto@gen.com">>)
    end),
    ?assertEqual(1, length(CS2#kura_changeset.prepare)),
    ?assertEqual(<<"Alice">>, kura_changeset:get_change(CS2, name)).

prepare_changes_chained_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:prepare_changes(CS, fun(C) ->
        kura_changeset:put_change(C, role, <<"user">>)
    end),
    CS3 = kura_changeset:prepare_changes(CS2, fun(C) ->
        kura_changeset:put_change(C, active, true)
    end),
    ?assertEqual(2, length(CS3#kura_changeset.prepare)).

%%----------------------------------------------------------------------
%% optimistic_lock
%%----------------------------------------------------------------------

optimistic_lock_increments_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{lock_version => 0}, #{name => <<"Alice">>}, [
        name
    ]),
    CS2 = kura_changeset:optimistic_lock(CS, lock_version),
    ?assertEqual(1, kura_changeset:get_change(CS2, lock_version)),
    ?assertEqual(lock_version, CS2#kura_changeset.optimistic_lock).

optimistic_lock_from_nil_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    CS2 = kura_changeset:optimistic_lock(CS, lock_version),
    ?assertEqual(1, kura_changeset:get_change(CS2, lock_version)).

%%----------------------------------------------------------------------
%% validate_number — boundary and equal_to
%%----------------------------------------------------------------------

validate_number_gte_boundary_pass_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 18}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{greater_than_or_equal_to, 18}]),
    ?assert(CS2#kura_changeset.valid).

validate_number_gte_boundary_fail_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 17}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{greater_than_or_equal_to, 18}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_number_lte_boundary_pass_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 100}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{less_than_or_equal_to, 100}]),
    ?assert(CS2#kura_changeset.valid).

validate_number_lte_boundary_fail_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 101}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{less_than_or_equal_to, 100}]),
    ?assertNot(CS2#kura_changeset.valid).

validate_number_equal_to_pass_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 42}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{equal_to, 42}]),
    ?assert(CS2#kura_changeset.valid).

validate_number_equal_to_fail_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{age => 43}, [age]),
    CS2 = kura_changeset:validate_number(CS, age, [{equal_to, 42}]),
    ?assertNot(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% validate_required with null
%%----------------------------------------------------------------------

validate_required_null_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => null}, [name]),
    CS2 = kura_changeset:validate_required(CS, [name]),
    ?assertNot(CS2#kura_changeset.valid).

%%----------------------------------------------------------------------
%% record defaults
%%----------------------------------------------------------------------

changeset_defaults_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{}, []),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(#{}, CS#kura_changeset.changes),
    ?assertEqual([], CS#kura_changeset.errors).

%%----------------------------------------------------------------------
%% normalize_key edge cases
%%----------------------------------------------------------------------

cast_unknown_binary_key_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{}, #{<<"completely_unknown_key_xyz_999">> => <<"val">>}, [name]
    ),
    ?assert(CS#kura_changeset.valid).

cast_list_key_existing_atom_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{"name" => <<"Alice">>}, [name]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(<<"Alice">>, maps:get(name, CS#kura_changeset.changes)).

cast_list_key_nonexistent_atom_test() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{}, #{"nonexistent_key_xyz_888" => <<"val">>}, [name]
    ),
    ?assert(CS#kura_changeset.valid).

%%----------------------------------------------------------------------
%% schema constraints auto-registration
%%----------------------------------------------------------------------

schema_constraints_auto_registered_test() ->
    CS = kura_changeset:cast(
        kura_test_participant_schema,
        #{},
        #{
            chat_id => <<"550e8400-e29b-41d4-a716-446655440000">>,
            user_id => <<"550e8400-e29b-41d4-a716-446655440001">>
        },
        [chat_id, user_id]
    ),
    [C] = CS#kura_changeset.constraints,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(<<"participants_chat_id_user_id_key">>, C#kura_constraint.constraint),
    ?assertEqual(chat_id, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

schema_no_constraints_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    ?assertEqual([], CS#kura_changeset.constraints).

schema_index_constraints_auto_registered_test() ->
    CS = kura_changeset:cast(
        kura_test_indexed_schema, #{}, #{code => <<"ABC">>}, [code]
    ),
    ?assertEqual(1, length(CS#kura_changeset.constraints)),
    [C] = CS#kura_changeset.constraints,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(code, C#kura_constraint.field).

schema_constraints_merged_with_manual_test() ->
    CS = kura_changeset:cast(
        kura_test_participant_schema,
        #{},
        #{
            chat_id => <<"550e8400-e29b-41d4-a716-446655440000">>,
            user_id => <<"550e8400-e29b-41d4-a716-446655440001">>
        },
        [chat_id, user_id]
    ),
    CS1 = kura_changeset:unique_constraint(CS, user_id, #{
        name => <<"participants_user_id_key">>
    }),
    ?assertEqual(2, length(CS1#kura_changeset.constraints)).

%%----------------------------------------------------------------------
%% schema_table
%%----------------------------------------------------------------------

schema_table_test() ->
    ?assertEqual(<<"participants">>, kura_changeset:schema_table(kura_test_participant_schema)).

schema_table_indexed_test() ->
    ?assertEqual(<<"indexed_items">>, kura_changeset:schema_table(kura_test_indexed_schema)).

%%----------------------------------------------------------------------
%% join_col_names
%%----------------------------------------------------------------------

join_col_names_single_test() ->
    ?assertEqual(<<"chat_id">>, kura_changeset:join_col_names([chat_id], <<"_">>)).

join_col_names_multiple_test() ->
    ?assertEqual(
        <<"chat_id_user_id">>,
        kura_changeset:join_col_names([chat_id, user_id], <<"_">>)
    ).

join_col_names_different_sep_test() ->
    ?assertEqual(<<"a, b">>, kura_changeset:join_col_names([a, b], <<", ">>)).

%%----------------------------------------------------------------------
%% join_bins
%%----------------------------------------------------------------------

join_bins_empty_test() ->
    ?assertEqual(<<>>, kura_changeset:join_bins([], <<"_">>)).

join_bins_single_test() ->
    ?assertEqual(<<"hello">>, kura_changeset:join_bins([<<"hello">>], <<"_">>)).

join_bins_multiple_test() ->
    ?assertEqual(
        <<"a_b_c">>,
        kura_changeset:join_bins([<<"a">>, <<"b">>, <<"c">>], <<"_">>)
    ).

%%----------------------------------------------------------------------
%% build_table_constraints
%%----------------------------------------------------------------------

build_table_constraints_empty_test() ->
    ?assertEqual([], kura_changeset:build_table_constraints([], <<"t">>)).

build_table_constraints_unique_test() ->
    Result = kura_changeset:build_table_constraints(
        [{unique, [chat_id, user_id]}], <<"participants">>
    ),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(<<"participants_chat_id_user_id_key">>, C#kura_constraint.constraint),
    ?assertEqual(chat_id, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

build_table_constraints_check_test() ->
    Result = kura_changeset:build_table_constraints([{check, <<"qty > 0">>}], <<"orders">>),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(check, C#kura_constraint.type),
    ?assertEqual(<<"orders_check">>, C#kura_constraint.constraint),
    ?assertEqual(base, C#kura_constraint.field),
    ?assertEqual(<<"is invalid">>, C#kura_constraint.message).

build_table_constraints_multiple_test() ->
    Constraints = [{unique, [a, b]}, {check, <<"a > 0">>}],
    Result = kura_changeset:build_table_constraints(Constraints, <<"t">>),
    ?assertEqual(2, length(Result)),
    [Unique, Check] = Result,
    ?assertEqual(unique, Unique#kura_constraint.type),
    ?assertEqual(check, Check#kura_constraint.type).

build_table_constraints_skips_unknown_test() ->
    Result = kura_changeset:build_table_constraints([{foreign_key, something}], <<"t">>),
    ?assertEqual([], Result).

%%----------------------------------------------------------------------
%% build_index_constraints
%%----------------------------------------------------------------------

build_index_constraints_empty_test() ->
    ?assertEqual([], kura_changeset:build_index_constraints([], <<"t">>)).

build_index_constraints_unique_index_test() ->
    Result = kura_changeset:build_index_constraints(
        [{[code], #{unique => true}}], <<"indexed_items">>
    ),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(code, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

build_index_constraints_non_unique_skipped_test() ->
    Result = kura_changeset:build_index_constraints([{[code], #{unique => false}}], <<"t">>),
    ?assertEqual([], Result).

build_index_constraints_no_unique_key_skipped_test() ->
    Result = kura_changeset:build_index_constraints([{[code], #{}}], <<"t">>),
    ?assertEqual([], Result).

%%----------------------------------------------------------------------
%% cast on schema with constraints produces constraint records
%%----------------------------------------------------------------------

cast_participant_has_constraints_test() ->
    CS = kura_changeset:cast(
        kura_test_participant_schema,
        #{},
        #{chat_id => <<"abc">>, user_id => <<"def">>},
        [chat_id, user_id]
    ),
    Constraints = CS#kura_changeset.constraints,
    ?assert(length(Constraints) > 0),
    [C] = [X || X <- Constraints, X#kura_constraint.type =:= unique],
    ?assertEqual(chat_id, C#kura_constraint.field).

cast_indexed_has_constraints_test() ->
    CS = kura_changeset:cast(
        kura_test_indexed_schema,
        #{},
        #{code => <<"ABC">>},
        [code]
    ),
    Constraints = CS#kura_changeset.constraints,
    ?assert(length(Constraints) > 0),
    [C] = [X || X <- Constraints, X#kura_constraint.type =:= unique],
    ?assertEqual(code, C#kura_constraint.field).

cast_plain_schema_no_constraints_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    ?assertEqual([], CS#kura_changeset.constraints).
