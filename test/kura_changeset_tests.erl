-module(kura_changeset_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

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
