-module(kura_schemaless_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% cast schemaless
%%----------------------------------------------------------------------

cast_schemaless_valid_test() ->
    Types = #{email => string, password => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>, password => <<"secret">>}, [
        email, password
    ]),
    ?assert(CS#kura_changeset.valid),
    ?assertEqual(<<"a@b.com">>, maps:get(email, CS#kura_changeset.changes)),
    ?assertEqual(<<"secret">>, maps:get(password, CS#kura_changeset.changes)).

cast_schemaless_invalid_type_test() ->
    Types = #{age => integer},
    CS = kura_changeset:cast(Types, #{}, #{age => <<"not_a_number">>}, [age]),
    ?assertNot(CS#kura_changeset.valid),
    ?assertMatch([{age, _}], CS#kura_changeset.errors).

cast_schemaless_filters_unallowed_test() ->
    Types = #{email => string, password => string, role => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>, role => <<"admin">>}, [email]),
    ?assert(maps:is_key(email, CS#kura_changeset.changes)),
    ?assertNot(maps:is_key(role, CS#kura_changeset.changes)).

cast_schemaless_schema_undefined_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>}, [email]),
    ?assertEqual(undefined, CS#kura_changeset.schema).

%%----------------------------------------------------------------------
%% validators on schemaless
%%----------------------------------------------------------------------

validate_required_schemaless_test() ->
    Types = #{email => string, password => string},
    CS = kura_changeset:cast(Types, #{}, #{}, [email, password]),
    CS1 = kura_changeset:validate_required(CS, [email, password]),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertEqual(2, length(CS1#kura_changeset.errors)).

validate_format_schemaless_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"invalid">>}, [email]),
    CS1 = kura_changeset:validate_format(CS, email, <<"@">>),
    ?assertNot(CS1#kura_changeset.valid).

validate_number_schemaless_test() ->
    Types = #{age => integer},
    CS = kura_changeset:cast(Types, #{}, #{age => 17}, [age]),
    CS1 = kura_changeset:validate_number(CS, age, [{greater_than, 18}]),
    ?assertNot(CS1#kura_changeset.valid).

%%----------------------------------------------------------------------
%% apply / put_change
%%----------------------------------------------------------------------

apply_action_schemaless_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{name => <<"Alice">>}, #{email => <<"a@b.com">>}, [email]),
    {ok, Data} = kura_changeset:apply_action(CS, validate),
    ?assertEqual(<<"a@b.com">>, maps:get(email, Data)),
    ?assertEqual(<<"Alice">>, maps:get(name, Data)).

put_change_schemaless_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{}, [email]),
    CS1 = kura_changeset:put_change(CS, email, <<"put@test.com">>),
    ?assertEqual(<<"put@test.com">>, kura_changeset:get_change(CS1, email)).

%%----------------------------------------------------------------------
%% constraints on schemaless
%%----------------------------------------------------------------------

unique_constraint_schemaless_with_name_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>}, [email]),
    CS1 = kura_changeset:unique_constraint(CS, email, #{name => <<"users_email_key">>}),
    [C] = CS1#kura_changeset.constraints,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(<<"users_email_key">>, C#kura_constraint.constraint).

unique_constraint_schemaless_without_name_errors_test() ->
    Types = #{email => string},
    CS = kura_changeset:cast(Types, #{}, #{email => <<"a@b.com">>}, [email]),
    ?assertError({schemaless_constraint, _}, kura_changeset:unique_constraint(CS, email)).

foreign_key_constraint_schemaless_with_name_test() ->
    Types = #{team_id => integer},
    CS = kura_changeset:cast(Types, #{}, #{team_id => 1}, [team_id]),
    CS1 = kura_changeset:foreign_key_constraint(CS, team_id, #{name => <<"teams_id_fkey">>}),
    [C] = CS1#kura_changeset.constraints,
    ?assertEqual(foreign_key, C#kura_constraint.type),
    ?assertEqual(<<"teams_id_fkey">>, C#kura_constraint.constraint).

foreign_key_constraint_schemaless_without_name_errors_test() ->
    Types = #{team_id => integer},
    CS = kura_changeset:cast(Types, #{}, #{team_id => 1}, [team_id]),
    ?assertError({schemaless_constraint, _}, kura_changeset:foreign_key_constraint(CS, team_id)).
