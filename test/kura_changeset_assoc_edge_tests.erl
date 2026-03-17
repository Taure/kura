-module(kura_changeset_assoc_edge_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, cast_has_one_test/0}).
-eqwalizer({nowarn_function, put_assoc_single_map_test/0}).

put_assoc_unknown_test() ->
    CS = kura_changeset:cast(kura_test_post, #{}, #{title => <<"Post">>, author_id => 1}, [
        title, author_id
    ]),
    CS1 = kura_changeset:put_assoc(CS, nonexistent, []),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{nonexistent, <<"unknown association">>}], CS1#kura_changeset.errors).

cast_has_one_test() ->
    Params = #{
        name => <<"Alice">>,
        email => <<"a@b.com">>,
        profile => #{bio => <<"Developer">>, user_id => 1}
    },
    CS = kura_changeset:cast(kura_test_schema, #{}, Params, [name, email]),
    CS1 = kura_changeset:cast_assoc(CS, profile),
    ?assert(CS1#kura_changeset.valid),
    #{profile := ChildCS} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(insert, ChildCS#kura_changeset.action).

cast_has_one_invalid_child_test() ->
    Params = #{
        name => <<"Alice">>,
        email => <<"a@b.com">>,
        profile => #{bio => <<"Dev">>}
    },
    CS = kura_changeset:cast(kura_test_schema, #{}, Params, [name, email]),
    WithFun = fun(_Data, ChildParams) ->
        ChildCS = kura_changeset:cast(kura_test_profile_schema, #{}, ChildParams, [bio, user_id]),
        kura_changeset:validate_required(ChildCS, [user_id])
    end,
    CS1 = kura_changeset:cast_assoc(CS, profile, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid).

cast_has_one_non_map_params_test() ->
    Params = #{
        name => <<"Alice">>,
        email => <<"a@b.com">>,
        profile => [1, 2, 3]
    },
    CS = kura_changeset:cast(kura_test_schema, #{}, Params, [name, email]),
    CS1 = kura_changeset:cast_assoc(CS, profile),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{profile, <<"expected a map">>}], CS1#kura_changeset.errors).

cast_has_many_non_list_params_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => #{body => <<"not a list">>}
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{comments, <<"expected a list">>}], CS1#kura_changeset.errors).

cast_assoc_existing_nil_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [#{body => <<"New comment">>, author_id => 1}]
    },
    CS = kura_changeset:cast(kura_test_post, #{comments => undefined}, Params, [
        title, body, author_id
    ]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(insert, ChildCS#kura_changeset.action).

put_assoc_single_map_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, comments, [#{body => <<"Comment">>, author_id => 1}]),
    #{comments := Children} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(1, length(Children)).

put_assoc_single_changeset_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    ChildCS = kura_changeset:cast(
        kura_test_comment, #{}, #{body => <<"Manual">>, author_id => 1}, [body, author_id]
    ),
    CS1 = kura_changeset:put_assoc(CS, comments, ChildCS),
    #{comments := Child} = CS1#kura_changeset.assoc_changes,
    ?assert(is_record(Child, kura_changeset)).

coerce_assoc_non_map_non_changeset_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, comments, <<"not a valid value">>),
    #{comments := Value} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(<<"not a valid value">>, Value).

cast_has_many_new_child_without_pk_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [#{body => <<"New">>, author_id => 1}]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(insert, ChildCS#kura_changeset.action).

cast_has_many_child_with_undefined_pk_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [#{id => undefined, body => <<"New">>, author_id => 1}]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(insert, ChildCS#kura_changeset.action).
