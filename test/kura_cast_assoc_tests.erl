-module(kura_cast_assoc_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% cast_assoc has_many
%%----------------------------------------------------------------------

cast_assoc_has_many_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [
            #{body => <<"Great post!">>, author_id => 1},
            #{body => <<"Thanks!">>, author_id => 2}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    #{comments := ChildCSs} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(ChildCSs)),
    [C1, C2] = ChildCSs,
    ?assertEqual(insert, C1#kura_changeset.action),
    ?assertEqual(insert, C2#kura_changeset.action),
    ?assertEqual(<<"Great post!">>, maps:get(body, C1#kura_changeset.changes)),
    ?assertEqual(<<"Thanks!">>, maps:get(body, C2#kura_changeset.changes)).

%%----------------------------------------------------------------------
%% cast_assoc invalid child
%%----------------------------------------------------------------------

cast_assoc_invalid_child_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [
            #{author_id => 1}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    _CS2 = kura_changeset:validate_required(CS1, [title]),
    ok.

cast_assoc_invalid_child_with_custom_fn_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [
            #{author_id => 1}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    WithFun = fun(_Data, ChildParams) ->
        ChildCS = kura_changeset:cast(kura_test_comment, #{}, ChildParams, [body, author_id]),
        kura_changeset:validate_required(ChildCS, [body])
    end,
    CS1 = kura_changeset:cast_assoc(CS, comments, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertNot(ChildCS#kura_changeset.valid).

%%----------------------------------------------------------------------
%% cast_assoc custom with function
%%----------------------------------------------------------------------

cast_assoc_custom_with_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [
            #{body => <<"Nice">>, author_id => 1}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    WithFun = fun(_Data, ChildParams) ->
        ChildCS = kura_changeset:cast(kura_test_comment, #{}, ChildParams, [body]),
        kura_changeset:validate_required(ChildCS, [body])
    end,
    CS1 = kura_changeset:cast_assoc(CS, comments, #{with => WithFun}),
    ?assert(CS1#kura_changeset.valid),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(<<"Nice">>, maps:get(body, ChildCS#kura_changeset.changes)).

%%----------------------------------------------------------------------
%% cast_assoc no nested params
%%----------------------------------------------------------------------

cast_assoc_no_params_test() ->
    Params = #{title => <<"My Post">>, body => <<"Hello">>, author_id => 1},
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    ?assertEqual(#{}, CS1#kura_changeset.assoc_changes).

%%----------------------------------------------------------------------
%% cast_assoc update (existing children with PK)
%%----------------------------------------------------------------------

cast_assoc_update_existing_test() ->
    ExistingComments = [
        #{id => 1, body => <<"Old comment">>, post_id => 1, author_id => 1}
    ],
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        comments => [
            #{id => 1, body => <<"Updated comment">>, author_id => 1}
        ]
    },
    CS = kura_changeset:cast(
        kura_test_post, #{id => 1, comments => ExistingComments}, Params, [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(update, ChildCS#kura_changeset.action).

%%----------------------------------------------------------------------
%% put_assoc
%%----------------------------------------------------------------------

put_assoc_with_maps_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, comments, [
        #{body => <<"Comment 1">>, author_id => 1},
        #{body => <<"Comment 2">>, author_id => 2}
    ]),
    #{comments := Children} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(Children)),
    [C1, _C2] = Children,
    ?assertEqual(insert, C1#kura_changeset.action).

put_assoc_with_changeset_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    ChildCS = kura_changeset:cast(
        kura_test_comment, #{}, #{body => <<"Manual">>, author_id => 1}, [
            body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, comments, [ChildCS]),
    #{comments := [Child]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(<<"Manual">>, maps:get(body, Child#kura_changeset.changes)).

%%----------------------------------------------------------------------
%% cast_assoc unknown association
%%----------------------------------------------------------------------

cast_assoc_unknown_test() ->
    CS = kura_changeset:cast(kura_test_post, #{}, #{title => <<"Post">>, author_id => 1}, [
        title, author_id
    ]),
    CS1 = kura_changeset:cast_assoc(CS, nonexistent),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{nonexistent, <<"unknown association">>}], CS1#kura_changeset.errors).
