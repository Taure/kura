-module(kura_changeset_assoc_helper_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, cast_assoc_has_many_with_existing_test/0}).
-eqwalizer({nowarn_function, cast_assoc_has_many_insert_only_test/0}).

%%----------------------------------------------------------------------
%% cast_embed embeds_many: exercises cast_embed_list, all_valid, reverse_changesets
%%----------------------------------------------------------------------

embed_many_all_valid_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{label => <<"a">>, weight => 1},
            #{label => <<"b">>, weight => 2},
            #{label => <<"c">>, weight => 3}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, labels),
    ?assert(CS1#kura_changeset.valid),
    #{labels := Labels} = CS1#kura_changeset.changes,
    ?assertEqual(3, length(Labels)),
    [L1, L2, L3] = Labels,
    ?assertEqual(<<"a">>, maps:get(label, L1)),
    ?assertEqual(<<"b">>, maps:get(label, L2)),
    ?assertEqual(<<"c">>, maps:get(label, L3)).

embed_many_preserves_order_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{label => <<"first">>, weight => 1},
            #{label => <<"second">>, weight => 2}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, labels),
    #{labels := [L1, L2]} = CS1#kura_changeset.changes,
    ?assertEqual(<<"first">>, maps:get(label, L1)),
    ?assertEqual(<<"second">>, maps:get(label, L2)).

%%----------------------------------------------------------------------
%% cast_embed embeds_many invalid children: exercises collect_errors
%%----------------------------------------------------------------------

embed_many_collects_errors_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{weight => <<"not_int">>},
            #{weight => <<"also_bad">>}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    WithFun = fun(_Data, EmbedParams) ->
        ChildCS = kura_changeset:cast(kura_test_label, #{}, EmbedParams, [label, weight]),
        kura_changeset:validate_required(ChildCS, [label])
    end,
    CS1 = kura_changeset:cast_embed(CS, labels, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid),
    Errors = CS1#kura_changeset.errors,
    LabelErrors = [E || {label, _} = E <- Errors],
    ?assert(length(LabelErrors) >= 2).

embed_many_mixed_valid_invalid_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{label => <<"good">>, weight => 1},
            #{weight => 2}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    WithFun = fun(_Data, EmbedParams) ->
        ChildCS = kura_changeset:cast(kura_test_label, #{}, EmbedParams, [label, weight]),
        kura_changeset:validate_required(ChildCS, [label])
    end,
    CS1 = kura_changeset:cast_embed(CS, labels, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid).

embed_many_empty_list_test() ->
    Params = #{name => <<"Alice">>, labels => []},
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, labels),
    ?assert(CS1#kura_changeset.valid),
    #{labels := Labels} = CS1#kura_changeset.changes,
    ?assertEqual([], Labels).

%%----------------------------------------------------------------------
%% cast_assoc has_many with existing records:
%% exercises build_existing_lookup, cast_has_many_children
%%----------------------------------------------------------------------

cast_assoc_has_many_with_existing_test() ->
    ExistingComments = [
        #{id => 10, body => <<"Old">>, post_id => 1, author_id => 1},
        #{id => 20, body => <<"Other">>, post_id => 1, author_id => 2}
    ],
    Params = #{
        title => <<"Post">>,
        body => <<"Body">>,
        author_id => 1,
        comments => [
            #{id => 10, body => <<"Updated">>, author_id => 1},
            #{body => <<"New comment">>, author_id => 3}
        ]
    },
    CS = kura_changeset:cast(
        kura_test_post,
        #{id => 1, comments => ExistingComments},
        Params,
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    #{comments := ChildCSs} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(ChildCSs)),
    [C1, C2] = ChildCSs,
    ?assertEqual(update, C1#kura_changeset.action),
    ?assertEqual(insert, C2#kura_changeset.action),
    ?assertEqual(<<"Updated">>, maps:get(body, C1#kura_changeset.changes)).

cast_assoc_has_many_insert_only_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Body">>,
        author_id => 1,
        comments => [
            #{body => <<"C1">>, author_id => 1},
            #{body => <<"C2">>, author_id => 2}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid),
    #{comments := ChildCSs} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(ChildCSs)),
    lists:foreach(
        fun(ChildCS) ->
            ?assertEqual(insert, ChildCS#kura_changeset.action)
        end,
        ChildCSs
    ).

cast_assoc_has_many_empty_existing_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Body">>,
        author_id => 1,
        comments => [#{body => <<"New">>, author_id => 1}]
    },
    CS = kura_changeset:cast(
        kura_test_post,
        #{id => 1, comments => []},
        Params,
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    #{comments := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(insert, ChildCS#kura_changeset.action).

cast_assoc_has_many_bad_type_test() ->
    Params = #{
        title => <<"Post">>,
        body => <<"Body">>,
        author_id => 1,
        comments => <<"not a list">>
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assertNot(CS1#kura_changeset.valid).
