-module(kura_many_to_many_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Association record
%%----------------------------------------------------------------------

assoc_record_test() ->
    Assoc = #kura_assoc{
        name = tags,
        type = many_to_many,
        schema = kura_test_tag,
        join_through = <<"posts_tags">>,
        join_keys = {post_id, tag_id}
    },
    ?assertEqual(tags, Assoc#kura_assoc.name),
    ?assertEqual(many_to_many, Assoc#kura_assoc.type),
    ?assertEqual(kura_test_tag, Assoc#kura_assoc.schema),
    ?assertEqual(undefined, Assoc#kura_assoc.foreign_key),
    ?assertEqual(<<"posts_tags">>, Assoc#kura_assoc.join_through),
    ?assertEqual({post_id, tag_id}, Assoc#kura_assoc.join_keys).

%%----------------------------------------------------------------------
%% Schema association lookup
%%----------------------------------------------------------------------

association_lookup_test() ->
    {ok, Assoc} = kura_schema:association(kura_test_post, tags),
    ?assertEqual(tags, Assoc#kura_assoc.name),
    ?assertEqual(many_to_many, Assoc#kura_assoc.type),
    ?assertEqual(kura_test_tag, Assoc#kura_assoc.schema),
    ?assertEqual(<<"posts_tags">>, Assoc#kura_assoc.join_through),
    ?assertEqual({post_id, tag_id}, Assoc#kura_assoc.join_keys).

%%----------------------------------------------------------------------
%% cast_assoc many_to_many
%%----------------------------------------------------------------------

cast_assoc_many_to_many_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        tags => [
            #{name => <<"erlang">>},
            #{name => <<"otp">>}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, tags),
    ?assert(CS1#kura_changeset.valid),
    #{tags := ChildCSs} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(ChildCSs)),
    [C1, C2] = ChildCSs,
    ?assertEqual(insert, C1#kura_changeset.action),
    ?assertEqual(insert, C2#kura_changeset.action),
    ?assertEqual(<<"erlang">>, maps:get(name, C1#kura_changeset.changes)),
    ?assertEqual(<<"otp">>, maps:get(name, C2#kura_changeset.changes)).

cast_assoc_many_to_many_update_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        tags => [
            #{id => 1, name => <<"updated-tag">>}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, tags),
    ?assert(CS1#kura_changeset.valid),
    #{tags := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(update, ChildCS#kura_changeset.action).

cast_assoc_many_to_many_invalid_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        tags => <<"not a list">>
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, tags),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{tags, <<"expected a list">>}], CS1#kura_changeset.errors).

%%----------------------------------------------------------------------
%% put_assoc many_to_many
%%----------------------------------------------------------------------

put_assoc_many_to_many_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, tags, [
        #{name => <<"tag1">>},
        #{name => <<"tag2">>}
    ]),
    #{tags := Children} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(2, length(Children)),
    [C1, _C2] = Children,
    ?assertEqual(insert, C1#kura_changeset.action).

put_assoc_many_to_many_existing_test() ->
    CS = kura_changeset:cast(
        kura_test_post, #{}, #{title => <<"Post">>, body => <<"Hi">>, author_id => 1}, [
            title, body, author_id
        ]
    ),
    CS1 = kura_changeset:put_assoc(CS, tags, [
        #{id => 99, name => <<"existing-tag">>}
    ]),
    #{tags := [Child]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(undefined, Child#kura_changeset.action).

%%----------------------------------------------------------------------
%% default_cast_fun no FK exclusion
%%----------------------------------------------------------------------

default_cast_fun_no_fk_test() ->
    Params = #{
        title => <<"My Post">>,
        body => <<"Hello">>,
        author_id => 1,
        tags => [
            #{name => <<"erlang">>}
        ]
    },
    CS = kura_changeset:cast(kura_test_post, #{}, Params, [title, body, author_id]),
    CS1 = kura_changeset:cast_assoc(CS, tags),
    #{tags := [ChildCS]} = CS1#kura_changeset.assoc_changes,
    ?assertEqual(<<"erlang">>, maps:get(name, ChildCS#kura_changeset.changes)).
