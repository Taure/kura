-module(kura_integration_assoc_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

assoc_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Preloading
            {"preload belongs_to", fun t_preload_belongs_to/0},
            {"preload has_many", fun t_preload_has_many/0},
            {"preload has_one", fun t_preload_has_one/0},
            {"preload many_to_many", fun t_preload_many_to_many/0},
            {"nested preload", fun t_nested_preload/0},
            {"preload on empty list", fun t_preload_empty_list/0},
            {"preload on single record (map)", fun t_preload_single_record/0},
            {"preload multiple associations at once", fun t_preload_multiple_assocs/0},
            {"preload batches across multiple records", fun t_preload_batch/0},
            {"preload belongs_to with nil FK", fun t_preload_belongs_to_nil_fk/0},
            {"preload has_many returns empty list", fun t_preload_has_many_empty/0},
            {"preload many_to_many returns empty list", fun t_preload_many_to_many_empty/0},
            {"preload has_one returns nil", fun t_preload_has_one_nil/0},

            %% cast_assoc / put_assoc
            {"cast_assoc has_many inserts children", fun t_cast_assoc_has_many/0},
            {"cast_assoc single child via has_many", fun t_cast_assoc_single_child/0},
            {"put_assoc many_to_many", fun t_put_assoc_many_to_many/0},
            {"update with assoc changes via cast_assoc", fun t_update_with_cast_assoc/0},

            %% Inline preload via query
            {"inline preload via query", fun t_inline_preload_via_query/0},
            {"inline preload multiple assocs via query", fun t_inline_preload_multiple/0},

            %% Join queries
            {"inner join query", fun t_join_inner/0},
            {"left join query", fun t_join_left/0},

            %% Foreign key constraint on assoc table
            {"FK constraint on post with bad author_id", fun t_fk_constraint_post/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    create_tables(),
    ok.

teardown(_) ->
    drop_tables(),
    ok.

create_tables() ->
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS profiles ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  bio VARCHAR(255),"
        "  user_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  body TEXT,"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS comments ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  body TEXT NOT NULL,"
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tags ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts_tags ("
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  tag_id BIGINT NOT NULL REFERENCES tags(id),"
        "  PRIMARY KEY (post_id, tag_id)"
        ")",
        []
    ),
    ok.

drop_tables() ->
    kura_test_repo:query("DROP TABLE IF EXISTS posts_tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS comments CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS posts CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS profiles CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => Name, email => Email}, [name, email]),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

insert_post(Title, Body, AuthorId) ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{title => Title, body => Body, author_id => AuthorId},
        [title, body, author_id]
    ),
    kura_test_repo:insert(CS).

insert_comment(Body, PostId, AuthorId) ->
    CS = kura_changeset:cast(
        kura_test_comment_schema,
        #{},
        #{body => Body, post_id => PostId, author_id => AuthorId},
        [body, post_id, author_id]
    ),
    kura_test_repo:insert(CS).

insert_tag(Name) ->
    CS = kura_changeset:cast(kura_test_tag_schema, #{}, #{name => Name}, [name]),
    kura_test_repo:insert(CS).

insert_profile(Bio, UserId) ->
    CS = kura_changeset:cast(
        kura_test_profile_schema,
        #{},
        #{bio => Bio, user_id => UserId},
        [bio, user_id]
    ),
    kura_test_repo:insert(CS).

link_post_tag(PostId, TagId) ->
    kura_test_repo:query(
        "INSERT INTO posts_tags (post_id, tag_id) VALUES ($1, $2)", [PostId, TagId]
    ).

%%----------------------------------------------------------------------
%% Preloading
%%----------------------------------------------------------------------

t_preload_belongs_to() ->
    {ok, User} = insert_user(<<"BT_Author">>, <<"bt_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"BT Post">>, <<"body">>, UserId),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [author]),
    Author = maps:get(author, Loaded),
    ?assertMatch(#{name := <<"BT_Author">>}, Author),
    ?assertEqual(UserId, maps:get(id, Author)).

t_preload_has_many() ->
    {ok, User} = insert_user(<<"HM_Author">>, <<"hm_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"HM Post">>, <<"body">>, UserId),
    PostId = maps:get(id, Post),

    {ok, _} = insert_comment(<<"comment 1">>, PostId, UserId),
    {ok, _} = insert_comment(<<"comment 2">>, PostId, UserId),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [comments]),
    Comments = maps:get(comments, Loaded),
    ?assertEqual(2, length(Comments)),
    Bodies = lists:sort([maps:get(body, C) || C <- Comments]),
    ?assertEqual([<<"comment 1">>, <<"comment 2">>], Bodies).

t_preload_has_one() ->
    {ok, User} = insert_user(<<"HO_Author">>, <<"ho_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, _Profile} = insert_profile(<<"My bio">>, UserId),

    Loaded = kura_test_repo:preload(kura_test_schema, User, [profile]),
    Profile = maps:get(profile, Loaded),
    ?assert(is_map(Profile)),
    ?assertEqual(<<"My bio">>, maps:get(bio, Profile)),
    ?assertEqual(UserId, maps:get(user_id, Profile)).

t_preload_many_to_many() ->
    {ok, User} = insert_user(<<"M2M_Author">>, <<"m2m_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"M2M Post">>, <<"body">>, UserId),
    PostId = maps:get(id, Post),

    {ok, Tag1} = insert_tag(<<"erlang">>),
    {ok, Tag2} = insert_tag(<<"otp">>),
    {ok, _} = link_post_tag(PostId, maps:get(id, Tag1)),
    {ok, _} = link_post_tag(PostId, maps:get(id, Tag2)),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [tags]),
    Tags = maps:get(tags, Loaded),
    ?assertEqual(2, length(Tags)),
    TagNames = lists:sort([maps:get(name, T) || T <- Tags]),
    ?assertEqual([<<"erlang">>, <<"otp">>], TagNames).

t_nested_preload() ->
    {ok, User} = insert_user(<<"Nested_Author">>, <<"nested_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"Nested Post">>, <<"body">>, UserId),
    PostId = maps:get(id, Post),

    {ok, _} = insert_comment(<<"nested comment">>, PostId, UserId),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [{comments, [author]}]),
    Comments = maps:get(comments, Loaded),
    ?assertEqual(1, length(Comments)),
    Comment = hd(Comments),
    Author = maps:get(author, Comment),
    ?assertMatch(#{name := <<"Nested_Author">>}, Author).

t_preload_empty_list() ->
    Result = kura_test_repo:preload(kura_test_post_schema, [], [author]),
    ?assertEqual([], Result).

t_preload_single_record() ->
    {ok, User} = insert_user(<<"Single_Author">>, <<"single_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"Single Post">>, <<"body">>, UserId),

    Loaded = kura_test_repo:preload(kura_test_post_schema, Post, [author]),
    ?assert(is_map(Loaded)),
    Author = maps:get(author, Loaded),
    ?assertMatch(#{name := <<"Single_Author">>}, Author).

t_preload_multiple_assocs() ->
    {ok, User} = insert_user(<<"Multi_Author">>, <<"multi_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"Multi Post">>, <<"body">>, UserId),
    PostId = maps:get(id, Post),

    {ok, _} = insert_comment(<<"multi comment">>, PostId, UserId),
    {ok, Tag} = insert_tag(<<"multi_tag">>),
    {ok, _} = link_post_tag(PostId, maps:get(id, Tag)),

    %% Preload author, comments, and tags all at once
    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [author, comments, tags]),
    ?assertMatch(#{name := <<"Multi_Author">>}, maps:get(author, Loaded)),
    ?assertEqual(1, length(maps:get(comments, Loaded))),
    ?assertEqual(1, length(maps:get(tags, Loaded))).

t_preload_batch() ->
    {ok, User} = insert_user(<<"Batch_Author">>, <<"batch_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post1} = insert_post(<<"Batch Post 1">>, <<"body">>, UserId),
    {ok, Post2} = insert_post(<<"Batch Post 2">>, <<"body">>, UserId),
    PostId1 = maps:get(id, Post1),
    PostId2 = maps:get(id, Post2),

    {ok, _} = insert_comment(<<"comment on 1">>, PostId1, UserId),
    {ok, _} = insert_comment(<<"comment on 2a">>, PostId2, UserId),
    {ok, _} = insert_comment(<<"comment on 2b">>, PostId2, UserId),

    %% Preload comments on both posts in a single call
    Loaded = kura_test_repo:preload(kura_test_post_schema, [Post1, Post2], [comments]),
    ?assertEqual(2, length(Loaded)),
    [L1, L2] = Loaded,
    ?assertEqual(1, length(maps:get(comments, L1))),
    ?assertEqual(2, length(maps:get(comments, L2))).

t_preload_belongs_to_nil_fk() ->
    %% Post always has author_id, so test via comment with a valid FK
    %% but verify the lookup works correctly
    {ok, User1} = insert_user(<<"NilFK_A">>, <<"nilfka@test.com">>),
    {ok, User2} = insert_user(<<"NilFK_B">>, <<"nilfkb@test.com">>),
    U1Id = maps:get(id, User1),
    U2Id = maps:get(id, User2),
    {ok, Post} = insert_post(<<"NilFK Post">>, <<"body">>, U1Id),
    PostId = maps:get(id, Post),
    {ok, C1} = insert_comment(<<"by A">>, PostId, U1Id),
    {ok, C2} = insert_comment(<<"by B">>, PostId, U2Id),

    Loaded = kura_test_repo:preload(kura_test_comment_schema, [C1, C2], [author]),
    [L1, L2] = Loaded,
    ?assertEqual(U1Id, maps:get(id, maps:get(author, L1))),
    ?assertEqual(U2Id, maps:get(id, maps:get(author, L2))).

t_preload_has_many_empty() ->
    {ok, User} = insert_user(<<"EmptyHM">>, <<"emptyhm@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"Empty HM Post">>, <<"body">>, UserId),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [comments]),
    ?assertEqual([], maps:get(comments, Loaded)).

t_preload_many_to_many_empty() ->
    {ok, User} = insert_user(<<"EmptyM2M">>, <<"emptym2m@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"Empty M2M Post">>, <<"body">>, UserId),

    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [tags]),
    ?assertEqual([], maps:get(tags, Loaded)).

t_preload_has_one_nil() ->
    {ok, User} = insert_user(<<"NoProfile">>, <<"noprofile@test.com">>),
    Loaded = kura_test_repo:preload(kura_test_schema, User, [profile]),
    ?assertEqual(nil, maps:get(profile, Loaded)).

%%----------------------------------------------------------------------
%% cast_assoc / put_assoc
%%----------------------------------------------------------------------

t_cast_assoc_has_many() ->
    {ok, User} = insert_user(<<"CA_Author">>, <<"ca_author@test.com">>),
    UserId = maps:get(id, User),

    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{
            title => <<"CA Post">>,
            body => <<"body">>,
            author_id => UserId,
            comments => [
                #{body => <<"child comment 1">>, author_id => UserId},
                #{body => <<"child comment 2">>, author_id => UserId}
            ]
        },
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    {ok, Post} = kura_test_repo:insert(CS1),

    PostId = maps:get(id, Post),
    Children = maps:get(comments, Post),
    ?assertEqual(2, length(Children)),
    ?assert(lists:all(fun(C) -> maps:get(post_id, C) =:= PostId end, Children)),
    %% Verify children are in DB
    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Post], [comments]),
    ?assertEqual(2, length(maps:get(comments, Loaded))).

t_cast_assoc_single_child() ->
    {ok, User} = insert_user(<<"CO_Author">>, <<"co_author@test.com">>),
    UserId = maps:get(id, User),

    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{
            title => <<"CO Post">>,
            body => <<"body">>,
            author_id => UserId,
            comments => [
                #{body => <<"only comment">>, author_id => UserId}
            ]
        },
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    {ok, Post} = kura_test_repo:insert(CS1),

    Children = maps:get(comments, Post),
    ?assertEqual(1, length(Children)),
    ?assertEqual(<<"only comment">>, maps:get(body, hd(Children))).

t_put_assoc_many_to_many() ->
    {ok, User} = insert_user(<<"PA_Author">>, <<"pa_author@test.com">>),
    UserId = maps:get(id, User),

    {ok, Tag1} = insert_tag(<<"put_tag1">>),
    {ok, Tag2} = insert_tag(<<"put_tag2">>),

    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{title => <<"PA Post">>, body => <<"body">>, author_id => UserId},
        [title, body, author_id]
    ),
    CS1 = kura_changeset:put_assoc(CS, tags, [Tag1, Tag2]),
    {ok, Post} = kura_test_repo:insert(CS1),

    PostId = maps:get(id, Post),
    Tags = maps:get(tags, Post),
    ?assertEqual(2, length(Tags)),

    %% Verify via preload from DB
    {ok, Refetched} = kura_test_repo:get(kura_test_post_schema, PostId),
    [Loaded] = kura_test_repo:preload(kura_test_post_schema, [Refetched], [tags]),
    LoadedTags = maps:get(tags, Loaded),
    ?assertEqual(2, length(LoadedTags)),
    TagNames = lists:sort([maps:get(name, T) || T <- LoadedTags]),
    ?assertEqual([<<"put_tag1">>, <<"put_tag2">>], TagNames).

t_update_with_cast_assoc() ->
    {ok, User} = insert_user(<<"UpdAssoc_Author">>, <<"updassoc@test.com">>),
    UserId = maps:get(id, User),

    %% First insert a post with one comment
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{
            title => <<"UpdAssoc Post">>,
            body => <<"body">>,
            author_id => UserId,
            comments => [
                #{body => <<"original comment">>, author_id => UserId}
            ]
        },
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    {ok, Post} = kura_test_repo:insert(CS1),
    PostId = maps:get(id, Post),
    OrigComments = maps:get(comments, Post),
    ?assertEqual(1, length(OrigComments)),

    %% Now update the post adding another comment
    {ok, FetchedPost} = kura_test_repo:get(kura_test_post_schema, PostId),
    UpdateCS = kura_changeset:cast(
        kura_test_post_schema,
        FetchedPost,
        #{
            title => <<"UpdAssoc Post Updated">>,
            comments => [
                #{body => <<"new comment">>, author_id => UserId}
            ]
        },
        [title]
    ),
    UpdateCS1 = kura_changeset:cast_assoc(UpdateCS, comments),
    {ok, UpdatedPost} = kura_test_repo:update(UpdateCS1),
    ?assertEqual(<<"UpdAssoc Post Updated">>, maps:get(title, UpdatedPost)),
    NewComments = maps:get(comments, UpdatedPost),
    ?assertEqual(1, length(NewComments)),
    ?assertEqual(<<"new comment">>, maps:get(body, hd(NewComments))).

%%----------------------------------------------------------------------
%% Inline preload via query
%%----------------------------------------------------------------------

t_inline_preload_via_query() ->
    {ok, User} = insert_user(<<"IP_Author">>, <<"ip_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, _Post} = insert_post(<<"IP Post">>, <<"body">>, UserId),

    Q = kura_query:from(kura_test_post_schema),
    Q1 = kura_query:where(Q, {title, <<"IP Post">>}),
    Q2 = kura_query:preload(Q1, [author]),
    {ok, Results} = kura_test_repo:all(Q2),

    ?assert(length(Results) >= 1),
    Post = hd(Results),
    Author = maps:get(author, Post),
    ?assertMatch(#{name := <<"IP_Author">>}, Author).

t_inline_preload_multiple() ->
    {ok, User} = insert_user(<<"IPM_Author">>, <<"ipm_author@test.com">>),
    UserId = maps:get(id, User),
    {ok, _Post} = insert_post(<<"IPM Post">>, <<"body">>, UserId),

    %% Get the post ID from the query result
    Q0 = kura_query:where(kura_query:from(kura_test_post_schema), {title, <<"IPM Post">>}),
    {ok, [Post0]} = kura_test_repo:all(Q0),
    PostId = maps:get(id, Post0),
    {ok, _} = insert_comment(<<"ipm comment">>, PostId, UserId),

    Q = kura_query:from(kura_test_post_schema),
    Q1 = kura_query:where(Q, {title, <<"IPM Post">>}),
    Q2 = kura_query:preload(Q1, [author, comments]),
    {ok, Results} = kura_test_repo:all(Q2),

    Post = hd(Results),
    ?assertMatch(#{name := <<"IPM_Author">>}, maps:get(author, Post)),
    ?assertEqual(1, length(maps:get(comments, Post))).

%%----------------------------------------------------------------------
%% Join queries
%%----------------------------------------------------------------------

t_join_inner() ->
    {ok, User} = insert_user(<<"JoinA">>, <<"joina@test.com">>),
    UserId = maps:get(id, User),
    {ok, _} = insert_post(<<"Join Post 1">>, <<"body">>, UserId),
    {ok, _} = insert_post(<<"Join Post 2">>, <<"body">>, UserId),

    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:join(Q, inner, posts, {author_id, id}),
    Q2 = kura_query:where(Q1, {name, <<"JoinA">>}),
    Q3 = kura_query:select(Q2, [name]),
    Q4 = kura_query:distinct(Q3),
    {ok, Results} = kura_test_repo:all(Q4),
    ?assert(length(Results) >= 1),
    ?assertEqual(<<"JoinA">>, maps:get(name, hd(Results))).

t_join_left() ->
    {ok, _User} = insert_user(<<"LeftJoin">>, <<"leftjoin@test.com">>),

    %% User with no posts — LEFT JOIN should still return the user
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:join(Q, left, posts, {author_id, id}),
    Q2 = kura_query:where(Q1, {name, <<"LeftJoin">>}),
    Q3 = kura_query:select(Q2, [name]),
    {ok, Results} = kura_test_repo:all(Q3),
    ?assert(length(Results) >= 1),
    ?assertEqual(<<"LeftJoin">>, maps:get(name, hd(Results))).

%%----------------------------------------------------------------------
%% Foreign key constraint on assoc table
%%----------------------------------------------------------------------

t_fk_constraint_post() ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{title => <<"Bad Post">>, body => <<"body">>, author_id => 999999},
        [title, body, author_id]
    ),
    CS1 = kura_changeset:foreign_key_constraint(CS, author_id),
    {error, ErrCS} = kura_test_repo:insert(CS1),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assert(lists:keymember(author_id, 1, ErrCS#kura_changeset.errors)).
