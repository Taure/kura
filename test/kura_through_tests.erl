-module(kura_through_tests).
-moduledoc "Integration tests for has_many :through associations.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

through_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"preload through collects nested records", fun t_preload_through/0},
            {"preload through with multiple owners", fun t_multiple_owners/0},
            {"preload through with empty intermediary", fun t_empty_intermediary/0},
            {"preload through with no matching records", fun t_no_matching/0},
            {"direct preload still works alongside through", fun t_direct_and_through/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS through_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS through_posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  user_id BIGINT NOT NULL REFERENCES through_users(id),"
        "  title VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS through_comments ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  post_id BIGINT NOT NULL REFERENCES through_posts(id),"
        "  body TEXT NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query("TRUNCATE through_users CASCADE", []),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS through_comments CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS through_posts CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS through_users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name) ->
    CS = kura_changeset:cast(kura_test_through_user, #{}, #{name => Name}, [name]),
    {ok, R} = kura_test_repo:insert(CS),
    R.

insert_post(UserId, Title) ->
    CS = kura_changeset:cast(kura_test_through_post, #{}, #{user_id => UserId, title => Title}, [
        user_id, title
    ]),
    {ok, R} = kura_test_repo:insert(CS),
    R.

insert_comment(PostId, Body) ->
    CS = kura_changeset:cast(kura_test_through_comment, #{}, #{post_id => PostId, body => Body}, [
        post_id, body
    ]),
    {ok, R} = kura_test_repo:insert(CS),
    R.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_preload_through() ->
    User = insert_user(~"Alice"),
    Post1 = insert_post(maps:get(id, User), ~"Post 1"),
    Post2 = insert_post(maps:get(id, User), ~"Post 2"),
    _C1 = insert_comment(maps:get(id, Post1), ~"Comment on post 1"),
    _C2 = insert_comment(maps:get(id, Post1), ~"Another on post 1"),
    _C3 = insert_comment(maps:get(id, Post2), ~"Comment on post 2"),
    [Loaded] = kura_test_repo:preload(kura_test_through_user, [User], [comments]),
    Comments = maps:get(comments, Loaded),
    ?assertEqual(3, length(Comments)),
    Bodies = lists:sort([maps:get(body, C) || C <- Comments]),
    ?assertEqual([~"Another on post 1", ~"Comment on post 1", ~"Comment on post 2"], Bodies).

t_multiple_owners() ->
    U1 = insert_user(~"Bob"),
    U2 = insert_user(~"Carol"),
    P1 = insert_post(maps:get(id, U1), ~"Bob post"),
    P2 = insert_post(maps:get(id, U2), ~"Carol post"),
    _C1 = insert_comment(maps:get(id, P1), ~"Bob comment"),
    _C2 = insert_comment(maps:get(id, P2), ~"Carol comment 1"),
    _C3 = insert_comment(maps:get(id, P2), ~"Carol comment 2"),
    Loaded = kura_test_repo:preload(kura_test_through_user, [U1, U2], [comments]),
    [L1, L2] = Loaded,
    ?assertEqual(1, length(maps:get(comments, L1))),
    ?assertEqual(2, length(maps:get(comments, L2))).

t_empty_intermediary() ->
    User = insert_user(~"Dave"),
    %% No posts, so no comments
    [Loaded] = kura_test_repo:preload(kura_test_through_user, [User], [comments]),
    ?assertEqual([], maps:get(comments, Loaded)).

t_no_matching() ->
    User = insert_user(~"Eve"),
    _Post = insert_post(maps:get(id, User), ~"Empty post"),
    %% Post exists but has no comments
    [Loaded] = kura_test_repo:preload(kura_test_through_user, [User], [comments]),
    ?assertEqual([], maps:get(comments, Loaded)).

t_direct_and_through() ->
    User = insert_user(~"Frank"),
    Post = insert_post(maps:get(id, User), ~"Frank post"),
    _C = insert_comment(maps:get(id, Post), ~"Frank comment"),
    %% Preload both direct (posts) and through (comments)
    [Loaded] = kura_test_repo:preload(kura_test_through_user, [User], [posts, comments]),
    ?assertEqual(1, length(maps:get(posts, Loaded))),
    ?assertEqual(1, length(maps:get(comments, Loaded))).
