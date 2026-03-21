-module(kura_conditional_preload_tests).
-moduledoc "Integration tests for conditional preloading.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture — reuses kura_test_schema (users) + kura_test_post_simple_schema
%%----------------------------------------------------------------------

conditional_preload_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"preload with where condition", fun t_where/0},
            {"preload with order_by", fun t_order_by/0},
            {"preload with limit", fun t_limit/0},
            {"preload with combined opts", fun t_combined/0},
            {"preload where filters to empty", fun t_where_empty/0},
            {"regular preload unaffected", fun t_regular_unaffected/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS cond_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS cond_posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  user_id BIGINT NOT NULL REFERENCES cond_users(id),"
        "  title VARCHAR(255) NOT NULL,"
        "  published BOOLEAN DEFAULT false,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query("TRUNCATE cond_users CASCADE", []),
    %% Seed: user with 4 posts, 2 published and 2 drafts
    CS = kura_changeset:cast(kura_test_cond_user, #{}, #{name => ~"Alice"}, [name]),
    {ok, User} = kura_test_repo:insert(CS),
    UserId = maps:get(id, User),
    lists:foreach(
        fun({Title, Published}) ->
            PCS = kura_changeset:cast(
                kura_test_cond_post,
                #{},
                #{user_id => UserId, title => Title, published => Published},
                [user_id, title, published]
            ),
            {ok, _} = kura_test_repo:insert(PCS)
        end,
        [{~"Draft 1", false}, {~"Published A", true}, {~"Draft 2", false}, {~"Published B", true}]
    ),
    {user, User}.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS cond_posts CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS cond_users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

t_where() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [
        {posts, #{where => {published, true}}}
    ]),
    Posts = maps:get(posts, Loaded),
    ?assertEqual(2, length(Posts)),
    Titles = [maps:get(title, P) || P <- Posts],
    ?assert(lists:member(~"Published A", Titles)),
    ?assert(lists:member(~"Published B", Titles)).

t_order_by() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [
        {posts, #{order_by => [{title, desc}]}}
    ]),
    Posts = maps:get(posts, Loaded),
    Titles = [maps:get(title, P) || P <- Posts],
    ?assertEqual([~"Published B", ~"Published A", ~"Draft 2", ~"Draft 1"], Titles).

t_limit() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [{posts, #{limit => 2}}]),
    Posts = maps:get(posts, Loaded),
    ?assertEqual(2, length(Posts)).

t_combined() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [
        {posts, #{where => {published, true}, order_by => [{title, asc}], limit => 1}}
    ]),
    Posts = maps:get(posts, Loaded),
    [Post] = Posts,
    ?assertEqual(~"Published A", maps:get(title, Post)).

t_where_empty() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [
        {posts, #{where => {title, ~"nonexistent"}}}
    ]),
    ?assertEqual([], maps:get(posts, Loaded)).

t_regular_unaffected() ->
    {ok, [User]} = kura_repo_worker:all(kura_test_repo, kura_query:from(kura_test_cond_user)),
    [Loaded] = kura_test_repo:preload(kura_test_cond_user, [User], [posts]),
    Posts = maps:get(posts, Loaded),
    ?assertEqual(4, length(Posts)).
