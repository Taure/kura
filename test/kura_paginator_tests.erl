-module(kura_paginator_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, t_cursor_with_where/0}).

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

paginator_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Offset pagination
            {"first page", fun t_offset_first_page/0},
            {"middle page", fun t_offset_middle_page/0},
            {"last page partial", fun t_offset_last_page_partial/0},
            {"beyond last page", fun t_offset_beyond_last/0},
            {"empty table", fun t_offset_empty/0},
            {"default options", fun t_offset_defaults/0},
            {"with where clause", fun t_offset_with_where/0},
            {"with order_by", fun t_offset_with_order/0},

            %% Cursor pagination
            {"first page forward", fun t_cursor_first_page/0},
            {"forward with after", fun t_cursor_forward_after/0},
            {"backward with before", fun t_cursor_backward_before/0},
            {"end of results", fun t_cursor_end_of_results/0},
            {"empty table", fun t_cursor_empty/0},
            {"custom cursor field", fun t_cursor_custom_field/0},
            {"with where clause", fun t_cursor_with_where/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    seed_users(30),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

seed_users(N) ->
    Entries = [
        #{
            name => iolist_to_binary(io_lib:format("User ~3..0B", [I])),
            email => iolist_to_binary(io_lib:format("user~B@test.com", [I])),
            age => 20 + (I rem 30),
            active => (I rem 3 =/= 0)
        }
     || I <- lists:seq(1, N)
    ],
    {ok, N} = kura_repo_worker:insert_all(kura_test_repo, kura_test_schema, Entries).

%%----------------------------------------------------------------------
%% Offset pagination tests
%%----------------------------------------------------------------------

t_offset_first_page() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 1, page_size => 10}),
    ?assertEqual(10, length(maps:get(entries, Page))),
    ?assertEqual(1, maps:get(page, Page)),
    ?assertEqual(10, maps:get(page_size, Page)),
    ?assertEqual(30, maps:get(total_entries, Page)),
    ?assertEqual(3, maps:get(total_pages, Page)).

t_offset_middle_page() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 2, page_size => 10}),
    ?assertEqual(10, length(maps:get(entries, Page))),
    ?assertEqual(2, maps:get(page, Page)).

t_offset_last_page_partial() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 4, page_size => 8}),
    ?assertEqual(6, length(maps:get(entries, Page))),
    ?assertEqual(4, maps:get(total_pages, Page)).

t_offset_beyond_last() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 100, page_size => 10}),
    ?assertEqual([], maps:get(entries, Page)),
    ?assertEqual(30, maps:get(total_entries, Page)).

t_offset_empty() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"nonexistent">>}),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 1, page_size => 10}),
    ?assertEqual([], maps:get(entries, Page)),
    ?assertEqual(0, maps:get(total_entries, Page)),
    ?assertEqual(0, maps:get(total_pages, Page)).

t_offset_defaults() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{}),
    ?assertEqual(20, length(maps:get(entries, Page))),
    ?assertEqual(1, maps:get(page, Page)),
    ?assertEqual(20, maps:get(page_size, Page)).

t_offset_with_where() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {active, true}),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 1, page_size => 5}),
    ?assertEqual(5, length(maps:get(entries, Page))),
    ?assertEqual(20, maps:get(total_entries, Page)).

t_offset_with_order() ->
    Q = kura_query:order_by(kura_query:from(kura_test_schema), [{name, desc}]),
    {ok, Page} = kura_paginator:paginate(kura_test_repo, Q, #{page => 1, page_size => 5}),
    Entries = maps:get(entries, Page),
    Names = [maps:get(name, E) || E <- Entries],
    ?assertEqual(Names, lists:reverse(lists:sort(Names))).

%%----------------------------------------------------------------------
%% Cursor pagination tests
%%----------------------------------------------------------------------

t_cursor_first_page() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 10}),
    ?assertEqual(10, length(maps:get(entries, Page))),
    ?assertEqual(true, maps:get(has_next, Page)),
    ?assertEqual(false, maps:get(has_prev, Page)),
    ?assertNotEqual(undefined, maps:get(start_cursor, Page)),
    ?assertNotEqual(undefined, maps:get(end_cursor, Page)).

t_cursor_forward_after() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page1} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 10}),
    Cursor = maps:get(end_cursor, Page1),
    {ok, Page2} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{
        limit => 10, 'after' => Cursor
    }),
    ?assertEqual(10, length(maps:get(entries, Page2))),
    ?assertEqual(true, maps:get(has_next, Page2)),
    ?assertEqual(true, maps:get(has_prev, Page2)),
    %% Entries should not overlap
    Ids1 = [maps:get(id, E) || E <- maps:get(entries, Page1)],
    Ids2 = [maps:get(id, E) || E <- maps:get(entries, Page2)],
    ?assertEqual([], Ids1 -- (Ids1 -- Ids2)).

t_cursor_backward_before() ->
    Q = kura_query:from(kura_test_schema),
    %% Get second page first
    {ok, Page1} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 10}),
    {ok, Page2} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{
        limit => 10, 'after' => maps:get(end_cursor, Page1)
    }),
    %% Go backward from page2's start
    StartCursor = maps:get(start_cursor, Page2),
    {ok, BackPage} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{
        limit => 10, before => StartCursor
    }),
    ?assertEqual(true, maps:get(has_next, BackPage)),
    ?assertEqual(false, maps:get(has_prev, BackPage)),
    %% Should match first page
    BackIds = [maps:get(id, E) || E <- maps:get(entries, BackPage)],
    Page1Ids = [maps:get(id, E) || E <- maps:get(entries, Page1)],
    ?assertEqual(Page1Ids, BackIds).

t_cursor_end_of_results() ->
    Q = kura_query:from(kura_test_schema),
    %% Get last page by fetching all 30 in one go
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 30}),
    ?assertEqual(30, length(maps:get(entries, Page))),
    ?assertEqual(false, maps:get(has_next, Page)).

t_cursor_empty() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"nonexistent">>}),
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 10}),
    ?assertEqual([], maps:get(entries, Page)),
    ?assertEqual(false, maps:get(has_next, Page)),
    ?assertEqual(false, maps:get(has_prev, Page)),
    ?assertEqual(undefined, maps:get(start_cursor, Page)),
    ?assertEqual(undefined, maps:get(end_cursor, Page)).

t_cursor_custom_field() ->
    Q = kura_query:from(kura_test_schema),
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{
        limit => 5, cursor_field => age
    }),
    Entries = maps:get(entries, Page),
    Ages = [maps:get(age, E) || E <- Entries],
    %% Should be sorted ascending by age
    ?assertEqual(Ages, lists:sort(Ages)).

t_cursor_with_where() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {active, true}),
    {ok, Page} = kura_paginator:cursor_paginate(kura_test_repo, Q, #{limit => 5}),
    Entries = maps:get(entries, Page),
    ?assertEqual(5, length(Entries)),
    lists:foreach(
        fun(E) ->
            ?assertEqual(true, maps:get(active, E))
        end,
        Entries
    ).
