-module(kura_vector_SUITE).
-moduledoc """
pgvector distance queries against a live server.

Needs a Postgres with the pgvector extension installed. CI runs the
`pgvector/pgvector` image on port 5555. Point the suite elsewhere with
`KURA_PGVECTOR_URL`, for example
`postgres://postgres:postgres@localhost:5556/postgres`. The suite skips
when the server cannot create the `vector` extension.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    vector_round_trips_through_a_changeset/1,
    nearest_rows_come_first_for_each_metric/1,
    distance_binds_alongside_where_limit_and_offset/1,
    cached_query_takes_each_new_vector/1,
    hnsw_index_serves_the_ordering/1,
    ordering_by_the_select_alias_uses_the_index/1,
    large_vector_round_trips_and_ranks/1
]).

-define(REPO, kura_vector_test_repo).
-define(SCHEMA, kura_test_vector_schema).
-define(INDEX, "kura_vector_items_embedding_cos").
-define(DEFAULT_URL, "postgres://postgres:root@localhost:5555/kura_test").

all() ->
    [
        vector_round_trips_through_a_changeset,
        nearest_rows_come_first_for_each_metric,
        distance_binds_alongside_where_limit_and_offset,
        cached_query_takes_each_new_vector,
        hnsw_index_serves_the_ordering,
        ordering_by_the_select_alias_uses_the_index,
        large_vector_round_trips_and_ranks
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    application:set_env(kura, ensure_database, false),
    application:set_env(kura, ?REPO, repo_config(url())),
    ok = kura_repo_worker:start(?REPO),
    case kura_repo_worker:query(?REPO, ~"CREATE EXTENSION IF NOT EXISTS vector", []) of
        {ok, _} ->
            Config;
        {error, Reason} ->
            #{hostname := Host, port := Port} = repo_config(url()),
            {skip,
                lists:flatten(
                    io_lib:format("pgvector is not available on ~s:~p: ~s", [
                        Host, Port, reason(Reason)
                    ])
                )}
    end.

end_per_suite(_Config) ->
    _ = kura_repo_worker:query(?REPO, ~"DROP TABLE IF EXISTS kura_vector_items", []),
    application:unset_env(kura, ?REPO),
    application:unset_env(kura, ensure_database),
    ok.

init_per_testcase(_TC, Config) ->
    {ok, _} = kura_repo_worker:query(?REPO, ~"DROP TABLE IF EXISTS kura_vector_items", []),
    {ok, _} = kura_repo_worker:query(
        ?REPO,
        ~"CREATE TABLE kura_vector_items (id BIGSERIAL PRIMARY KEY, content TEXT, embedding VECTOR(3), any_embedding VECTOR)",
        []
    ),
    kura_query_cache:flush(),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

vector_round_trips_through_a_changeset(_Config) ->
    #{id := Id} = insert(~"a", [1, 2.5, -3]),
    {ok, Row} = kura_repo_worker:get(?REPO, ?SCHEMA, Id),
    ?assertEqual([1.0, 2.5, -3.0], maps:get(embedding, Row)).

nearest_rows_come_first_for_each_metric(_Config) ->
    seed(),
    Query = [1, 0, 0],
    [
        begin
            D = kura_query:distance(embedding, Metric, Query),
            Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
            {ok, Rows} = kura_repo_worker:all(?REPO, kura_query:order_by(Q0, [{D, asc}])),
            Expected = expected(Metric, Query),
            ?assertEqual([C || {C, _} <- Expected], [C || #{content := C} <- Rows]),
            [
                ?assert(abs(Want - Got) < 1.0e-5)
             || {{_, Want}, #{dist := Got}} <- lists:zip(Expected, Rows)
            ]
        end
     || Metric <- [l2, cosine, inner_product]
    ].

distance_binds_alongside_where_limit_and_offset(_Config) ->
    seed(),
    D = kura_query:distance(embedding, l2, [1, 0, 0]),
    Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
    Q1 = kura_query:where(Q0, {content, '!=', ~"w"}),
    Q2 = kura_query:having(
        kura_query:group_by(Q1, [content, embedding]), {fragment, ~"count(*) >= ?", [1]}
    ),
    Q3 = kura_query:offset(kura_query:limit(kura_query:order_by(Q2, [{D, asc}]), 2), 1),
    {ok, Rows} = kura_repo_worker:all(?REPO, Q3),
    ?assertEqual([~"y", ~"z"], [C || #{content := C} <- Rows]).

cached_query_takes_each_new_vector(_Config) ->
    seed(),
    Nearest = fun(Vec) ->
        D = kura_query:distance(embedding, l2, Vec),
        Q = kura_query:limit(kura_query:order_by(kura_query:from(?SCHEMA), [{D, asc}]), 1),
        {ok, [#{content := C}]} = kura_repo_worker:all(?REPO, Q),
        C
    end,
    ?assertEqual(~"x", Nearest([1, 0, 0])),
    ?assertEqual(~"y", Nearest([0, 1, 0])),
    ?assertEqual(~"z", Nearest([0, 0, 1])).

hnsw_index_serves_the_ordering(_Config) ->
    D = kura_query:distance(embedding, cosine, [1, 0, 0]),
    Q = kura_query:limit(kura_query:order_by(kura_query:from(?SCHEMA), [{D, asc}]), 2),
    ?assertNotEqual(nomatch, binary:match(index_plan(Q), <<"Index Scan using " ?INDEX>>)).

%% Ordering by the alias of a selected distance sends the vector once,
%% and the planner still matches the ORDER BY to the index.
ordering_by_the_select_alias_uses_the_index(_Config) ->
    D = kura_query:distance(embedding, cosine, [1, 0, 0]),
    Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
    Q = kura_query:limit(kura_query:order_by(Q0, [{dist, asc}]), 2),
    {_SQL, Params} = kura_query_compiler:to_sql(?REPO, Q),
    ?assertEqual([~"[1.0,0.0,0.0]", 2], Params),
    ?assertNotEqual(nomatch, binary:match(index_plan(Q), <<"Index Scan using " ?INDEX>>)),
    {ok, Rows} = kura_repo_worker:all(?REPO, Q),
    ?assertEqual([~"x", ~"y"], [C || #{content := C} <- Rows]).

large_vector_round_trips_and_ranks(_Config) ->
    Dim = 1536,
    Near = [float(I rem 7) || I <- lists:seq(1, Dim)],
    Far = [float(-(I rem 5)) || I <- lists:seq(1, Dim)],
    #{id := NearId} = insert_any(~"near", Near),
    #{id := _} = insert_any(~"far", Far),
    {ok, #{any_embedding := Loaded}} = kura_repo_worker:get(?REPO, ?SCHEMA, NearId),
    ?assertEqual(Near, Loaded),
    D = kura_query:distance(any_embedding, l2, Near),
    Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
    {ok, [First, Second]} = kura_repo_worker:all(?REPO, kura_query:order_by(Q0, [{D, asc}])),
    ?assertMatch(#{content := ~"near", dist := +0.0}, First),
    ?assertMatch(#{content := ~"far"}, Second).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Seeds the table, builds the HNSW index and returns the plan for Q with
%% sequential scans off, under the settings the guide recommends.
index_plan(Q) ->
    seed(),
    {ok, _} = kura_repo_worker:query(
        ?REPO,
        <<"CREATE INDEX " ?INDEX " ON kura_vector_items USING hnsw (embedding vector_cosine_ops)">>,
        []
    ),
    {SQL, Params} = kura_query_compiler:to_sql(?REPO, Q),
    Plan = kura_repo_worker:transaction(?REPO, fun() ->
        {ok, _} = kura_repo_worker:query(?REPO, ~"SET LOCAL enable_seqscan = off", []),
        {ok, _} = kura_repo_worker:query(?REPO, ~"SET LOCAL hnsw.ef_search = 100", []),
        {ok, _} = kura_repo_worker:query(
            ?REPO, ~"SET LOCAL hnsw.iterative_scan = strict_order", []
        ),
        {ok, Rows} = kura_repo_worker:query(?REPO, [~"EXPLAIN ", SQL], Params),
        iolist_to_binary([maps:get('QUERY PLAN', R) || R <- Rows])
    end),
    unwrap(Plan).

seed() ->
    _ = insert(~"x", [1, 0, 0]),
    _ = insert(~"y", [0.8, 0.6, 0]),
    _ = insert(~"z", [0, 0.6, 0.8]),
    ok.

%% Distances from `Query' to the seed rows, nearest first.
expected(Metric, Query) ->
    Rows = [{~"x", [1, 0, 0]}, {~"y", [0.8, 0.6, 0]}, {~"z", [0, 0.6, 0.8]}],
    lists:keysort(2, [{C, metric(Metric, Query, V)} || {C, V} <- Rows]).

metric(l2, A, B) -> math:sqrt(lists:sum([(X - Y) * (X - Y) || {X, Y} <- lists:zip(A, B)]));
metric(cosine, A, B) -> 1 - dot(A, B) / (norm(A) * norm(B));
metric(inner_product, A, B) -> -dot(A, B).

dot(A, B) -> lists:sum([X * Y || {X, Y} <- lists:zip(A, B)]).
norm(A) -> math:sqrt(dot(A, A)).

insert(Content, Embedding) ->
    insert_fields(#{content => Content, embedding => Embedding}).

insert_any(Content, Embedding) ->
    insert_fields(#{content => Content, any_embedding => Embedding}).

insert_fields(Params) ->
    CS = kura_changeset:cast(?SCHEMA, #{}, Params, maps:keys(Params)),
    {ok, Row} = kura_repo_worker:insert(?REPO, CS),
    Row.

reason({pgsql_error, #{message := Message}}) -> Message;
reason(Reason) -> io_lib:format("~p", [Reason]).

unwrap({ok, Plan}) -> Plan;
unwrap(Plan) -> Plan.

url() ->
    case os:getenv("KURA_PGVECTOR_URL") of
        false -> ?DEFAULT_URL;
        "" -> ?DEFAULT_URL;
        Url -> Url
    end.

repo_config(Url) ->
    case uri_string:parse(list_to_binary(Url)) of
        #{host := Host, port := Port, userinfo := UserInfo, path := <<"/", Database/binary>>} when
            is_binary(Host), is_integer(Port), is_binary(UserInfo)
        ->
            [User, Password] = binary:split(UserInfo, ~":"),
            #{
                pool => ?REPO,
                dialect => kura_dialect_pg,
                pool_module => kura_pool_minato,
                driver_module => kura_driver_minato,
                database => Database,
                hostname => Host,
                port => Port,
                username => User,
                password => Password,
                pool_size => 2
            }
    end.
