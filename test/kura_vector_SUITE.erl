-module(kura_vector_SUITE).
-moduledoc """
pgvector distance queries against a live server.

Needs a Postgres with the pgvector extension installed. CI runs the
`pgvector/pgvector` image on port 5555. Point the suite elsewhere with
`KURA_PGVECTOR_URL`, for example
`postgres://postgres:postgres@localhost:5556/postgres`. When the server
cannot create the `vector` extension the suite skips, or fails when the
`CI` environment variable is set.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    server_runs_a_patched_pgvector/1,
    vector_round_trips_through_a_changeset/1,
    nearest_rows_come_first_for_each_metric/1,
    distance_binds_alongside_where_limit_and_offset/1,
    cached_query_takes_each_new_vector/1,
    hnsw_index_serves_the_ordering/1,
    ordering_by_the_select_alias_uses_the_index/1,
    large_vector_round_trips_and_ranks/1,
    distance_threshold_keeps_the_rows_within_it/1,
    distance_threshold_binds_with_order_by_and_limit/1,
    distance_threshold_in_having/1,
    create_index_builds_an_hnsw_index_the_planner_uses/1,
    create_index_builds_an_ivfflat_index_the_planner_uses/1
]).

-define(REPO, kura_vector_test_repo).
-define(SCHEMA, kura_test_vector_schema).
-define(DEFAULT_URL, "postgres://postgres:root@localhost:5555/kura_test").

all() ->
    [
        server_runs_a_patched_pgvector,
        vector_round_trips_through_a_changeset,
        nearest_rows_come_first_for_each_metric,
        distance_binds_alongside_where_limit_and_offset,
        cached_query_takes_each_new_vector,
        hnsw_index_serves_the_ordering,
        ordering_by_the_select_alias_uses_the_index,
        large_vector_round_trips_and_ranks,
        distance_threshold_keeps_the_rows_within_it,
        distance_threshold_binds_with_order_by_and_limit,
        distance_threshold_in_having,
        create_index_builds_an_hnsw_index_the_planner_uses,
        create_index_builds_an_ivfflat_index_the_planner_uses
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
            Message = lists:flatten(
                io_lib:format("pgvector is not available on ~s:~p: ~s", [Host, Port, reason(Reason)])
            ),
            unavailable(os:getenv("CI"), Message)
    end.

%% CI runs the pgvector image, so a missing extension there is a broken
%% setup, not a reason to skip.
unavailable(CI, Message) when CI =:= false; CI =:= "" ->
    {skip, Message};
unavailable(_CI, Message) ->
    {fail, Message}.

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

%% CVE-2026-3172: parallel HNSW builds overflow before pgvector 0.8.2.
server_runs_a_patched_pgvector(_Config) ->
    {ok, [#{extversion := Version}]} = kura_repo_worker:query(
        ?REPO, ~"SELECT extversion FROM pg_extension WHERE extname = 'vector'", []
    ),
    Parts = [binary_to_integer(P) || P <- binary:split(Version, ~".", [global])],
    ?assert(Parts >= [0, 8, 2], Version).

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
    ?assertNotEqual(
        nomatch, binary:match(index_plan(Q), ~"Index Scan using kura_vector_items_embedding_cos")
    ).

%% Ordering by the alias of a selected distance sends the vector once,
%% and the planner still matches the ORDER BY to the index.
ordering_by_the_select_alias_uses_the_index(_Config) ->
    D = kura_query:distance(embedding, cosine, [1, 0, 0]),
    Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
    Q = kura_query:limit(kura_query:order_by(Q0, [{dist, asc}]), 2),
    {_SQL, Params} = kura_query_compiler:to_sql(?REPO, Q),
    ?assertEqual([~"[1.0,0.0,0.0]", 2], Params),
    ?assertNotEqual(
        nomatch, binary:match(index_plan(Q), ~"Index Scan using kura_vector_items_embedding_cos")
    ),
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

%% Each comparison against a threshold between the seed rows' distances,
%% for every metric, with a float and an integer threshold.
distance_threshold_keeps_the_rows_within_it(_Config) ->
    seed(),
    Query = [1, 0, 0],
    [
        begin
            Expected = expected(Metric, Query),
            [{_, Nearest}, {_, Middle}, {_, Farthest}] = Expected,
            Between = (Nearest + Middle) / 2,
            Within = fun(Op, Threshold) ->
                D = kura_query:distance(embedding, Metric, Query),
                Q = kura_query:where(kura_query:from(?SCHEMA), {D, Op, Threshold}),
                {ok, Rows} = kura_repo_worker:all(?REPO, kura_query:order_by(Q, [{D, asc}])),
                [C || #{content := C} <- Rows]
            end,
            All = [C || {C, _} <- Expected],
            [First | Rest] = All,
            ?assertEqual([First], Within('<', Between)),
            ?assertEqual([First], Within('<=', Between)),
            ?assertEqual(Rest, Within('>', Between)),
            ?assertEqual(Rest, Within('>=', Between)),
            ?assertEqual(All, Within('<=', Farthest + 1.0e-4)),
            ?assertEqual([], Within('<', Nearest - 1.0e-4)),
            ?assertEqual(All, Within('<', 2)),
            ?assertEqual([], Within('>', 2))
        end
     || Metric <- [l2, cosine, inner_product]
    ].

%% The threshold's vector and number bind between the select and the
%% order by, and the cached query takes new values for both.
distance_threshold_binds_with_order_by_and_limit(_Config) ->
    seed(),
    _ = insert(~"w", [-1, -1, 0]),
    Search = fun(Vec, Threshold, Limit, Offset) ->
        D = kura_query:distance(embedding, l2, Vec),
        Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}, {dist, D}]),
        Q1 = kura_query:where(Q0, {'and', [{content, '!=', ~"y"}, {D, '<', Threshold}]}),
        Q2 = kura_query:order_by(Q1, [{dist, asc}]),
        {ok, Rows} = kura_repo_worker:all(
            ?REPO, kura_query:offset(kura_query:limit(Q2, Limit), Offset)
        ),
        [{C, Dist} || #{content := C, dist := Dist} <- Rows]
    end,
    ?assertMatch([{~"x", +0.0}, {~"z", _}], Search([1, 0, 0], 1.5, 10, 0)),
    ?assertMatch([{~"z", _}], Search([1, 0, 0], 1.5, 1, 1)),
    ?assertMatch([{~"z", +0.0}, {~"x", _}], Search([0, 0.6, 0.8], 1.5, 2, 0)),
    ?assertMatch([{~"w", +0.0}], Search([-1, -1, 0], 1, 10, 0)),
    ?assertEqual(1, kura_query_cache:entry_count()).

distance_threshold_in_having(_Config) ->
    seed(),
    D = kura_query:distance(embedding, cosine, [1, 0, 0]),
    Q0 = kura_query:select_expr(kura_query:from(?SCHEMA), [{content, content}]),
    Q1 = kura_query:having(kura_query:group_by(Q0, [content, embedding]), {D, '<', 0.5}),
    {ok, Rows} = kura_repo_worker:all(?REPO, kura_query:order_by(Q1, [{content, asc}])),
    ?assertEqual([~"x", ~"y"], [C || #{content := C} <- Rows]).

create_index_builds_an_hnsw_index_the_planner_uses(_Config) ->
    seed(),
    run_operation(
        {create_index, ~"kura_vector_items", [embedding], #{
            using => hnsw,
            opclass => vector_cosine_ops,
            with => #{m => 16, ef_construction => 64},
            name => ~"kura_vector_items_embedding_hnsw_cos"
        }}
    ),
    ?assertEqual(
        [
            #{
                amname => ~"hnsw",
                opcname => ~"vector_cosine_ops",
                reloptions => ~"ef_construction=64,m=16"
            }
        ],
        index_catalog(~"kura_vector_items_embedding_hnsw_cos")
    ),
    D = kura_query:distance(embedding, cosine, [1, 0, 0]),
    Q = kura_query:limit(kura_query:order_by(kura_query:from(?SCHEMA), [{D, asc}]), 2),
    ?assertNotEqual(
        nomatch, binary:match(plan(Q), ~"Index Scan using kura_vector_items_embedding_hnsw_cos")
    ),
    {ok, Rows} = kura_repo_worker:all(?REPO, Q),
    ?assertEqual([~"x", ~"y"], [C || #{content := C} <- Rows]).

create_index_builds_an_ivfflat_index_the_planner_uses(_Config) ->
    seed(),
    run_operation(
        {create_index, ~"kura_vector_items", [embedding], #{
            using => ivfflat, opclass => vector_l2_ops, with => #{lists => 1}
        }}
    ),
    ?assertEqual(
        [#{amname => ~"ivfflat", opcname => ~"vector_l2_ops", reloptions => ~"lists=1"}],
        index_catalog(~"kura_vector_items_embedding_index")
    ),
    D = kura_query:distance(embedding, l2, [1, 0, 0]),
    Q = kura_query:limit(kura_query:order_by(kura_query:from(?SCHEMA), [{D, asc}]), 2),
    ?assertNotEqual(
        nomatch, binary:match(plan(Q), ~"Index Scan using kura_vector_items_embedding_index")
    ).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

run_operation(Op) ->
    {ok, _} = kura_repo_worker:query(?REPO, kura_migrator:compile_operation(?REPO, Op), []),
    ok.

index_catalog(Name) ->
    {ok, Rows} = kura_repo_worker:query(
        ?REPO,
        ~"""
        SELECT am.amname::text AS amname, opc.opcname::text AS opcname,
               array_to_string(i.reloptions, ',') AS reloptions
        FROM pg_catalog.pg_class i
        JOIN pg_catalog.pg_am am ON am.oid = i.relam
        JOIN pg_catalog.pg_index ix ON ix.indexrelid = i.oid
        JOIN pg_catalog.pg_opclass opc ON opc.oid = ix.indclass[0]
        WHERE i.relname = $1
        """,
        [Name]
    ),
    Rows.

%% Seeds the table, builds the HNSW index and returns the plan for Q with
%% sequential scans off, under the settings the guide recommends.
index_plan(Q) ->
    seed(),
    {ok, _} = kura_repo_worker:query(
        ?REPO,
        ~"CREATE INDEX kura_vector_items_embedding_cos ON kura_vector_items USING hnsw (embedding vector_cosine_ops)",
        []
    ),
    plan(Q).

plan(Q) ->
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
