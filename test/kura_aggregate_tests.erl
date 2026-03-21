-module(kura_aggregate_tests).
-moduledoc "Integration tests for kura_repo_worker aggregate functions.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

aggregate_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% count
            {"count all rows", fun t_count_all/0},
            {"count with where", fun t_count_where/0},
            {"count empty table", fun t_count_empty/0},
            {"count/2 shorthand", fun t_count_shorthand/0},

            %% sum
            {"sum of integer field", fun t_sum_int/0},
            {"sum of float field", fun t_sum_float/0},
            {"sum with where filter", fun t_sum_where/0},
            {"sum empty result returns nil", fun t_sum_empty/0},
            {"sum with default", fun t_sum_default/0},

            %% avg
            {"avg of integer field", fun t_avg/0},
            {"avg empty with default", fun t_avg_empty_default/0},

            %% min / max
            {"min of integer field", fun t_min/0},
            {"max of integer field", fun t_max/0},
            {"min of string field", fun t_min_string/0},
            {"max of string field", fun t_max_string/0},

            %% edge cases
            {"aggregate on complex query", fun t_aggregate_complex_query/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS agg_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  category VARCHAR(255),"
        "  score DOUBLE PRECISION,"
        "  count INTEGER,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query("TRUNCATE agg_items", []),
    seed_data(),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS agg_items CASCADE", []),
    ok.

seed_data() ->
    Entries = [
        #{name => ~"alice", category => ~"a", score => 10.0, count => 1},
        #{name => ~"bob", category => ~"a", score => 20.0, count => 2},
        #{name => ~"carol", category => ~"b", score => 30.0, count => 3},
        #{name => ~"dave", category => ~"b", score => 40.0, count => 4},
        #{name => ~"eve", category => ~"c", score => 50.0, count => 5}
    ],
    {ok, 5} = kura_repo_worker:insert_all(kura_test_repo, kura_test_agg_schema, Entries).

%%----------------------------------------------------------------------
%% count
%%----------------------------------------------------------------------

t_count_all() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, 5} = kura_repo_worker:aggregate(kura_test_repo, Q, count).

t_count_where() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"a"}),
    {ok, 2} = kura_repo_worker:aggregate(kura_test_repo, Q, count).

t_count_empty() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"nonexistent"}),
    {ok, 0} = kura_repo_worker:aggregate(kura_test_repo, Q, count).

t_count_shorthand() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, 5} = kura_repo_worker:count(kura_test_repo, Q).

%%----------------------------------------------------------------------
%% sum
%%----------------------------------------------------------------------

t_sum_int() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, 15} = kura_repo_worker:aggregate(kura_test_repo, Q, {sum, count}).

t_sum_float() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, Sum} = kura_repo_worker:aggregate(kura_test_repo, Q, {sum, score}),
    ?assert(is_number(Sum)),
    ?assert(abs(to_float(Sum) - 150.0) < 0.01).

t_sum_where() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"b"}),
    {ok, 7} = kura_repo_worker:aggregate(kura_test_repo, Q, {sum, count}).

t_sum_empty() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"none"}),
    {ok, null} = kura_repo_worker:aggregate(kura_test_repo, Q, {sum, count}).

t_sum_default() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"none"}),
    {ok, 0} = kura_repo_worker:aggregate(kura_test_repo, Q, {sum, count}, 0).

%%----------------------------------------------------------------------
%% avg
%%----------------------------------------------------------------------

t_avg() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, Avg} = kura_repo_worker:aggregate(kura_test_repo, Q, {avg, count}),
    ?assert(is_number(Avg)),
    ?assert(abs(to_float(Avg) - 3.0) < 0.01).

t_avg_empty_default() ->
    Q = kura_query:where(kura_query:from(kura_test_agg_schema), {category, ~"none"}),
    {ok, +0.0} = kura_repo_worker:aggregate(kura_test_repo, Q, {avg, score}, +0.0).

%%----------------------------------------------------------------------
%% min / max
%%----------------------------------------------------------------------

t_min() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, 1} = kura_repo_worker:aggregate(kura_test_repo, Q, {min, count}).

t_max() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, 5} = kura_repo_worker:aggregate(kura_test_repo, Q, {max, count}).

t_min_string() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, ~"alice"} = kura_repo_worker:aggregate(kura_test_repo, Q, {min, name}).

t_max_string() ->
    Q = kura_query:from(kura_test_agg_schema),
    {ok, ~"eve"} = kura_repo_worker:aggregate(kura_test_repo, Q, {max, name}).

%%----------------------------------------------------------------------
%% complex query
%%----------------------------------------------------------------------

t_aggregate_complex_query() ->
    Q0 = kura_query:from(kura_test_agg_schema),
    Q1 = kura_query:where(Q0, {count, '>', 1}),
    Q2 = kura_query:where(Q1, {count, '<', 5}),
    {ok, 3} = kura_repo_worker:aggregate(kura_test_repo, Q2, count),
    {ok, 9} = kura_repo_worker:aggregate(kura_test_repo, Q2, {sum, count}),
    {ok, Avg} = kura_repo_worker:aggregate(kura_test_repo, Q2, {avg, score}),
    ?assert(is_number(Avg)),
    ?assert(abs(to_float(Avg) - 30.0) < 0.01).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

-spec to_float(term()) -> float().
to_float(V) when is_float(V) -> V;
to_float(V) when is_integer(V) -> erlang:float(V).
