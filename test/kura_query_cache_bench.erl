-module(kura_query_cache_bench).
-moduledoc """
Compile-path benchmark for `kura_query_compiler:to_sql_cached/2` (kura#163).
No database: the cache only saves query compilation, so that is all this
times. Each figure is the best of six runs, each from an empty cache, so
a workload of distinct values is measured as the misses it really is.
Skipped unless `KURA_BENCH=1`:

    KURA_BENCH=1 rebar3 eunit --module=kura_query_cache_bench
""".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_query_cache_bench_repo).
-define(CALLS, 20000).

bench_test_() ->
    case os:getenv("KURA_BENCH") of
        "1" -> {timeout, 300, fun run/0};
        _ -> []
    end.

run() ->
    application:set_env(kura, repos, #{?REPO => #{dialect => kura_dialect_pg}}),
    kura_query_cache:init(),
    try
        report(measure_all())
    after
        kura_query_cache:flush(),
        application:unset_env(kura, repos)
    end.

measure_all() ->
    Workloads = [
        {"get by id, distinct ids", fun get_by_id/1},
        {"get by id, one id", fun(_) -> get_by_id(1) end},
        {"filtered page, distinct values", fun filtered_page/1},
        {"filtered page, one value set", fun(_) -> filtered_page(7) end}
    ],
    [measure(Name, Fun) || {Name, Fun} <- Workloads].

report(Results) ->
    io:format(user, "~n~-34s ~10s ~10s ~10s~n", ["workload", "cached us", "uncached", "entries"]),
    [
        io:format(user, "~-34s ~10.2f ~10.2f ~10b~n", [Name, Cached, Uncached, Entries])
     || {Name, Cached, Uncached, Entries} <- Results
    ],
    ok.

measure(Name, Fun) ->
    Queries = [Fun(I) || I <- lists:seq(1, ?CALLS)],
    Cached = time_per_call(fun(Q) -> kura_query_compiler:to_sql_cached(?REPO, Q) end, Queries),
    Entries = ets:info(kura_query_cache, size),
    Uncached = time_per_call(fun(Q) -> kura_query_compiler:to_sql(?REPO, Q) end, Queries),
    {Name, Cached, Uncached, Entries}.

-spec time_per_call(fun((#kura_query{}) -> term()), [#kura_query{}]) -> float().
time_per_call(Compile, Queries) ->
    best_of(5, Compile, Queries, run_once(Compile, Queries)) / length(Queries).

-spec best_of(
    non_neg_integer(), fun((#kura_query{}) -> term()), [#kura_query{}], non_neg_integer()
) ->
    non_neg_integer().
best_of(0, _Compile, _Queries, Best) ->
    Best;
best_of(N, Compile, Queries, Best) ->
    best_of(N - 1, Compile, Queries, min(Best, run_once(Compile, Queries))).

-spec run_once(fun((#kura_query{}) -> term()), [#kura_query{}]) -> non_neg_integer().
run_once(Compile, Queries) ->
    kura_query_cache:flush(),
    {Micros, _} = timer:tc(fun() -> [Compile(Q) || Q <- Queries] end),
    Micros.

get_by_id(Id) ->
    kura_query:limit(kura_query:where(kura_query:from(kura_bench_schema_user), {id, Id}), 1).

filtered_page(I) ->
    Q0 = kura_query:from(kura_bench_schema_user),
    Q1 = kura_query:where(Q0, {active, true}),
    Q2 = kura_query:where(Q1, {age, '>', I rem 90}),
    Q3 = kura_query:where(Q2, {role, in, [admin, moderator]}),
    Q4 = kura_query:where(Q3, {email, like, iolist_to_binary([~"%", integer_to_binary(I), ~"%"])}),
    Q5 = kura_query:order_by(Q4, [{inserted_at, desc}]),
    kura_query:offset(kura_query:limit(Q5, 20), (I rem 50) * 20).
