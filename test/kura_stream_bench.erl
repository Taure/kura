-module(kura_stream_bench).
-moduledoc """
Streaming benchmark — compares kura_stream (server-side cursors) vs
kura_repo_worker:all (load everything) for large result sets.

Run manually:
    docker compose up -d
    rebar3 eunit --module kura_stream_bench
""".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, t_summary/0}).
-eqwalizer({nowarn_function, store_result/2}).

-define(TABLE, ~"stream_bench").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

stream_bench_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"verify 1M rows exist", fun verify_data/0},
            {"stream 1M rows (batch 500)", {timeout, 600, fun t_stream_500/0}},
            {"stream 1M rows (batch 1000)", {timeout, 600, fun t_stream_1000/0}},
            {"stream 1M rows (batch 5000)", {timeout, 600, fun t_stream_5000/0}},
            {"stream 1M rows (batch 10000)", {timeout, 600, fun t_stream_10000/0}},
            {"stream 1M rows (batch 50000)", {timeout, 600, fun t_stream_50000/0}},
            {"all() load 1M rows", {timeout, 600, fun t_all_100k/0}},
            {"stream 100k — memory stays flat", {timeout, 600, fun t_stream_memory/0}},
            {"all 100k — memory spike", {timeout, 600, fun t_all_memory/0}},
            {"summary", fun t_summary/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_stress_repo:start(),
    ok.

teardown(_) ->
    ok.

%%----------------------------------------------------------------------
%% Seed
%%----------------------------------------------------------------------

verify_data() ->
    {ok, [#{count := Count}]} = kura_stress_repo:query(
        iolist_to_binary([~"SELECT count(*)::bigint AS count FROM ", ?TABLE]), []
    ),
    fmtprint("~n  Table has ~B rows~n", [Count]),
    ?assertEqual(1_000_000, Count).

%%----------------------------------------------------------------------
%% Stream benchmarks
%%----------------------------------------------------------------------

t_stream_500() ->
    run_stream_bench(500).

t_stream_1000() ->
    run_stream_bench(1000).

t_stream_5000() ->
    run_stream_bench(5000).

t_stream_10000() ->
    run_stream_bench(10000).

t_stream_50000() ->
    run_stream_bench(50000).

run_stream_bench(BatchSize) ->
    Q = kura_query:from(stream_bench_schema),
    Counter = counters:new(1, []),
    erlang:garbage_collect(),
    T0 = erlang:monotonic_time(microsecond),
    ok = kura_stream:stream(
        kura_stress_repo,
        Q,
        fun(Batch) ->
            counters:add(Counter, 1, length(Batch)),
            ok
        end,
        #{batch_size => BatchSize}
    ),
    T1 = erlang:monotonic_time(microsecond),
    Total = counters:get(Counter, 1),
    DurationMs = (T1 - T0) / 1000,
    Throughput = Total / (DurationMs / 1000),
    fmtprint("~n  stream(batch_size=~B): ~B rows in ~sms (~s rows/sec)~n", [
        BatchSize, Total, DurationMs, Throughput
    ]),
    store_result({stream, BatchSize}, #{
        rows => Total, duration_ms => DurationMs, throughput => Throughput
    }),
    ?assertEqual(1_000_000, Total).

%%----------------------------------------------------------------------
%% all() benchmark
%%----------------------------------------------------------------------

t_all_100k() ->
    Q = kura_query:from(stream_bench_schema),
    erlang:garbage_collect(),
    T0 = erlang:monotonic_time(microsecond),
    {ok, Rows} = kura_repo_worker:all(kura_stress_repo, Q),
    T1 = erlang:monotonic_time(microsecond),
    Total = length(Rows),
    DurationMs = (T1 - T0) / 1000,
    Throughput = Total / (DurationMs / 1000),
    fmtprint("~n  all(): ~B rows in ~sms (~s rows/sec)~n", [
        Total, DurationMs, Throughput
    ]),
    store_result(all, #{
        rows => Total, duration_ms => DurationMs, throughput => Throughput
    }),
    ?assertEqual(1_000_000, Total).

%%----------------------------------------------------------------------
%% Memory benchmarks
%%----------------------------------------------------------------------

t_stream_memory() ->
    Q = kura_query:from(stream_bench_schema),
    erlang:garbage_collect(),
    {memory, MemBefore} = process_info(self(), memory),
    PeakRef = counters:new(1, []),
    counters:put(PeakRef, 1, MemBefore),
    ok = kura_stream:stream(
        kura_stress_repo,
        Q,
        fun(_Batch) ->
            {memory, MemNow} = process_info(self(), memory),
            case MemNow > counters:get(PeakRef, 1) of
                true -> counters:put(PeakRef, 1, MemNow);
                false -> ok
            end,
            ok
        end,
        #{batch_size => 1000}
    ),
    erlang:garbage_collect(),
    {memory, MemAfter} = process_info(self(), memory),
    PeakMem = counters:get(PeakRef, 1),
    PeakDeltaMB = (PeakMem - MemBefore) / (1024 * 1024),
    AfterDeltaMB = (MemAfter - MemBefore) / (1024 * 1024),
    fmtprint("~n  stream memory: peak +~s MB, after GC +~s MB~n", [
        PeakDeltaMB, AfterDeltaMB
    ]),
    store_result(stream_memory, #{
        before_mb => MemBefore / (1024 * 1024),
        peak_mb => PeakMem / (1024 * 1024),
        peak_delta_mb => PeakDeltaMB,
        after_delta_mb => AfterDeltaMB
    }),
    %% Stream should use very little memory — well under 50MB for 1M rows
    ?assert(PeakDeltaMB < 50).

t_all_memory() ->
    Q = kura_query:from(stream_bench_schema),
    erlang:garbage_collect(),
    {memory, MemBefore} = process_info(self(), memory),
    {ok, Rows} = kura_repo_worker:all(kura_stress_repo, Q),
    {memory, MemPeak} = process_info(self(), memory),
    _ = length(Rows),
    erlang:garbage_collect(),
    {memory, MemAfter} = process_info(self(), memory),
    PeakDeltaMB = (MemPeak - MemBefore) / (1024 * 1024),
    AfterDeltaMB = (MemAfter - MemBefore) / (1024 * 1024),
    fmtprint("~n  all() memory: peak +~s MB, after GC +~s MB~n", [
        PeakDeltaMB, AfterDeltaMB
    ]),
    store_result(all_memory, #{
        before_mb => MemBefore / (1024 * 1024),
        peak_mb => MemPeak / (1024 * 1024),
        peak_delta_mb => PeakDeltaMB,
        after_delta_mb => AfterDeltaMB
    }),
    ok.

%%----------------------------------------------------------------------
%% Summary
%%----------------------------------------------------------------------

t_summary() ->
    fmtprint("~n~n========================================~n", []),
    fmtprint("  STREAMING BENCHMARK RESULTS (1M rows)~n", []),
    fmtprint("========================================~n~n", []),

    lists:foreach(
        fun({Key, Val}) ->
            case Key of
                {stream, BS} ->
                    fmtprint("  stream(batch=~B): ~sms, ~s rows/sec~n", [
                        BS,
                        maps:get(duration_ms, Val),
                        maps:get(throughput, Val)
                    ]);
                all ->
                    fmtprint("  all():            ~sms, ~s rows/sec~n", [
                        maps:get(duration_ms, Val),
                        maps:get(throughput, Val)
                    ]);
                stream_memory ->
                    fmtprint("~n  stream memory:    peak +~s MB~n", [
                        maps:get(peak_delta_mb, Val)
                    ]);
                all_memory ->
                    fmtprint("  all() memory:     peak +~s MB~n", [
                        maps:get(peak_delta_mb, Val)
                    ]);
                _ ->
                    ok
            end
        end,
        get_results()
    ),

    StreamMem = maps:get(peak_delta_mb, get_result(stream_memory)),
    AllMem = maps:get(peak_delta_mb, get_result(all_memory)),
    case AllMem > 0.0 of
        true ->
            fmtprint("~n  Memory savings:   ~sx less memory with streaming~n", [
                AllMem / max(StreamMem, 0.01)
            ]);
        false ->
            ok
    end,
    fmtprint("========================================~n~n", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

store_result(Key, Val) ->
    Results =
        case get(bench_results) of
            undefined -> [];
            R -> R
        end,
    put(bench_results, Results ++ [{Key, Val}]).

get_results() ->
    case get(bench_results) of
        undefined -> [];
        R -> R
    end.

get_result(Key) ->
    {Key, Val} = lists:keyfind(Key, 1, get_results()),
    Val.

fmtprint(Fmt, Args) ->
    Converted = convert_floats(Args),
    Str = lists:flatten(io_lib:format(Fmt, Converted)),
    io:put_chars(standard_error, Str).

convert_floats([]) ->
    [];
convert_floats([V | Rest]) when is_float(V) ->
    [float_to_list(V, [{decimals, 1}]) | convert_floats(Rest)];
convert_floats([V | Rest]) ->
    [V | convert_floats(Rest)].
