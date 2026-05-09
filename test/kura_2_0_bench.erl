-module(kura_2_0_bench).

-export([run/0, run/1]).

-include("kura.hrl").

run() ->
    run(#{rows => 5000, concurrency => 8}).

run(Opts) ->
    Rows = maps:get(rows, Opts, 5000),
    Conc = maps:get(concurrency, Opts, 8),
    application:ensure_all_started(kura),
    kura_stress_repo:start(),
    setup_table(),
    Cases = [
        {seq_insert, fun() -> seq_insert(Rows) end},
        {bulk_insert, fun() -> reset_data(), bulk_insert(Rows) end},
        {seq_select, fun() -> seq_select(Rows) end},
        {par_insert, fun() -> reset_data(), par_insert(Conc, Rows div Conc) end},
        {par_select, fun() -> par_select(Conc, Rows div Conc) end}
    ],
    Results = [{Name, time_us(Fun)} || {Name, Fun} <- Cases],
    print_results(Rows, Conc, Results),
    Results.

time_us(Fun) ->
    Start = erlang:monotonic_time(microsecond),
    _ = Fun(),
    erlang:monotonic_time(microsecond) - Start.

setup_table() ->
    kura_stress_repo:query(
        "CREATE TABLE IF NOT EXISTS stress_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  counter INTEGER DEFAULT 0,"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    reset_data().

reset_data() ->
    kura_stress_repo:query("TRUNCATE stress_users RESTART IDENTITY CASCADE", []),
    ok.

seq_insert(N) ->
    [insert_one(seq, I) || I <- lists:seq(1, N)],
    ok.

bulk_insert(N) ->
    Entries = [
        #{
            name => name(<<"b">>, I),
            email => email(<<"b">>, I)
        }
     || I <- lists:seq(1, N)
    ],
    {ok, N} = kura_stress_repo:insert_all(kura_stress_schema, Entries),
    ok.

seq_select(N) ->
    Q = kura_query:limit(kura_query:from(kura_stress_schema), 1),
    [{ok, _} = kura_stress_repo:all(Q) || _ <- lists:seq(1, N)],
    ok.

par_insert(Conc, PerWorker) ->
    parallel(Conc, fun(W) ->
        [insert_one({par, W}, I) || I <- lists:seq(1, PerWorker)],
        ok
    end).

par_select(Conc, PerWorker) ->
    parallel(Conc, fun(_) ->
        Q = kura_query:limit(kura_query:from(kura_stress_schema), 1),
        [{ok, _} = kura_stress_repo:all(Q) || _ <- lists:seq(1, PerWorker)],
        ok
    end).

parallel(Conc, Fun) ->
    Self = self(),
    _ = [spawn(fun() -> Self ! {done, W, Fun(W)} end) || W <- lists:seq(1, Conc)],
    [
        receive
            {done, _, _} -> ok
        end
     || _ <- lists:seq(1, Conc)
    ].

insert_one(Tag, I) ->
    Params = #{
        <<"name">> => name(tag(Tag), I),
        <<"email">> => email(tag(Tag), I)
    },
    CS = kura_changeset:cast(kura_stress_schema, #{}, Params, [name, email]),
    {ok, _} = kura_stress_repo:insert(CS),
    ok.

tag(seq) -> <<"s">>;
tag({par, W}) -> iolist_to_binary([<<"p">>, integer_to_binary(W)]).

name(Prefix, I) ->
    iolist_to_binary([<<"u_">>, Prefix, <<"_">>, integer_to_binary(I)]).

email(Prefix, I) ->
    iolist_to_binary([
        <<"u_">>, Prefix, <<"_">>, integer_to_binary(I), <<"@b.test">>
    ]).

print_results(Rows, Conc, Results) ->
    io:format("~n==== kura bench (rows=~p, concurrency=~p) ====~n", [Rows, Conc]),
    [print_row(Name, Us, Rows) || {Name, Us} <- Results],
    io:format("~n").

print_row(Name, Us, Rows) ->
    Ms = Us / 1000,
    PerOp = Us / Rows,
    Throughput = Rows / (Us / 1_000_000),
    io:format(
        "~w\ttotal ~p ms\tper-op ~p us\t~p ops/sec~n",
        [Name, round(Ms), round(PerOp), round(Throughput)]
    ).
