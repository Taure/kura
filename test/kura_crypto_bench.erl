-module(kura_crypto_bench).
-moduledoc """
Per-value cost of field encryption (kura#137): `kura_crypto` v1 against v2,
which derives an HKDF subkey on every call, deterministic v2 (two subkeys,
an HMAC and the IV recheck on decrypt) against randomized v2, a
deterministic lookup under one and two keys, and `kura_db:load_row/2` on a
schema with no encrypted fields, which the v2 seams also touch. No database.
Each figure is the best of six runs. Skipped unless `KURA_BENCH=1`:

    KURA_BENCH=1 rebar3 eunit --module=kura_crypto_bench
""".

-include_lib("eunit/include/eunit.hrl").

-define(CALLS, 20000).
-define(CTX, {~"secrets", ~"ssn"}).
-define(PLAIN, ~"123-45-6789").

bench_test_() ->
    case os:getenv("KURA_BENCH") of
        "1" -> {timeout, 300, fun run/0};
        _ -> []
    end.

run() ->
    application:set_env(kura, encryption, #{
        active => 1,
        deterministic_active => 1,
        keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    try
        report(measure_all())
    after
        application:unset_env(kura, encryption),
        application:unset_env(kura, encryption_write_format)
    end.

measure_all() ->
    V1 = kura_crypto:encrypt(?PLAIN),
    application:set_env(kura, encryption_write_format, v2),
    V2 = kura_crypto:encrypt(?PLAIN, ?CTX),
    Det = kura_crypto:encrypt(?PLAIN, ?CTX, deterministic),
    OneKey = time_per_call(lookup),
    {ok, #{keys := Keys} = Encryption} = application:get_env(kura, encryption),
    application:set_env(kura, encryption, Encryption#{
        deterministic_keys => [2],
        keys => Keys ++ [{2, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    TwoKeys = time_per_call(lookup),
    V2Results = [
        {"encrypt v2", time_per_call(encrypt_v2)},
        {"decrypt v2", time_per_call({decrypt_v2, V2})},
        {"encrypt v2 deterministic", time_per_call(encrypt_det)},
        {"decrypt v2 deterministic", time_per_call({decrypt_det, Det})},
        {"lookup, one key", OneKey},
        {"lookup, two keys", TwoKeys}
    ],
    application:set_env(kura, encryption_write_format, v1),
    V2Results ++
        [
            {"encrypt v1", time_per_call(encrypt_v1)},
            {"decrypt v1", time_per_call({decrypt_v1, V1})},
            {"load_row, no encrypted fields", time_per_call({load_row, plain_row()})}
        ].

report(Results) ->
    io:format(user, "~n~-32s ~10s~n", ["operation", "us/call"]),
    [io:format(user, "~-32s ~10.2f~n", [Name, Micros]) || {Name, Micros} <- Results],
    ok.

time_per_call(Op) ->
    best_of(5, Op, run_once(Op)) / ?CALLS.

best_of(0, _Op, Best) ->
    Best;
best_of(N, Op, Best) ->
    best_of(N - 1, Op, min(Best, run_once(Op))).

run_once(Op) ->
    {Micros, _} = timer:tc(fun() -> [call(Op) || _ <- lists:seq(1, ?CALLS)] end),
    Micros.

call(encrypt_v1) ->
    kura_crypto:encrypt(?PLAIN);
call(encrypt_v2) ->
    kura_crypto:encrypt(?PLAIN, ?CTX);
call(encrypt_det) ->
    kura_crypto:encrypt(?PLAIN, ?CTX, deterministic);
call({decrypt_det, Frame}) ->
    kura_crypto:decrypt(Frame, ?CTX, #{mode => deterministic, legacy => reject});
call(lookup) ->
    kura_crypto:lookup([?PLAIN], ?CTX);
call({decrypt_v1, Frame}) ->
    kura_crypto:decrypt(Frame);
call({decrypt_v2, Frame}) ->
    kura_crypto:decrypt(Frame, ?CTX);
call({load_row, Row}) ->
    kura_db:load_row(kura_test_schema, Row).

plain_row() ->
    #{
        id => 1,
        name => ~"Alice",
        email => ~"alice@example.com",
        age => 30,
        active => true,
        role => ~"user",
        score => 1.5,
        metadata => ~"{\"a\":1}",
        status => ~"active",
        tags => [~"a", ~"b"],
        lock_version => 0,
        inserted_at => {{2026, 1, 1}, {0, 0, 0}},
        updated_at => {{2026, 1, 1}, {0, 0, 0}}
    }.
