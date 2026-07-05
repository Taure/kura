-module(kura_crypto_tests).
-include_lib("eunit/include/eunit.hrl").

crypto_test_() ->
    {setup, fun setup/0, fun teardown/1, [
        {"round-trips a value", fun t_round_trip/0},
        {"frame carries version and key id", fun t_frame/0},
        {"tampered ciphertext raises bad_tag", fun t_tamper/0},
        {"short/malformed frame raises", fun t_malformed/0},
        {"unknown version raises", fun t_version/0},
        {"no active key raises", fun t_no_active/0},
        {"unknown key id raises", fun t_unknown_key/0},
        {"rotation: old ciphertext decrypts after active flip", fun t_rotation/0}
    ]}.

setup() ->
    K1 = base64:encode(crypto:strong_rand_bytes(32)),
    K2 = base64:encode(crypto:strong_rand_bytes(32)),
    application:set_env(kura, encryption, #{active => 1, keys => [{1, K1}, {2, K2}]}),
    ok.

teardown(_) ->
    application:unset_env(kura, encryption),
    ok.

t_round_trip() ->
    P = <<"top secret">>,
    ?assertEqual(P, kura_crypto:decrypt(kura_crypto:encrypt(P))).

t_frame() ->
    CT = kura_crypto:encrypt(<<"x">>),
    %% version=1, key_id=1 (active), then 12-byte nonce + 16-byte tag + 1-byte ct
    ?assertEqual(1, binary:at(CT, 0)),
    ?assertEqual(1, binary:at(CT, 1)),
    ?assertEqual(2 + 12 + 16 + 1, byte_size(CT)).

t_tamper() ->
    CT = kura_crypto:encrypt(<<"x">>),
    Last = binary:last(CT),
    Bad = <<(binary:part(CT, 0, byte_size(CT) - 1))/binary, (Last bxor 255):8>>,
    ?assertError({kura_crypto, {bad_tag, _}}, kura_crypto:decrypt(Bad)).

t_malformed() ->
    ?assertError({kura_crypto, malformed_frame}, kura_crypto:decrypt(<<1, 2, 3>>)),
    ?assertError({kura_crypto, malformed_frame}, kura_crypto:decrypt(<<>>)).

t_version() ->
    <<_V:8, Rest/binary>> = kura_crypto:encrypt(<<"x">>),
    ?assertError(
        {kura_crypto, {unsupported_version, 9}}, kura_crypto:decrypt(<<9, Rest/binary>>)
    ).

t_no_active() ->
    application:set_env(kura, encryption, #{
        keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    try
        ?assertError({kura_crypto, no_active_key}, kura_crypto:encrypt(<<"x">>))
    after
        setup()
    end.

t_unknown_key() ->
    CT = kura_crypto:encrypt(<<"x">>),
    application:set_env(kura, encryption, #{
        active => 2, keys => [{2, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    try
        ?assertError({kura_crypto, {unknown_key_id, 1}}, kura_crypto:decrypt(CT))
    after
        setup()
    end.

t_rotation() ->
    CT1 = kura_crypto:encrypt(<<"old">>),
    Cfg = application:get_env(kura, encryption, #{}),
    application:set_env(kura, encryption, Cfg#{active => 2}),
    try
        CT2 = kura_crypto:encrypt(<<"new">>),
        ?assertEqual(<<"old">>, kura_crypto:decrypt(CT1)),
        ?assertEqual(<<"new">>, kura_crypto:decrypt(CT2)),
        ?assertEqual(2, binary:at(CT2, 1))
    after
        setup()
    end.
