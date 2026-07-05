-module(kura_keyring_env_tests).
-include_lib("eunit/include/eunit.hrl").

keyring_env_test_() ->
    {foreach, fun() -> ok end, fun(_) -> application:unset_env(kura, encryption) end, [
        {"active/0 is undefined with no active id", fun() ->
            application:set_env(kura, encryption, #{keys => []}),
            ?assertEqual(undefined, kura_keyring_env:active())
        end},
        {"a 32-byte key resolves", fun() ->
            K = base64:encode(crypto:strong_rand_bytes(32)),
            application:set_env(kura, encryption, #{active => 1, keys => [{1, K}]}),
            ?assertMatch({1, <<_:32/binary>>}, kura_keyring_env:active()),
            ?assertMatch({ok, <<_:32/binary>>}, kura_keyring_env:fetch(1))
        end},
        {"a wrong-length key is treated as absent", fun() ->
            Short = base64:encode(crypto:strong_rand_bytes(31)),
            application:set_env(kura, encryption, #{active => 1, keys => [{1, Short}]}),
            ?assertEqual(undefined, kura_keyring_env:active()),
            ?assertEqual(error, kura_keyring_env:fetch(1))
        end},
        {"a non-base64 key is treated as absent", fun() ->
            application:set_env(kura, encryption, #{active => 1, keys => [{1, <<"!!!not-b64!!!">>}]}),
            ?assertEqual(undefined, kura_keyring_env:active()),
            ?assertEqual(error, kura_keyring_env:fetch(1))
        end},
        {"fetch/1 of an unknown id is error", fun() ->
            application:set_env(kura, encryption, #{keys => []}),
            ?assertEqual(error, kura_keyring_env:fetch(7))
        end}
    ]}.
