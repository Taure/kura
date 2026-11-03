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
        {"deterministic_active/0 resolves its own id", fun() ->
            K1 = base64:encode(crypto:strong_rand_bytes(32)),
            K2 = base64:encode(crypto:strong_rand_bytes(32)),
            application:set_env(kura, encryption, #{
                active => 1, deterministic_active => 2, keys => [{1, K1}, {2, K2}]
            }),
            ?assertEqual({2, base64:decode(K2)}, kura_keyring_env:deterministic_active()),
            ?assertEqual({1, base64:decode(K1)}, kura_keyring_env:active())
        end},
        {"deterministic_active/0 is undefined without an id or a valid key", fun() ->
            Short = base64:encode(crypto:strong_rand_bytes(31)),
            application:set_env(kura, encryption, #{active => 1, keys => [{1, Short}]}),
            ?assertEqual(undefined, kura_keyring_env:deterministic_active()),
            application:set_env(kura, encryption, #{deterministic_active => 1, keys => [{1, Short}]}),
            ?assertEqual(undefined, kura_keyring_env:deterministic_active()),
            application:set_env(kura, encryption, #{deterministic_active => 3, keys => []}),
            ?assertEqual(undefined, kura_keyring_env:deterministic_active())
        end},
        {"deterministic_key_ids/0 returns deterministic_keys", fun() ->
            K = base64:encode(crypto:strong_rand_bytes(32)),
            application:set_env(kura, encryption, #{
                active => 3,
                deterministic_active => 1,
                deterministic_keys => [255, 0, 1],
                keys => [{1, K}, {3, K}, {0, K}, {255, K}]
            }),
            ?assertEqual([255, 0, 1], kura_keyring_env:deterministic_key_ids()),
            application:set_env(kura, encryption, #{deterministic_active => 1, keys => [{1, K}]}),
            ?assertEqual([], kura_keyring_env:deterministic_key_ids()),
            application:set_env(kura, encryption, #{}),
            ?assertEqual([], kura_keyring_env:deterministic_key_ids())
        end},
        {"deterministic_key_ids/0 raises on an id not in the ring", fun() ->
            K = base64:encode(crypto:strong_rand_bytes(32)),
            application:set_env(kura, encryption, #{deterministic_keys => [1, 3], keys => [{1, K}]}),
            ?assertError(
                {kura_crypto, {unknown_key_id, 3}}, kura_keyring_env:deterministic_key_ids()
            )
        end},
        {"a lookup raises on a deterministic key that does not decode", fun() ->
            K = base64:encode(crypto:strong_rand_bytes(32)),
            Short = base64:encode(crypto:strong_rand_bytes(31)),
            application:set_env(kura, encryption, #{
                deterministic_active => 1, deterministic_keys => [2], keys => [{1, K}, {2, Short}]
            }),
            ?assertEqual([2], kura_keyring_env:deterministic_key_ids()),
            ?assertError(
                {kura_crypto, {unknown_key_id, 2}}, kura_crypto:lookup([~"x"], {~"t", ~"c"})
            )
        end},
        {"deterministic_key_ids/0 raises on a malformed deterministic_keys", fun() ->
            K = base64:encode(crypto:strong_rand_bytes(32)),
            [
                begin
                    application:set_env(kura, encryption, #{
                        deterministic_keys => Bad, keys => [{1, K}, {256, K}, {-1, K}]
                    }),
                    ?assertError(
                        {kura_crypto, invalid_deterministic_keys},
                        kura_keyring_env:deterministic_key_ids()
                    )
                end
             || Bad <- [1, [1, 256], [-1], [one], improper(1, 2), #{1 => 1}]
            ]
        end},
        {"a malformed config raises without key material", fun() ->
            B64 = base64:encode(crypto:strong_rand_bytes(32)),
            Raw = base64:decode(B64),
            Configs = [
                [{active, 1}, {keys, [{1, B64}]}],
                #{
                    active => 1,
                    deterministic_active => 1,
                    deterministic_keys => [1],
                    keys => #{1 => B64}
                },
                #{
                    active => 1,
                    deterministic_active => 1,
                    deterministic_keys => [1],
                    keys => improper({2, B64}, B64)
                }
            ],
            Calls = [
                fun kura_keyring_env:active/0,
                fun kura_keyring_env:deterministic_active/0,
                fun kura_keyring_env:deterministic_key_ids/0,
                fun() -> kura_keyring_env:fetch(1) end
            ],
            [
                begin
                    application:set_env(kura, encryption, Config),
                    try Call() of
                        Result -> ?assertEqual(raised, Result)
                    catch
                        Class:Reason:Stack ->
                            ?assertEqual({kura_crypto, invalid_keyring_config}, Reason),
                            Bin = term_to_binary({Class, Reason, Stack}),
                            ?assertEqual(nomatch, binary:match(Bin, B64)),
                            ?assertEqual(nomatch, binary:match(Bin, Raw))
                    end
                end
             || Config <- Configs, Call <- Calls
            ]
        end},
        {"fetch/1 of an unknown id is error", fun() ->
            application:set_env(kura, encryption, #{keys => []}),
            ?assertEqual(error, kura_keyring_env:fetch(7))
        end}
    ]}.

-spec improper(term(), term()) -> dynamic().
improper(Head, Tail) ->
    [Head | ill_typed(Tail)].

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.
