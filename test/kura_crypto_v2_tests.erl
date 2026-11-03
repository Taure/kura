-module(kura_crypto_v2_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CTX, {~"secrets", ~"ssn"}).

%% Computed with an independent HKDF-SHA256 and AES-256-GCM implementation
%% (Python hmac and the cryptography package) from the layout in the
%% kura_crypto moduledoc: root key 1 is the bytes 1..32.
-define(SSN_SUBKEY, ~"38d15dc8071980bbbc667608ba83e12c3944cf4606b90d9cbd4c224b0172e6aa").
-define(NICKNAME_SUBKEY, ~"315b162e5c2e8070b73d7387d6a743f78ebb8980f159ecdc0545015bf7607be6").
-define(LONG_COLUMN_SUBKEY, ~"d8686599f5621c48cfb6cda888b54cb8ab738eab781569f092af1834fcf985c3").
%% Key id 1, IV the bytes 16#a0..16#ab, plaintext "123-45-6789", context ?CTX.
-define(PINNED_FRAME,
    ~"02000001a0a1a2a3a4a5a6a7a8a9aaab6d60a388e8bee23d2e36218f7a4b7c5154ffc23d867aee8524a206"
).

v2_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"subkeys match the pinned vectors", fun t_subkeys/0},
        {"the pinned v2 frame decrypts", fun t_pinned_frame/0},
        {"the pinned v2 frame does not decrypt elsewhere", fun t_pinned_frame_elsewhere/0},
        {"writes v1 by default, even with a context", fun t_default_v1/0},
        {"writes v2 when configured", fun t_writes_v2/0},
        {"v2 frame layout", fun t_v2_layout/0},
        {"v2 subkey is not the root key", fun t_not_root_key/0},
        {"v2 IVs are fresh", fun t_fresh_iv/0},
        {"v2 frame without a context raises", fun t_no_context_decrypt/0},
        {"writing v2 without a context raises", fun t_no_context_encrypt/0},
        {"a non-binary plaintext raises malformed_plaintext", fun t_non_binary_plaintext/0},
        {"an invalid write format raises", fun t_invalid_write_format/0},
        {"an invalid context raises", fun t_invalid_context/0},
        {"the context is length-prefixed", fun t_unambiguous_context/0},
        {"another column or table raises", fun t_wrong_context/0},
        {"mode swap raises", fun t_mode_swap/0},
        {"key id swap raises", fun t_key_id_swap/0},
        {"key id above 255 raises without a lookup", fun t_wide_key_id/0},
        {"key id 255 round trips", fun t_key_id_255/0},
        {"key ids 0 to 255 are the only active ids", fun t_active_key_id_range/0},
        {"truncated v2 frame raises", fun t_truncated/0},
        {"unknown versions raise", fun t_unknown_version/0},
        {"rotation reads old v2 frames", fun t_rotation/0},
        {"a keyring key that is not 32 bytes raises", fun t_bad_keyring/0},
        {"errors carry no key material or plaintext", fun t_no_secrets_in_errors/0}
    ]}.

setup() ->
    application:set_env(kura, encryption, #{active => 1, keys => keys()}),
    application:unset_env(kura, encryption_write_format),
    application:unset_env(kura, keyring).

teardown(_) ->
    application:unset_env(kura, encryption),
    application:unset_env(kura, encryption_write_format),
    application:unset_env(kura, keyring).

keys() ->
    [{1, base64:encode(root_key(1))}, {2, base64:encode(root_key(2))}].

root_key(1) -> list_to_binary(lists:seq(1, 32));
root_key(2) -> list_to_binary(lists:seq(101, 132)).

write_v2() ->
    application:set_env(kura, encryption_write_format, v2).

t_subkeys() ->
    ?assertEqual(hex(?SSN_SUBKEY), kura_crypto:subkey(root_key(1), ?CTX)),
    ?assertEqual(hex(?NICKNAME_SUBKEY), kura_crypto:subkey(root_key(1), {~"secrets", ~"nickname"})),
    ?assertEqual(
        hex(?LONG_COLUMN_SUBKEY),
        kura_crypto:subkey(root_key(1), {~"secrets", binary:copy(~"c", 300)})
    ).

t_pinned_frame() ->
    ?assertEqual(~"123-45-6789", kura_crypto:decrypt(hex(?PINNED_FRAME), ?CTX)).

t_pinned_frame_elsewhere() ->
    Frame = hex(?PINNED_FRAME),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(Frame, {~"secrets", ~"ssn2"})),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(Frame, {~"secret", ~"ssn"})),
    ?assertError({kura_crypto, no_context}, kura_crypto:decrypt(Frame)).

t_default_v1() ->
    ?assertEqual(v1, kura_crypto:write_format()),
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    ?assertMatch(<<1, 1, _:12/binary, _:16/binary, _:1/binary>>, Frame),
    ?assertEqual(~"x", kura_crypto:decrypt(Frame)),
    ?assertEqual(~"x", kura_crypto:decrypt(Frame, {~"any", ~"column"})).

t_writes_v2() ->
    write_v2(),
    ?assertEqual(v2, kura_crypto:write_format()),
    Frame = kura_crypto:encrypt(~"top secret", ?CTX),
    ?assertEqual(~"top secret", kura_crypto:decrypt(Frame, ?CTX)),
    application:set_env(kura, encryption_write_format, v1),
    ?assertEqual(~"top secret", kura_crypto:decrypt(Frame, ?CTX)).

t_v2_layout() ->
    write_v2(),
    Plain = ~"123-45-6789",
    <<2, 0, 1:16, IV:12/binary, Tag:16/binary, CT/binary>> = kura_crypto:encrypt(Plain, ?CTX),
    ?assertEqual(byte_size(Plain), byte_size(CT)),
    {Table, Column} = ?CTX,
    Ctx = <<(byte_size(Table)):16, Table/binary, (byte_size(Column)):16, Column/binary>>,
    Aad = <<"kura", 2, 0, 1:16, Ctx/binary>>,
    ?assertEqual(
        Plain,
        crypto:crypto_one_time_aead(aes_256_gcm, hex(?SSN_SUBKEY), IV, CT, Aad, Tag, false)
    ).

t_not_root_key() ->
    write_v2(),
    <<2, 0, 1:16, IV:12/binary, Tag:16/binary, CT/binary>> = kura_crypto:encrypt(~"x", ?CTX),
    Aad = <<"kura", 2, 0, 1:16, 7:16, "secrets", 3:16, "ssn">>,
    ?assertEqual(
        error, crypto:crypto_one_time_aead(aes_256_gcm, root_key(1), IV, CT, Aad, Tag, false)
    ).

t_fresh_iv() ->
    write_v2(),
    IVs = [IV || _ <- lists:seq(1, 50), <<_:4/binary, IV:12/binary, _/binary>> <- [encrypt_x()]],
    ?assertEqual(50, length(lists:usort(IVs))).

encrypt_x() ->
    kura_crypto:encrypt(~"x", ?CTX).

t_no_context_decrypt() ->
    write_v2(),
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    ?assertError({kura_crypto, no_context}, kura_crypto:decrypt(Frame)),
    ?assertError({kura_crypto, no_context}, kura_crypto:decrypt(<<2>>)).

t_no_context_encrypt() ->
    write_v2(),
    ?assertError({kura_crypto, no_context}, kura_crypto:encrypt(~"x")).

t_non_binary_plaintext() ->
    Secret = ill_typed("do-not-leak"),
    [
        assert_clean(Call, [~"do-not-leak"])
     || Call <- [
            fun() -> kura_crypto:encrypt(Secret) end, fun() -> kura_crypto:encrypt(Secret, ?CTX) end
        ]
    ],
    ?assertError({kura_crypto, malformed_plaintext}, kura_crypto:encrypt(Secret)),
    write_v2(),
    ?assertError({kura_crypto, malformed_plaintext}, kura_crypto:encrypt(Secret, ?CTX)).

t_invalid_write_format() ->
    application:set_env(kura, encryption_write_format, v3),
    ?assertError({kura_crypto, invalid_write_format}, kura_crypto:write_format()),
    ?assertError({kura_crypto, invalid_write_format}, kura_crypto:encrypt(~"x", ?CTX)),
    ?assertError({kura_crypto, invalid_write_format}, kura_crypto:encrypt(~"x")).

t_invalid_context() ->
    Long = binary:copy(~"a", 65536),
    Max = binary:copy(~"a", 65535),
    Bad = [{<<>>, ~"ssn"}, {~"secrets", <<>>}, {Long, ~"ssn"}, {~"secrets", Long}],
    [
        ?assertError({kura_crypto, invalid_context}, kura_crypto:encrypt(~"x", ill_typed(C)))
     || C <- Bad ++ [{secrets, ~"ssn"}, {~"secrets", ssn}, ~"secrets.ssn", {~"a", ~"b", ~"c"}]
    ],
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    [?assertError({kura_crypto, invalid_context}, kura_crypto:decrypt(Frame, C)) || C <- Bad],
    write_v2(),
    ?assertEqual(~"x", kura_crypto:decrypt(kura_crypto:encrypt(~"x", {Max, Max}), {Max, Max})).

t_unambiguous_context() ->
    write_v2(),
    Frame = kura_crypto:encrypt(~"x", {~"ab", ~"c"}),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(Frame, {~"a", ~"bc"})),
    ?assertNotEqual(
        kura_crypto:subkey(root_key(1), {~"ab", ~"c"}),
        kura_crypto:subkey(root_key(1), {~"a", ~"bc"})
    ).

t_wrong_context() ->
    write_v2(),
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    [
        ?assertError({kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(Frame, C))
     || C <- [{~"secrets", ~"nickname"}, {~"other", ~"ssn"}, {~"ssn", ~"secrets"}]
    ].

t_mode_swap() ->
    write_v2(),
    <<2, 0, Rest/binary>> = kura_crypto:encrypt(~"x", ?CTX),
    ?assertError(
        {kura_crypto, {unsupported_mode, 1}}, kura_crypto:decrypt(<<2, 1, Rest/binary>>, ?CTX)
    ),
    ?assertError(
        {kura_crypto, {unsupported_mode, 255}}, kura_crypto:decrypt(<<2, 255, Rest/binary>>, ?CTX)
    ).

t_key_id_swap() ->
    write_v2(),
    <<2, 0, 1:16, Rest/binary>> = kura_crypto:encrypt(~"x", ?CTX),
    ?assertError(
        {kura_crypto, {bad_tag, 2}}, kura_crypto:decrypt(<<2, 0, 2:16, Rest/binary>>, ?CTX)
    ),
    ?assertError(
        {kura_crypto, {unknown_key_id, 3}}, kura_crypto:decrypt(<<2, 0, 3:16, Rest/binary>>, ?CTX)
    ).

t_wide_key_id() ->
    write_v2(),
    application:set_env(kura, keyring, kura_test_lenient_keyring),
    <<2, 0, 1:16, Rest/binary>> = hex(?PINNED_FRAME),
    ?assertEqual(~"123-45-6789", kura_crypto:decrypt(hex(?PINNED_FRAME), ?CTX)),
    [
        ?assertError(
            {kura_crypto, {unknown_key_id, Id}},
            kura_crypto:decrypt(<<2, 0, Id:16, Rest/binary>>, ?CTX)
        )
     || Id <- [256, 257, 1000, 65535]
    ].

t_key_id_255() ->
    write_v2(),
    application:set_env(kura, encryption, #{
        active => 255, keys => [{255, base64:encode(root_key(1))}]
    }),
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    ?assertMatch(<<2, 0, 255:16, _/binary>>, Frame),
    ?assertEqual(~"x", kura_crypto:decrypt(Frame, ?CTX)).

t_active_key_id_range() ->
    application:set_env(kura, keyring, kura_test_lenient_keyring),
    try
        [
            begin
                application:set_env(kura, test_active_key_id, Id),
                ?assertMatch(<<1, Id, _/binary>>, kura_crypto:encrypt(~"x"))
            end
         || Id <- [0, 255]
        ],
        [
            begin
                application:set_env(kura, test_active_key_id, Id),
                ?assertError({kura_crypto, no_active_key}, kura_crypto:encrypt(~"x"))
            end
         || Id <- [-1, 256, one]
        ]
    after
        application:unset_env(kura, test_active_key_id)
    end.

t_truncated() ->
    write_v2(),
    Frame = kura_crypto:encrypt(~"", ?CTX),
    ?assertEqual(32, byte_size(Frame)),
    ?assertEqual(<<>>, kura_crypto:decrypt(Frame, ?CTX)),
    [
        ?assertError(
            {kura_crypto, malformed_frame}, kura_crypto:decrypt(binary:part(Frame, 0, N), ?CTX)
        )
     || N <- lists:seq(1, 31)
    ],
    ?assertError({kura_crypto, malformed_frame}, kura_crypto:decrypt(<<>>, ?CTX)),
    Long = kura_crypto:encrypt(~"abc", ?CTX),
    ?assertError(
        {kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(binary:part(Long, 0, 34), ?CTX)
    ).

t_unknown_version() ->
    Frame = kura_crypto:encrypt(~"x", ?CTX),
    <<_, Rest/binary>> = Frame,
    [
        ?assertError(
            {kura_crypto, {unsupported_version, V}}, kura_crypto:decrypt(<<V, Rest/binary>>, ?CTX)
        )
     || V <- [0, 3, 255]
    ],
    ?assertError({kura_crypto, {unsupported_version, 3}}, kura_crypto:decrypt(<<3>>, ?CTX)),
    ?assertError({kura_crypto, {unsupported_version, 3}}, kura_crypto:decrypt(<<3>>)).

t_rotation() ->
    write_v2(),
    Old = kura_crypto:encrypt(~"old", ?CTX),
    application:set_env(kura, encryption, #{active => 2, keys => keys()}),
    New = kura_crypto:encrypt(~"new", ?CTX),
    ?assertMatch(<<2, 0, 2:16, _/binary>>, New),
    ?assertEqual(~"old", kura_crypto:decrypt(Old, ?CTX)),
    ?assertEqual(~"new", kura_crypto:decrypt(New, ?CTX)).

t_bad_keyring() ->
    Frame = hex(?PINNED_FRAME),
    V1 = kura_crypto:encrypt(~"x"),
    application:set_env(kura, keyring, kura_test_bad_keyring),
    ?assertError({kura_crypto, no_active_key}, kura_crypto:encrypt(~"x")),
    ?assertError({kura_crypto, no_active_key}, kura_crypto:encrypt(~"x", ?CTX)),
    ?assertError({kura_crypto, {unknown_key_id, 1}}, kura_crypto:decrypt(Frame, ?CTX)),
    ?assertError({kura_crypto, {unknown_key_id, 1}}, kura_crypto:decrypt(V1)),
    write_v2(),
    ?assertError({kura_crypto, no_active_key}, kura_crypto:encrypt(~"x", ?CTX)).

t_no_secrets_in_errors() ->
    write_v2(),
    Plain = ~"do-not-leak-this-plaintext",
    Frame = kura_crypto:encrypt(Plain, ?CTX),
    <<Head:4/binary, IV:12/binary, Tag:16/binary, CT/binary>> = Frame,
    Flipped = <<Head/binary, IV/binary, Tag/binary, (flip_first(CT))/binary>>,
    Calls = [
        fun() -> kura_crypto:decrypt(Frame, {~"secrets", ~"other"}) end,
        fun() -> kura_crypto:decrypt(Flipped, ?CTX) end,
        fun() -> kura_crypto:decrypt(Frame) end
    ],
    Secrets = [root_key(1), hex(?SSN_SUBKEY), Plain, CT],
    [assert_clean(Call, Secrets) || Call <- Calls].

assert_clean(Call, Secrets) ->
    try Call() of
        V -> ?assertEqual(raised, V)
    catch
        error:{kura_crypto, _} = Reason:Stack ->
            Bin = term_to_binary({Reason, Stack}),
            [?assertEqual(nomatch, binary:match(Bin, S)) || S <- Secrets]
    end.

flip_first(<<B, Rest/binary>>) -> <<(B bxor 1), Rest/binary>>.

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.

hex(Hex) ->
    binary:decode_hex(Hex).
