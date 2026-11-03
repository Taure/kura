-module(kura_crypto_det_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CTX, {~"secrets", ~"ssn"}).
-define(DET, #{mode => deterministic, legacy => reject}).
-define(DET_ACCEPT, #{mode => deterministic, legacy => accept}).
-define(RND, #{mode => randomized, legacy => reject}).

%% Computed with an independent HKDF-SHA256, HMAC-SHA256 and AES-256-GCM
%% (Python hmac and the cryptography package) from the layout in the
%% kura_crypto moduledoc: root key 1 is the bytes 1..32.
-define(SSN_MAC_KEY, ~"233d7df30fafe1e6b2572687b5d9afb0f1b39974c2a7cdd71dec63690c3a3b8e").
-define(SSN_ENC_KEY, ~"9704807d5fbf5c774a4f0bc27e870166341c7bd9033606f36063666e3355d393").
-define(EMAIL_MAC_KEY, ~"764fd330569a1051d8c74d577811adcc4ef1e65b3f80b0b3fd7dd71192ee3977").
-define(EMAIL_ENC_KEY, ~"18ea6948a69829164151f7253b331ac2cd81f4041363db28e3e51773d8a767a4").
%% Key id 1, "123-45-6789", ?CTX.
-define(SSN_FRAME,
    ~"02010001da498d2ea2ea5ed58df266c2ba0d876efdc9f1eb8925801d5457415267e2ace61482e10244b4b2"
).
%% Key id 1, the empty plaintext, ?CTX.
-define(EMPTY_FRAME, ~"02010001f56e886e2dd706920a40f0b427ee7c0c5a99a018ac5f7dd43c87cc4f").
%% Key id 7, "ada@example.com", {"users", "email"}.
-define(EMAIL_FRAME,
    ~"020100075ca70a9a8aef73a2aab6484430d2d982d37743ca2f836bd18e2a2e9336f230dc6f2d4f9a3aab7451e8d14f"
).

det_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"subkeys match the pinned vectors", fun t_subkeys/0},
        {"frames match the pinned vectors", fun t_pinned_frames/0},
        {"the pinned frames decrypt", fun t_pinned_decrypt/0},
        {"the frame is laid out as documented", fun t_layout/0},
        {"deterministic subkeys differ from the randomized one", fun t_key_separation/0},
        {"equal plaintexts encrypt equally", fun t_equal/0},
        {"deterministic ignores the write format", fun t_ignores_write_format/0},
        {"deterministic writes use deterministic_active", fun t_uses_deterministic_active/0},
        {"a GCM-valid frame with another IV raises noncanonical", fun t_noncanonical/0},
        {"another column or table raises", fun t_wrong_context/0},
        {"legacy reject refuses v1 frames", fun t_reject_v1/0},
        {"legacy reject refuses the other mode", fun t_reject_other_mode/0},
        {"legacy accept reads v1 and the other mode", fun t_accept_legacy/0},
        {"decrypt/2 reads a deterministic frame", fun t_decrypt2_reads_deterministic/0},
        {"an unknown mode raises", fun t_unknown_mode/0},
        {"an invalid policy raises", fun t_invalid_policy/0},
        {"an invalid mode raises", fun t_invalid_mode/0},
        {"a non-binary plaintext raises", fun t_malformed_plaintext/0},
        {"no deterministic_active raises", fun t_no_deterministic_key/0},
        {"a keyring without the callbacks raises", fun t_keyring_without_callbacks/0},
        {"an invalid deterministic_active raises", fun t_invalid_deterministic_active/0},
        {"lookup encrypts under every key id", fun t_lookup_key_ids/0},
        {"lookup finds rows of both generations", fun t_lookup_generations/0},
        {"lookup puts deterministic_active first", fun t_lookup_order/0},
        {"lookup without key_ids uses deterministic_active", fun t_lookup_without_key_ids/0},
        {"lookup is capped at 8 keys", fun t_lookup_cap/0},
        {"invalid key ids raise", fun t_invalid_key_ids/0},
        {"an unresolvable key id raises", fun t_unresolvable_key_id/0},
        {"is_deterministic_frame/1", fun t_is_deterministic_frame/0},
        {"errors carry no key material or plaintext", fun t_no_secrets_in_errors/0}
    ]}.

setup() ->
    application:set_env(kura, encryption, #{
        active => 1, deterministic_active => 1, keys => keys([1, 2])
    }),
    application:unset_env(kura, encryption_write_format),
    application:unset_env(kura, keyring).

teardown(_) ->
    [
        application:unset_env(kura, K)
     || K <- [encryption, encryption_write_format, keyring, test_det_active, test_key_ids]
    ],
    ok.

keys(Ids) ->
    [{Id, base64:encode(root_key(Id))} || Id <- Ids].

root_key(1) -> list_to_binary(lists:seq(1, 32));
root_key(Id) -> binary:copy(<<Id>>, 32).

det(Plain) ->
    kura_crypto:encrypt(Plain, ?CTX, deterministic).

t_subkeys() ->
    ?assertEqual(
        {hex(?SSN_MAC_KEY), hex(?SSN_ENC_KEY)}, kura_crypto:deterministic_subkeys(root_key(1), ?CTX)
    ),
    ?assertEqual(
        {hex(?EMAIL_MAC_KEY), hex(?EMAIL_ENC_KEY)},
        kura_crypto:deterministic_subkeys(root_key(1), {~"users", ~"email"})
    ).

t_pinned_frames() ->
    ?assertEqual(hex(?SSN_FRAME), det(~"123-45-6789")),
    ?assertEqual(hex(?EMPTY_FRAME), det(<<>>)),
    application:set_env(kura, encryption, #{
        deterministic_active => 7, keys => [{7, base64:encode(root_key(1))}]
    }),
    ?assertEqual(
        hex(?EMAIL_FRAME),
        kura_crypto:encrypt(~"ada@example.com", {~"users", ~"email"}, deterministic)
    ).

t_pinned_decrypt() ->
    ?assertEqual(~"123-45-6789", kura_crypto:decrypt(hex(?SSN_FRAME), ?CTX, ?DET)),
    ?assertEqual(<<>>, kura_crypto:decrypt(hex(?EMPTY_FRAME), ?CTX, ?DET)).

t_layout() ->
    Plain = ~"123-45-6789",
    <<2, 1, 1:16, IV:12/binary, Tag:16/binary, CT/binary>> = det(Plain),
    Ctx = <<7:16, "secrets", 3:16, "ssn">>,
    Aad = <<"kura", 2, 1, 1:16, Ctx/binary>>,
    <<Synthetic:12/binary, _/binary>> = crypto:mac(hmac, sha256, hex(?SSN_MAC_KEY), [Aad, Plain]),
    ?assertEqual(Synthetic, IV),
    ?assertEqual(
        Plain, crypto:crypto_one_time_aead(aes_256_gcm, hex(?SSN_ENC_KEY), IV, CT, Aad, Tag, false)
    ).

t_key_separation() ->
    {Mac, Enc} = kura_crypto:deterministic_subkeys(root_key(1), ?CTX),
    Rnd = kura_crypto:subkey(root_key(1), ?CTX),
    ?assertEqual(4, length(lists:usort([Mac, Enc, Rnd, root_key(1)]))).

t_equal() ->
    ?assertEqual(det(~"a"), det(~"a")),
    ?assertNotEqual(det(~"a"), det(~"b")),
    ?assertNotEqual(det(~"a"), kura_crypto:encrypt(~"a", {~"secrets", ~"other"}, deterministic)),
    ?assertNotEqual(
        kura_crypto:encrypt(~"a", ?CTX, randomized), kura_crypto:encrypt(~"a", ?CTX, randomized)
    ).

t_ignores_write_format() ->
    [
        begin
            application:set_env(kura, encryption_write_format, Format),
            ?assertEqual(hex(?SSN_FRAME), det(~"123-45-6789")),
            ?assertMatch(<<2, 0, 1:16, _/binary>>, kura_crypto:encrypt(~"x", ?CTX, randomized))
        end
     || Format <- [v1, v2]
    ].

t_uses_deterministic_active() ->
    application:set_env(kura, encryption, #{
        active => 1, deterministic_active => 2, keys => keys([1, 2])
    }),
    ?assertMatch(<<2, 1, 2:16, _/binary>>, det(~"x")),
    ?assertMatch(<<2, 0, 1:16, _/binary>>, kura_crypto:encrypt(~"x", ?CTX, randomized)),
    ?assertEqual(~"x", kura_crypto:decrypt(det(~"x"), ?CTX, ?DET)).

t_noncanonical() ->
    Plain = ~"123-45-6789",
    Aad = <<"kura", 2, 1, 1:16, 7:16, "secrets", 3:16, "ssn">>,
    IV = crypto:strong_rand_bytes(12),
    {CT, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, hex(?SSN_ENC_KEY), IV, Plain, Aad, true),
    Frame = <<2, 1, 1:16, IV/binary, Tag/binary, CT/binary>>,
    ?assertError({kura_crypto, {noncanonical, 1}}, kura_crypto:decrypt(Frame, ?CTX, ?DET)),
    ?assertError({kura_crypto, {noncanonical, 1}}, kura_crypto:decrypt(Frame, ?CTX)).

t_wrong_context() ->
    Frame = det(~"x"),
    [
        ?assertError({kura_crypto, {bad_tag, 1}}, kura_crypto:decrypt(Frame, C, ?DET))
     || C <- [{~"secrets", ~"nickname"}, {~"other", ~"ssn"}]
    ],
    ?assertError({kura_crypto, no_context}, kura_crypto:decrypt(Frame)).

t_reject_v1() ->
    V1 = kura_crypto:encrypt(~"x"),
    [
        ?assertError({kura_crypto, {refused_version, 1}}, kura_crypto:decrypt(F, ?CTX, P))
     || F <- [V1, <<1>>], P <- [?DET, ?RND]
    ].

t_reject_other_mode() ->
    Rnd = kura_crypto:encrypt(~"x", ?CTX, randomized),
    ?assertError({kura_crypto, {refused_mode, 0}}, kura_crypto:decrypt(Rnd, ?CTX, ?DET)),
    ?assertError({kura_crypto, {refused_mode, 1}}, kura_crypto:decrypt(det(~"x"), ?CTX, ?RND)).

t_accept_legacy() ->
    V1 = kura_crypto:encrypt(~"one"),
    Rnd = kura_crypto:encrypt(~"two", ?CTX, randomized),
    ?assertEqual(~"one", kura_crypto:decrypt(V1, ?CTX, ?DET_ACCEPT)),
    ?assertEqual(~"two", kura_crypto:decrypt(Rnd, ?CTX, ?DET_ACCEPT)),
    ?assertEqual(
        ~"x", kura_crypto:decrypt(det(~"x"), ?CTX, #{mode => randomized, legacy => accept})
    ).

t_decrypt2_reads_deterministic() ->
    ?assertEqual(~"123-45-6789", kura_crypto:decrypt(hex(?SSN_FRAME), ?CTX)).

t_unknown_mode() ->
    <<2, 1, Rest/binary>> = det(~"x"),
    [
        ?assertError(
            {kura_crypto, {unsupported_mode, M}},
            kura_crypto:decrypt(<<2, M, Rest/binary>>, ?CTX, ?DET_ACCEPT)
        )
     || M <- [2, 255]
    ].

t_invalid_policy() ->
    Frame = det(~"x"),
    [
        ?assertError({kura_crypto, invalid_policy}, kura_crypto:decrypt(Frame, ?CTX, ill_typed(P)))
     || P <- [
            #{},
            #{mode => deterministic},
            #{legacy => accept},
            #{mode => other, legacy => accept},
            #{mode => deterministic, legacy => 'maybe'},
            deterministic
        ]
    ].

t_invalid_mode() ->
    ?assertError({kura_crypto, invalid_mode}, kura_crypto:encrypt(~"x", ?CTX, ill_typed(siv))),
    ?assertError(
        {kura_crypto, invalid_context}, kura_crypto:encrypt(~"x", ill_typed(bad), deterministic)
    ).

t_malformed_plaintext() ->
    ?assertError(
        {kura_crypto, malformed_plaintext}, kura_crypto:encrypt(ill_typed(1), ?CTX, deterministic)
    ),
    ?assertError(
        {kura_crypto, malformed_plaintext}, kura_crypto:lookup([~"a", ill_typed(1)], ?CTX)
    ),
    ?assertError({kura_crypto, malformed_plaintext}, kura_crypto:lookup(ill_typed(~"a"), ?CTX)).

t_no_deterministic_key() ->
    application:set_env(kura, encryption, #{active => 1, keys => keys([1])}),
    ?assertError({kura_crypto, no_deterministic_key}, det(~"x")),
    ?assertError({kura_crypto, no_deterministic_key}, kura_crypto:lookup([~"x"], ?CTX)),
    ?assertMatch(<<2, 0, _/binary>>, kura_crypto:encrypt(~"x", ?CTX, randomized)).

t_keyring_without_callbacks() ->
    application:set_env(kura, keyring, kura_test_lenient_keyring),
    ?assertError({kura_crypto, no_deterministic_key}, det(~"x")),
    ?assertError({kura_crypto, no_deterministic_key}, kura_crypto:lookup([~"x"], ?CTX)),
    ?assertEqual(~"123-45-6789", kura_crypto:decrypt(hex(?SSN_FRAME), ?CTX, ?DET)).

t_invalid_deterministic_active() ->
    application:set_env(kura, keyring, kura_test_det_keyring),
    [
        begin
            application:set_env(kura, test_det_active, Bad),
            ?assertError({kura_crypto, no_deterministic_key}, det(~"x"))
        end
     || Bad <- [
            undefined,
            {256, root_key(1)},
            {-1, root_key(1)},
            {one, root_key(1)},
            {1, binary:copy(<<1>>, 31)},
            root_key(1)
        ]
    ],
    [
        begin
            application:set_env(kura, test_det_active, {Id, root_key(1)}),
            ?assertMatch(<<2, 1, Id:16, _/binary>>, det(~"x"))
        end
     || Id <- [0, 255]
    ].

t_lookup_key_ids() ->
    application:set_env(kura, encryption, #{deterministic_active => 2, keys => keys([1, 2, 3])}),
    Frames = kura_crypto:lookup([~"a", ~"b"], ?CTX),
    ?assertEqual(
        [{2, ~"a"}, {2, ~"b"}, {1, ~"a"}, {1, ~"b"}, {3, ~"a"}, {3, ~"b"}],
        [{Id, kura_crypto:decrypt(F, ?CTX, ?DET)} || <<2, 1, Id:16, _/binary>> = F <- Frames]
    ),
    ?assertEqual([], kura_crypto:lookup([], ?CTX)).

t_lookup_generations() ->
    application:set_env(kura, encryption, #{deterministic_active => 1, keys => keys([1])}),
    Old = det(~"x"),
    application:set_env(kura, encryption, #{deterministic_active => 2, keys => keys([1, 2])}),
    New = det(~"x"),
    ?assertNotEqual(Old, New),
    Frames = kura_crypto:lookup([~"x"], ?CTX),
    ?assert(lists:member(Old, Frames)),
    ?assert(lists:member(New, Frames)),
    ?assertEqual(~"x", kura_crypto:decrypt(Old, ?CTX, ?DET)).

t_lookup_order() ->
    application:set_env(kura, keyring, kura_test_det_keyring),
    application:set_env(kura, test_det_active, {3, binary:copy(<<3>>, 32)}),
    application:set_env(kura, test_key_ids, [1, 3, 2, 1]),
    ?assertEqual(
        [3, 1, 2], [Id || <<2, 1, Id:16, _/binary>> <- kura_crypto:lookup([~"x"], ?CTX)]
    ).

t_lookup_without_key_ids() ->
    application:set_env(kura, keyring, kura_test_lenient_keyring),
    ?assertError({kura_crypto, no_deterministic_key}, kura_crypto:lookup([~"x"], ?CTX)),
    application:set_env(kura, keyring, kura_test_det_keyring),
    application:set_env(kura, test_key_ids, []),
    ?assertMatch([<<2, 1, 1:16, _/binary>>], kura_crypto:lookup([~"x"], ?CTX)).

t_lookup_cap() ->
    application:set_env(kura, encryption, #{
        deterministic_active => 1, keys => keys(lists:seq(1, 8))
    }),
    ?assertEqual(8, length(kura_crypto:lookup([~"x"], ?CTX))),
    application:set_env(kura, encryption, #{
        deterministic_active => 1, keys => keys(lists:seq(1, 9))
    }),
    ?assertError({kura_crypto, {too_many_lookup_keys, 9}}, kura_crypto:lookup([~"x"], ?CTX)),
    ?assertMatch(<<2, 1, 1:16, _/binary>>, det(~"x")).

t_invalid_key_ids() ->
    application:set_env(kura, keyring, kura_test_det_keyring),
    [
        begin
            application:set_env(kura, test_key_ids, Bad),
            ?assertError({kura_crypto, invalid_key_ids}, kura_crypto:lookup([~"x"], ?CTX))
        end
     || Bad <- [[256], [-1], [one], one, improper(1, 2)]
    ].

t_unresolvable_key_id() ->
    application:set_env(kura, keyring, kura_test_det_keyring),
    application:set_env(kura, test_key_ids, [1, 99]),
    ?assertError({kura_crypto, {unknown_key_id, 99}}, kura_crypto:lookup([~"x"], ?CTX)).

t_is_deterministic_frame() ->
    Frame = det(~"x"),
    ?assert(kura_crypto:is_deterministic_frame(Frame)),
    ?assert(kura_crypto:is_deterministic_frame(binary:part(Frame, 0, 32))),
    ?assertNot(kura_crypto:is_deterministic_frame(binary:part(Frame, 0, 31))),
    ?assertNot(kura_crypto:is_deterministic_frame(kura_crypto:encrypt(~"x", ?CTX, randomized))),
    ?assertNot(kura_crypto:is_deterministic_frame(kura_crypto:encrypt(~"x"))),
    <<2, 1, Rest/binary>> = Frame,
    ?assertNot(kura_crypto:is_deterministic_frame(<<3, 1, Rest/binary>>)),
    ?assertNot(kura_crypto:is_deterministic_frame(1)).

t_no_secrets_in_errors() ->
    Plain = ~"do-not-leak-this-plaintext",
    Frame = det(Plain),
    <<Head:4/binary, IV:12/binary, Tag:16/binary, CT/binary>> = Frame,
    Flipped = <<Head/binary, IV/binary, Tag/binary, (flip_first(CT))/binary>>,
    {_Mac, Enc} = kura_crypto:deterministic_subkeys(root_key(1), ?CTX),
    Aad = <<"kura", 2, 1, 1:16, 7:16, "secrets", 3:16, "ssn">>,
    OtherIV = crypto:strong_rand_bytes(12),
    {CT2, Tag2} = crypto:crypto_one_time_aead(aes_256_gcm, Enc, OtherIV, Plain, Aad, true),
    Noncanonical = <<2, 1, 1:16, OtherIV/binary, Tag2/binary, CT2/binary>>,
    Calls = [
        fun() -> kura_crypto:decrypt(Frame, {~"secrets", ~"other"}, ?DET) end,
        fun() -> kura_crypto:decrypt(Flipped, ?CTX, ?DET) end,
        fun() -> kura_crypto:decrypt(Noncanonical, ?CTX, ?DET) end,
        fun() -> kura_crypto:decrypt(Frame, ?CTX, ?RND) end
    ],
    [assert_clean(Call, [root_key(1), Enc, Plain, CT]) || Call <- Calls].

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

-spec improper(term(), term()) -> dynamic().
improper(Head, Tail) ->
    [Head | ill_typed(Tail)].
