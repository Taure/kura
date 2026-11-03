-module(kura_crypto_det_prop_tests).
-moduledoc "Property-based tests for the deterministic v2 frame.".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUMTESTS, 300).
-define(DET, #{mode => deterministic, legacy => reject}).

property_test_() ->
    {setup, fun setup/0, fun teardown/1,
        {timeout, 180, [
            {"round trip", fun() -> run(prop_round_trip()) end},
            {"equal ciphertexts iff equal plaintexts", fun() -> run(prop_equality()) end},
            {"another context encrypts differently", fun() -> run(prop_context_differs()) end},
            {"a frame moved to another column or table raises", fun() ->
                run(prop_other_context_raises())
            end},
            {"any 1-bit flip raises", fun() -> run(prop_bit_flip_raises()) end},
            {"any truncation raises", fun() -> run(prop_truncation_raises()) end},
            {"a key id swap raises", fun() -> run(prop_key_id_swap_raises()) end},
            {"a mode swap raises", fun() -> run(prop_mode_swap_raises()) end},
            {"a GCM-valid frame with a random IV raises noncanonical", fun() ->
                run(prop_random_iv_noncanonical())
            end},
            {"a uuid encrypts the same in any case and form", fun() ->
                run(prop_uuid_canonical())
            end},
            {"casefold encrypts case variants the same", fun() -> run(prop_casefold()) end},
            {"normalising twice changes nothing", fun() -> run(prop_normalize_idempotent()) end},
            {"a lookup matches the stored frame", fun() -> run(prop_lookup_matches()) end}
        ]}}.

setup() ->
    application:set_env(kura, encryption, #{
        active => 1,
        deterministic_active => 1,
        keys => [
            {1, base64:encode(crypto:strong_rand_bytes(32))},
            {2, base64:encode(crypto:strong_rand_bytes(32))}
        ]
    }).

teardown(_) ->
    application:unset_env(kura, encryption).

run(Prop) ->
    ?assert(proper:quickcheck(Prop, [?NUMTESTS, {to_file, user}, noshrink])).

gen_name() ->
    ?LET(Chars, non_empty(list(range($a, $z))), chars_to_binary(Chars)).

gen_context() ->
    {gen_name(), gen_name()}.

gen_frame() ->
    ?LET({Plain, Ctx}, {binary(), gen_context()}, {Plain, Ctx, encrypt(Plain, Ctx)}).

prop_round_trip() ->
    ?FORALL({Plain, Ctx, Frame}, gen_frame(), decrypt(Frame, Ctx) =:= Plain).

prop_equality() ->
    ?FORALL(
        {P1, P2, Ctx},
        {binary(), oneof([binary(), exactly(same)]), gen_context()},
        begin
            Other = same_or(P2, P1),
            (encrypt(P1, Ctx) =:= encrypt(Other, Ctx)) =:= (P1 =:= Other)
        end
    ).

prop_context_differs() ->
    ?FORALL(
        {Plain, Ctx, Other},
        {binary(), gen_context(), gen_context()},
        ?IMPLIES(Ctx =/= Other, encrypt(Plain, Ctx) =/= encrypt(Plain, Other))
    ).

prop_other_context_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Other},
        {gen_frame(), gen_context()},
        ?IMPLIES(Other =/= Ctx, raises(Frame, Other))
    ).

prop_bit_flip_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Seed},
        {gen_frame(), non_neg_integer()},
        raises(flip_bit(bin(Frame), Seed), Ctx)
    ).

prop_truncation_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Seed},
        {gen_frame(), non_neg_integer()},
        raises(truncate(bin(Frame), Seed), Ctx)
    ).

prop_key_id_swap_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Other},
        {gen_frame(), range(0, 65535)},
        ?IMPLIES(Other =/= 1, raises(set_key_id(bin(Frame), Other), Ctx))
    ).

prop_mode_swap_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Mode, Legacy},
        {gen_frame(), oneof([0, range(2, 255)]), oneof([accept, reject])},
        raises(set_mode(bin(Frame), Mode), Ctx, #{mode => deterministic, legacy => Legacy})
    ).

prop_random_iv_noncanonical() ->
    ?FORALL(
        {Plain, Ctx, IV},
        {binary(), gen_context(), binary(12)},
        begin
            Synthetic = binary:part(bin(encrypt(Plain, Ctx)), 4, 12),
            ?IMPLIES(IV =/= Synthetic, noncanonical(bin(Plain), ctx(Ctx), bin(IV)))
        end
    ).

prop_uuid_canonical() ->
    ?FORALL(
        Raw,
        binary(16),
        begin
            Hex = binary:encode_hex(bin(Raw), lowercase),
            Dashed = dashed(Hex),
            Forms = [bin(Raw), Hex, string:uppercase(Hex), Dashed, string:uppercase(Dashed)],
            Frames = lists:usort([dump(#{mode => deterministic}, uuid, F) || F <- Forms]),
            length(Frames) =:= 1 andalso
                load(#{mode => deterministic}, uuid, hd(Frames)) =:= {ok, Dashed}
        end
    ).

prop_casefold() ->
    ?FORALL(
        Plain,
        utf8(),
        dump(casefold(), string, string:uppercase(bin(Plain))) =:=
            dump(casefold(), string, string:lowercase(bin(Plain)))
    ).

prop_normalize_idempotent() ->
    ?FORALL(
        {Plain, Normalize},
        {utf8(), oneof([casefold, nfc, nfc_casefold])},
        begin
            Opts = #{mode => deterministic, normalize => Normalize},
            {ok, Cast} = kura_types:cast(type(Opts, text), Plain),
            {ok, Twice} = kura_types:cast(type(Opts, text), Cast),
            Cast =:= Twice andalso dump(Opts, text, Plain) =:= dump(Opts, text, Cast)
        end
    ).

prop_lookup_matches() ->
    ?FORALL(
        {Plain, Ctx},
        {binary(), gen_context()},
        lists:member(encrypt(Plain, Ctx), kura_crypto:lookup([bin(Plain)], ctx(Ctx)))
    ).

casefold() ->
    #{mode => deterministic, normalize => casefold}.

type(Opts, Inner) ->
    {encrypted, Inner, maps:merge(#{context => {~"t", ~"c"}}, Opts)}.

dump(Opts, Inner, Value) ->
    {ok, Frame} = kura_types:dump(type(Opts, Inner), Value),
    Frame.

load(Opts, Inner, Frame) ->
    kura_types:load(type(Opts, Inner), Frame).

dashed(<<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>>) ->
    <<A/binary, $-, B/binary, $-, C/binary, $-, D/binary, $-, E/binary>>.

noncanonical(Plain, Ctx, IV) ->
    {Table, Column} = Ctx,
    {_Mac, Enc} = kura_crypto:deterministic_subkeys(
        list_to_binary(lists:seq(1, 32)), Ctx
    ),
    CtxBytes = <<(byte_size(Table)):16, Table/binary, (byte_size(Column)):16, Column/binary>>,
    Aad = <<"kura", 2, 1, 7:16, CtxBytes/binary>>,
    {CT, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, Enc, IV, Plain, Aad, true),
    Frame = <<2, 1, 7:16, IV/binary, Tag/binary, CT/binary>>,
    application:set_env(kura, keyring, kura_test_lenient_keyring),
    try kura_crypto:decrypt(Frame, Ctx, ?DET) of
        _ -> false
    catch
        error:{kura_crypto, {noncanonical, 7}} -> true
    after
        application:unset_env(kura, keyring)
    end.

chars_to_binary(Chars) ->
    list_to_binary(Chars).

truncate(Frame, Seed) when is_integer(Seed) ->
    binary:part(Frame, 0, Seed rem byte_size(Frame)).

same_or(same, P1) -> P1;
same_or(P2, _P1) -> P2.

flip_bit(Frame, Seed) when is_integer(Seed) ->
    Bit = Seed rem bit_size(Frame),
    <<Pre:Bit/bitstring, B:1, Post/bitstring>> = Frame,
    <<Pre/bitstring, (1 - B):1, Post/bitstring>>.

set_key_id(<<2, 1, _:16, Rest/binary>>, KeyId) when is_integer(KeyId) ->
    <<2, 1, KeyId:16, Rest/binary>>.

set_mode(<<2, 1, Rest/binary>>, Mode) when is_integer(Mode) ->
    <<2, Mode, Rest/binary>>.

encrypt(Plain, Ctx) ->
    kura_crypto:encrypt(bin(Plain), ctx(Ctx), deterministic).

decrypt(Frame, Ctx) ->
    kura_crypto:decrypt(bin(Frame), ctx(Ctx), ?DET).

bin(B) when is_binary(B) -> B.

ctx({Table, Column}) when is_binary(Table), is_binary(Column) -> {Table, Column}.

raises(Frame, Ctx) ->
    raises(Frame, Ctx, ?DET).

raises(Frame, Ctx, Policy) ->
    try kura_crypto:decrypt(bin(Frame), ctx(Ctx), Policy) of
        _ -> false
    catch
        error:{kura_crypto, _} -> true
    end.
