-module(kura_crypto_v2_prop_tests).
-moduledoc "Property-based tests for the v2 ciphertext frame.".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUMTESTS, 300).

property_test_() ->
    {setup, fun setup/0, fun teardown/1,
        {timeout, 120, [
            {"v2 round trip", fun() -> run(prop_round_trip()) end},
            {"v2 round trip through kura_types", fun() -> run(prop_types_round_trip()) end},
            {"any 1-bit flip raises", fun() -> run(prop_bit_flip_raises()) end},
            {"any truncation raises", fun() -> run(prop_truncation_raises()) end},
            {"a key id swap raises", fun() -> run(prop_key_id_swap_raises()) end},
            {"a mode swap raises", fun() -> run(prop_mode_swap_raises()) end},
            {"a frame moved to another column or table raises", fun() ->
                run(prop_other_context_raises())
            end},
            {"equal plaintexts encrypt differently", fun() -> run(prop_randomized()) end}
        ]}}.

setup() ->
    application:set_env(kura, encryption, #{active => 1, keys => keys()}),
    application:set_env(kura, encryption_write_format, v2).

teardown(_) ->
    application:unset_env(kura, encryption),
    application:unset_env(kura, encryption_write_format).

keys() ->
    [
        {1, base64:encode(crypto:strong_rand_bytes(32))},
        {2, base64:encode(crypto:strong_rand_bytes(32))}
    ].

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

prop_types_round_trip() ->
    ?FORALL({Plain, Ctx}, {utf8(), gen_context()}, types_round_trip(Plain, ctx(Ctx))).

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
        ?IMPLIES(key_id(bin(Frame)) =/= Other, raises(set_key_id(bin(Frame), Other), Ctx))
    ).

prop_mode_swap_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Mode},
        {gen_frame(), range(1, 255)},
        raises(set_mode(bin(Frame), Mode), Ctx)
    ).

prop_other_context_raises() ->
    ?FORALL(
        {{_Plain, Ctx, Frame}, Other},
        {gen_frame(), gen_context()},
        ?IMPLIES(Other =/= Ctx, raises(Frame, Other))
    ).

prop_randomized() ->
    ?FORALL({Plain, Ctx}, {binary(), gen_context()}, encrypt(Plain, Ctx) =/= encrypt(Plain, Ctx)).

types_round_trip(Plain, Ctx) ->
    {ok, Frame} = kura_types:dump({encrypted, text}, Plain, Ctx),
    {ok, Loaded} = kura_types:load({encrypted, text}, Frame, Ctx),
    Loaded =:= Plain andalso binary:at(bin(Frame), 0) =:= 2.

flip_bit(Frame, Seed) when is_integer(Seed) ->
    Bit = Seed rem bit_size(Frame),
    <<Pre:Bit/bitstring, B:1, Post/bitstring>> = Frame,
    <<Pre/bitstring, (1 - B):1, Post/bitstring>>.

truncate(Frame, Seed) when is_integer(Seed) ->
    binary:part(Frame, 0, Seed rem byte_size(Frame)).

key_id(<<2, 0, KeyId:16, _/binary>>) -> KeyId.

set_key_id(<<2, 0, _:16, Rest/binary>>, KeyId) when is_integer(KeyId) ->
    <<2, 0, KeyId:16, Rest/binary>>.

set_mode(<<2, 0, Rest/binary>>, Mode) when is_integer(Mode) ->
    <<2, Mode, Rest/binary>>.

encrypt(Plain, Ctx) ->
    kura_crypto:encrypt(bin(Plain), ctx(Ctx)).

decrypt(Frame, Ctx) ->
    kura_crypto:decrypt(bin(Frame), ctx(Ctx)).

chars_to_binary(Chars) ->
    list_to_binary(Chars).

bin(B) when is_binary(B) -> B.

ctx({Table, Column}) when is_binary(Table), is_binary(Column) -> {Table, Column}.

raises(Frame, Ctx) ->
    try decrypt(Frame, Ctx) of
        _ -> false
    catch
        error:{kura_crypto, _} -> true
    end.
