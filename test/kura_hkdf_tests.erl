-module(kura_hkdf_tests).
-include_lib("eunit/include/eunit.hrl").

%% RFC 5869 Appendix A, test cases 1-3 (SHA-256).
rfc5869_test_() ->
    [
        {Name, fun() -> check(Salt, IKM, Info, Length, PRK, OKM) end}
     || {Name, Salt, IKM, Info, Length, PRK, OKM} <- vectors()
    ].

vectors() ->
    [
        {
            "A.1 basic",
            hex(~"000102030405060708090a0b0c"),
            binary:copy(<<16#0b>>, 22),
            hex(~"f0f1f2f3f4f5f6f7f8f9"),
            42,
            hex(~"077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5"),
            hex(
                ~"3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
            )
        },
        {
            "A.2 longer inputs and outputs",
            list_to_binary(lists:seq(16#60, 16#af)),
            list_to_binary(lists:seq(16#00, 16#4f)),
            list_to_binary(lists:seq(16#b0, 16#ff)),
            82,
            hex(~"06a6b88c5853361a06104c9ceb35b45cef760014904671014a193f40c15fc244"),
            hex([
                ~"b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c",
                ~"59045a99cac7827271cb41c65e590e09da3275600c2f09b8367793a9aca3db71",
                ~"cc30c58179ec3e87c14c01d5c1f3434f1d87"
            ])
        },
        {
            "A.3 empty salt and info",
            <<>>,
            binary:copy(<<16#0b>>, 22),
            <<>>,
            42,
            hex(~"19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04"),
            hex(
                ~"8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
            )
        }
    ].

check(Salt, IKM, Info, Length, PRK, OKM) ->
    ?assertEqual(PRK, kura_hkdf:extract(Salt, IKM)),
    ?assertEqual(OKM, kura_hkdf:expand(PRK, Info, Length)).

empty_salt_is_the_zero_salt_test() ->
    IKM = ~"input keying material",
    ?assertEqual(kura_hkdf:extract(<<0:256>>, IKM), kura_hkdf:extract(<<>>, IKM)).

output_is_a_prefix_of_a_longer_output_test() ->
    PRK = kura_hkdf:extract(~"salt", ~"ikm"),
    Long = kura_hkdf:expand(PRK, ~"info", 100),
    ?assertEqual(binary:part(Long, 0, 33), kura_hkdf:expand(PRK, ~"info", 33)).

info_separates_outputs_test() ->
    PRK = kura_hkdf:extract(~"salt", ~"ikm"),
    ?assertNotEqual(kura_hkdf:expand(PRK, ~"a", 32), kura_hkdf:expand(PRK, ~"b", 32)).

maximum_length_test() ->
    PRK = kura_hkdf:extract(~"salt", ~"ikm"),
    ?assertEqual(8160, byte_size(kura_hkdf:expand(PRK, <<>>, 8160))).

invalid_length_raises_test_() ->
    PRK = kura_hkdf:extract(~"salt", ~"ikm"),
    [
        ?_assertError({kura_hkdf, {invalid_length, L}}, kura_hkdf:expand(PRK, <<>>, L))
     || L <- [0, -1, 8161]
    ].

short_prk_raises_test_() ->
    [
        ?_assertError({kura_hkdf, short_prk}, kura_hkdf:expand(<<>>, <<>>, 32)),
        ?_assertError({kura_hkdf, short_prk}, kura_hkdf:expand(binary:copy(<<1>>, 31), <<>>, 32)),
        ?_assertEqual(32, byte_size(kura_hkdf:expand(binary:copy(<<1>>, 32), <<>>, 32)))
    ].

%% A function_clause would put the key material in the stacktrace.
badarg_raises_without_key_material_test_() ->
    Secret = binary:copy(<<16#5a>>, 32),
    SecretList = binary_to_list(Secret),
    Bad = ill_typed(SecretList),
    Calls = [
        {"extract, IKM not a binary", fun() -> kura_hkdf:extract(<<>>, Bad) end},
        {"extract, salt not a binary", fun() -> kura_hkdf:extract(Bad, Secret) end},
        {"expand, PRK not a binary", fun() -> kura_hkdf:expand(Bad, <<>>, 32) end},
        {"expand, info not a binary", fun() -> kura_hkdf:expand(Secret, Bad, 32) end},
        {"expand, length not an integer", fun() ->
            kura_hkdf:expand(Secret, <<>>, ill_typed(Secret))
        end}
    ],
    [{Name, fun() -> assert_badarg(Call, Secret) end} || {Name, Call} <- Calls].

%% A value of a type the kura_hkdf specs do not allow.
-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.

assert_badarg(Call, Secret) ->
    try Call() of
        V -> ?assertEqual(raised, V)
    catch
        error:Reason:Stack ->
            ?assertEqual({kura_hkdf, badarg}, Reason),
            ?assertEqual(nomatch, binary:match(term_to_binary(Stack), Secret))
    end.

hidden_from_docs_test() ->
    {ok, {docs_v1, _, _, _, ModuleDoc, _, _}} = code:get_doc(kura_hkdf),
    ?assertEqual(hidden, ModuleDoc).

hex(Hex) ->
    binary:decode_hex(iolist_to_binary(Hex)).
