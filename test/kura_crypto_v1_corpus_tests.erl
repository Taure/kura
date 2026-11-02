-module(kura_crypto_v1_corpus_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CORPUS, "kura_crypto_v1_corpus.eterm").

corpus_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Corpus) ->
        [
            {label(N, Inner), fun() -> decrypts(KeyId, Inner, Plain, Hex) end}
         || {N, {KeyId, Inner, Plain, Hex}} <- lists:enumerate(Corpus)
        ] ++
            [
                {"corpus covers every encryptable inner type", fun() ->
                    covers_every_inner(Corpus)
                end},
                {"a frame does not decrypt under the other key", fun() ->
                    wrong_key_raises(Corpus)
                end}
            ]
    end}.

setup() ->
    application:set_env(kura, encryption, #{active => 1, keys => keys()}),
    {ok, Corpus} = file:consult(filename:join(filename:dirname(?FILE), ?CORPUS)),
    Corpus.

teardown(_) ->
    application:unset_env(kura, encryption).

keys() ->
    [
        {1, base64:encode(list_to_binary(lists:seq(1, 32)))},
        {2, base64:encode(list_to_binary(lists:seq(101, 132)))}
    ].

decrypts(KeyId, Inner, Plain, Hex) ->
    Frame = binary:decode_hex(Hex),
    ?assertMatch(<<1, KeyId, _/binary>>, Frame),
    ?assertEqual({ok, Plain}, kura_types:load({encrypted, Inner}, Frame)).

covers_every_inner(Corpus) ->
    Inners = lists:usort([Inner || {_, Inner, _, _} <- Corpus]),
    Encryptable = [
        T
     || T <- [string, text, binary, uuid, jsonb, integer, bigint, smallint, boolean],
        kura_types:encryptable_inner(T)
    ],
    ?assertEqual(lists:sort(Encryptable), Inners).

wrong_key_raises(Corpus) ->
    [{1, Inner, _, Hex} | _] = Corpus,
    <<1, 1, Rest/binary>> = binary:decode_hex(Hex),
    ?assertError(
        {kura_crypto, {bad_tag, 2}}, kura_types:load({encrypted, Inner}, <<1, 2, Rest/binary>>)
    ).

label(N, Inner) ->
    lists:flatten(io_lib:format("v1 frame ~b (~s) decrypts", [N, Inner])).
