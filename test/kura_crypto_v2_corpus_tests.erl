-module(kura_crypto_v2_corpus_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CORPUS, "kura_crypto_v2_corpus.eterm").

corpus_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Corpus) ->
        [
            {label(N, Inner), fun() -> decrypts(Entry) end}
         || {N, {_, Inner, _, _, _, _} = Entry} <- lists:enumerate(Corpus)
        ] ++
            [
                {"corpus covers every encryptable inner type", fun() ->
                    covers_every_inner(Corpus)
                end},
                {"corpus covers both keys", fun() -> covers_both_keys(Corpus) end},
                {"a frame raises in any other column", fun() -> other_context_raises(Corpus) end},
                {"a frame raises without a context", fun() -> no_context_raises(Corpus) end},
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

decrypts({KeyId, Inner, Table, Column, Plain, Hex}) ->
    Frame = binary:decode_hex(Hex),
    ?assertMatch(<<2, 0, KeyId:16, _/binary>>, Frame),
    ?assertEqual({ok, Plain}, kura_types:load({encrypted, Inner}, Frame, {Table, Column})).

covers_every_inner(Corpus) ->
    Inners = lists:usort([Inner || {_, Inner, _, _, _, _} <- Corpus]),
    Encryptable = [
        T
     || T <- [string, text, binary, uuid, jsonb, integer, bigint, smallint, boolean],
        kura_types:encryptable_inner(T)
    ],
    ?assertEqual(lists:sort(Encryptable), Inners).

covers_both_keys(Corpus) ->
    ?assertEqual([1, 2], lists:usort([KeyId || {KeyId, _, _, _, _, _} <- Corpus])).

other_context_raises(Corpus) ->
    [
        ?assertError(
            {kura_crypto, {bad_tag, KeyId}},
            kura_types:load({encrypted, Inner}, binary:decode_hex(Hex), Other)
        )
     || {KeyId, Inner, Table, Column, _, Hex} <- Corpus,
        Other <- [{Table, <<Column/binary, "_x">>}, {<<Table/binary, "_x">>, Column}]
    ].

no_context_raises(Corpus) ->
    [
        ?assertError(
            {kura_crypto, no_context}, kura_types:load({encrypted, Inner}, binary:decode_hex(Hex))
        )
     || {_, Inner, _, _, _, Hex} <- Corpus
    ].

wrong_key_raises(Corpus) ->
    [{1, Inner, Table, Column, _, Hex} | _] = Corpus,
    <<2, 0, 1:16, Rest/binary>> = binary:decode_hex(Hex),
    ?assertError(
        {kura_crypto, {bad_tag, 2}},
        kura_types:load({encrypted, Inner}, <<2, 0, 2:16, Rest/binary>>, {Table, Column})
    ).

label(N, Inner) ->
    lists:flatten(io_lib:format("v2 frame ~b (~s) decrypts", [N, Inner])).
