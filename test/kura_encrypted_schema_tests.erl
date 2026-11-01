-module(kura_encrypted_schema_tests).
-include_lib("eunit/include/eunit.hrl").

%%----------------------------------------------------------------------
%% Recognition: one function, both forms
%%----------------------------------------------------------------------

encryption_test_() ->
    V2 = kura_test_enc_v2_schema:v2_type(),
    [
        ?_assertEqual({ok, string}, kura_types:encryption({encrypted, string})),
        ?_assertEqual(unsupported, kura_types:encryption(V2)),
        ?_assertEqual(none, kura_types:encryption(string)),
        ?_assertEqual(none, kura_types:encryption({array, string})),
        ?_assertEqual(none, kura_types:encryption({embed, embeds_one, kura_test_address})),
        ?_assertError(
            {kura_crypto, malformed_frame}, kura_types:load({encrypted, string}, 42)
        )
    ].

%% The options form is not supported yet. Every seam must treat it as
%% encrypted and raise rather than handle the value as plaintext.
options_form_fails_closed_test_() ->
    V2 = kura_test_enc_v2_schema:v2_type(),
    Unsupported = {kura_crypto, unsupported_encrypted_type},
    {setup, fun setup_keys/0, fun teardown_keys/1, [
        {"cast", ?_assertError(Unsupported, kura_types:cast(V2, ~"a@b.c"))},
        {"dump", ?_assertError(Unsupported, kura_types:dump(V2, ~"a@b.c"))},
        {"load", fun() ->
            {ok, Frame} = kura_types:dump({encrypted, string}, ~"a@b.c"),
            ?assertError(Unsupported, kura_types:load(V2, Frame))
        end},
        {"to_pg_type", ?_assertError(Unsupported, kura_types:to_pg_type(V2))},
        {"NULL stays NULL", ?_assertEqual({ok, null}, kura_types:dump(V2, undefined))},
        {"listed as encrypted",
            ?_assertEqual([email], kura_schema:encrypted_fields(kura_test_enc_v2_schema))},
        {"query guard",
            ?_assertError(
                {kura, {encrypted_field_in_where, email}},
                kura_query:where(kura_query:from(kura_test_enc_v2_schema), {email, ~"a@b.c"})
            )},
        {"audit redacts", fun() ->
            {undefined, New, undefined} = kura_audit:build_audit_data(
                kura_test_enc_v2_schema, insert, #{id => 1, email => ~"a@b.c"}
            ),
            ?assertEqual(~"[encrypted]", maps:get(email, New))
        end}
    ]}.

setup_keys() ->
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }).

teardown_keys(_) ->
    application:unset_env(kura, encryption).
