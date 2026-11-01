-module(kura_encrypted_schema_tests).
-include_lib("eunit/include/eunit.hrl").

%%----------------------------------------------------------------------
%% Placement: keys and embeds cannot be encrypted
%%----------------------------------------------------------------------

placement_test_() ->
    [
        {"encrypted primary key",
            ?_assertError(
                {kura, {encrypted_primary_key, kura_test_enc_pk_schema, ssn}},
                kura_schema:field_types(kura_test_enc_pk_schema)
            )},
        {"encrypted belongs_to foreign key",
            ?_assertError(
                {kura, {encrypted_association_key, kura_test_enc_fk_schema, owner, owner_id}},
                kura_schema:field_types(kura_test_enc_fk_schema)
            )},
        {"encrypted has_many foreign key on the target",
            ?_assertError(
                {kura,
                    {encrypted_association_key, kura_test_enc_has_many_schema, children, owner_id}},
                kura_schema:field_types(kura_test_enc_has_many_schema)
            )},
        {"encrypted many_to_many join key",
            ?_assertError(
                {kura, {encrypted_association_key, kura_test_enc_m2m_schema, links, owner_id}},
                kura_schema:field_types(kura_test_enc_m2m_schema)
            )},
        {"encrypted referenced key",
            ?_assertError(
                {kura, {encrypted_association_key, kura_test_enc_ref_schema, owner, ssn}},
                kura_schema:field_types(kura_test_enc_ref_schema)
            )},
        {"encrypted field in an embed",
            ?_assertError(
                {kura, {encrypted_embed_field, kura_test_enc_embed_middle, inner, secret}},
                kura_schema:field_types(kura_test_enc_embed_middle)
            )},
        {"encrypted field in a nested embed",
            ?_assertError(
                {kura, {encrypted_embed_field, kura_test_enc_embed_parent, inner, secret}},
                kura_schema:field_types(kura_test_enc_embed_parent)
            )},
        {"the query guard sees the refusal",
            ?_assertError(
                {kura, {encrypted_primary_key, kura_test_enc_pk_schema, ssn}},
                kura_query:where(kura_query:from(kura_test_enc_pk_schema), {name, ~"x"})
            )},
        {"an encrypted non-key field is accepted",
            ?_assertMatch(
                #{ssn := {encrypted, string}}, kura_schema:field_types(kura_test_encrypted_schema)
            )},
        {"an embedded schema may itself hold encrypted fields until embedded",
            ?_assertMatch(
                #{secret := {encrypted, string}}, kura_schema:field_types(kura_test_enc_embedded)
            )}
    ].

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
        {"schema load",
            ?_assertError(
                {kura, {unsupported_encrypted_type, kura_test_enc_v2_schema, email}},
                kura_schema:field_types(kura_test_enc_v2_schema)
            )},
        {"query guard",
            ?_assertError(
                {kura, {unsupported_encrypted_type, kura_test_enc_v2_schema, email}},
                kura_query:where(kura_query:from(kura_test_enc_v2_schema), {id, 1})
            )},
        {"audit",
            ?_assertError(
                {kura, {unsupported_encrypted_type, kura_test_enc_v2_schema, email}},
                kura_audit:build_audit_data(
                    kura_test_enc_v2_schema, insert, #{id => 1, email => ~"a@b.c"}
                )
            )}
    ]}.

setup_keys() ->
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }).

teardown_keys(_) ->
    application:unset_env(kura, encryption).
