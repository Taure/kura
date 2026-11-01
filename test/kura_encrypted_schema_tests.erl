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
        {"encrypted has_many owner-side referenced key",
            ?_assertError(
                {kura, {encrypted_association_key, kura_test_enc_owner_key_schema, kids, email}},
                kura_schema:field_types(kura_test_enc_owner_key_schema)
            )},
        {"encrypted has_one owner-side referenced key",
            ?_assertError(
                {kura, {encrypted_association_key, kura_test_enc_owner_key_one_schema, kid, email}},
                kura_schema:field_types(kura_test_enc_owner_key_one_schema)
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
        ?_assertEqual({ok, #{inner => string}}, kura_types:encryption({encrypted, string})),
        ?_assertEqual(
            {ok, #{inner => {array, string}}}, kura_types:encryption({encrypted, {array, string}})
        ),
        ?_assertEqual(unsupported, kura_types:encryption(V2)),
        ?_assertEqual(none, kura_types:encryption(string)),
        ?_assertEqual(none, kura_types:encryption({array, string})),
        ?_assertEqual(none, kura_types:encryption({array, {array, string}})),
        ?_assertEqual(
            {ok, #{inner => string, array => 1}},
            kura_types:encryption({array, {encrypted, string}})
        ),
        ?_assertEqual(
            {ok, #{inner => string, array => 2}},
            kura_types:encryption({array, {array, {encrypted, string}}})
        ),
        ?_assertEqual(unsupported, kura_types:encryption({array, V2})),
        ?_assertEqual(none, kura_types:encryption({embed, embeds_one, kura_test_address})),
        ?_assertError(
            {kura_crypto, malformed_frame}, kura_types:load({encrypted, string}, 42)
        ),
        ?_assertEqual({ok, undefined}, kura_types:load({encrypted, string}, undefined)),
        ?_assertEqual({ok, undefined}, kura_types:load({encrypted, string}, null)),
        ?_assertEqual(
            {ok, [undefined]}, kura_types:load({array, {encrypted, string}}, [undefined])
        )
    ].

%% An inner type that cannot be encrypted fails when the schema is first read,
%% not at the first write.
unencryptable_inner_type_test() ->
    kura_schema:forget(kura_test_enc_float_schema),
    ?assertError(
        {kura, {unsupported_encrypted_type, kura_test_enc_float_schema, amounts}},
        kura_schema:field_types(kura_test_enc_float_schema)
    ).

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

%% An array of an encrypted type is encrypted element by element. Every
%% "is this field encrypted" check must see it: before, the audit trail
%% stored its elements in plaintext and the filter guard let them be bound.
array_of_encrypted_is_encrypted_test_() ->
    Arr = kura_test_enc_array_schema,
    {setup, fun setup_keys/0, fun teardown_keys/1, [
        {"schema load", ?_assertMatch(#{tags := _}, kura_schema:field_types(Arr))},
        {"encrypted_fields", ?_assertEqual([tags], kura_schema:encrypted_fields(Arr))},
        {"nested",
            ?_assertEqual([grid], kura_schema:encrypted_fields(kura_test_enc_nested_array_schema))},
        {"query guard",
            ?_assertError(
                {kura, {encrypted_field_in_where, tags}},
                kura_query:where(kura_query:from(Arr), {tags, [~"x"]})
            )},
        {"audit",
            ?_assertMatch(
                {undefined, #{tags := ~"[encrypted]"}, undefined},
                kura_audit:build_audit_data(Arr, insert, #{id => 1, tags => [~"123-45-6789"]})
            )},
        {"column type",
            ?_assertEqual(~"BYTEA[]", kura_types:to_pg_type({array, {encrypted, string}}))},
        {"nested column type",
            ?_assertEqual(
                ~"BYTEA[][]", kura_types:to_pg_type({array, {array, {encrypted, string}}})
            )},
        {"migration column type",
            ?_assertEqual(~"BYTEA[]", kura_dialect_pg:column_type({array, {encrypted, string}}))},
        {"an array of the options form is refused",
            ?_assertError(
                {kura_crypto, unsupported_encrypted_type},
                kura_types:to_pg_type({array, kura_test_enc_v2_schema:v2_type()})
            )},
        {"elements round-trip encrypted", fun() ->
            T = {array, {array, {encrypted, string}}},
            V = [[~"123-45-6789", ~"b"], [~"c", ~"d"]],
            {ok, Dumped} = kura_types:dump(T, V),
            ?assertEqual(nomatch, binary:match(term_to_binary(Dumped), ~"123-45-6789")),
            ?assertEqual({ok, V}, kura_types:load(T, Dumped))
        end}
    ]}.

setup_keys() ->
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }).

teardown_keys(_) ->
    application:unset_env(kura, encryption).
