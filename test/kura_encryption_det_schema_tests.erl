-module(kura_encryption_det_schema_tests).
-include_lib("eunit/include/eunit.hrl").

-define(OPTS, kura_test_det_opts_schema).
-define(DET, kura_test_det_schema).
-define(CTX, {~"det_users", ~"email"}).

forget_before_test() ->
    forget().

forget() ->
    _ = [
        kura_schema:forget(M)
     || {M, _} <- code:all_loaded(), lists:prefix("kura_test_", atom_to_list(M))
    ],
    ok.

%%----------------------------------------------------------------------
%% Schema-load validation
%%----------------------------------------------------------------------

refused_types() ->
    Det = #{mode => deterministic},
    Long = binary:copy(~"c", 65536),
    [
        {[mode], invalid_encryption_options},
        {deterministic, invalid_encryption_options},
        {#{foo => 1}, {unknown_encryption_option, foo}},
        {Det#{bind => row}, {unknown_encryption_option, bind}},
        {#{mode => siv}, {invalid_encryption_option, mode}},
        {#{mode => "deterministic"}, {invalid_encryption_option, mode}},
        {#{legacy => 'maybe'}, {invalid_encryption_option, legacy}},
        {Det#{normalize => upper}, {invalid_encryption_option, normalize}},
        {Det#{context => ~"det_opts.value"}, {invalid_encryption_option, context}},
        {Det#{context => {~"t", ~"c", ~"x"}}, {invalid_encryption_option, context}},
        {Det#{context => {t, ~"c"}}, {invalid_encryption_option, context}},
        {Det#{context => {<<>>, ~"c"}}, {invalid_encryption_option, context}},
        {Det#{context => {~"t", Long}}, {invalid_encryption_option, context}},
        {#{normalize => casefold}, {invalid_encryption_option, normalize}},
        {#{mode => randomized, normalize => nfc}, {invalid_encryption_option, normalize}}
    ].

refused_inner() ->
    [
        {{encrypted, boolean, #{mode => deterministic}}, unsupported_deterministic_type},
        {{encrypted, jsonb, #{mode => deterministic}}, unsupported_deterministic_type},
        {{encrypted, float, #{mode => deterministic}}, unsupported_encrypted_type},
        {{encrypted, date, #{}}, unsupported_encrypted_type},
        {{encrypted, {array, string}, #{mode => deterministic}}, unsupported_encrypted_type},
        {{array, {encrypted, string, #{mode => deterministic}}}, unsupported_deterministic_type},
        {
            {array, {array, {encrypted, integer, #{mode => deterministic}}}},
            unsupported_deterministic_type
        },
        {{array, {encrypted, string, #{mode => bogus}}}, {invalid_encryption_option, mode}},
        {{encrypted, string, #{}, extra}, unsupported_encrypted_type}
    ] ++
        [
            {
                {encrypted, I, #{mode => deterministic, normalize => N}},
                {invalid_encryption_option, normalize}
            }
         || I <- [binary, uuid, integer, bigint, smallint], N <- [casefold, nfc, nfc_casefold]
        ].

refused_test_() ->
    Cases =
        [{{encrypted, string, Opts}, Reason} || {Opts, Reason} <- refused_types()] ++
            refused_inner(),
    {foreach, fun forget/0, fun teardown_type/1, [
        {lists:flatten(io_lib:format("~0p", [Type])), fun() -> refused(Type, Reason) end}
     || {Type, Reason} <- Cases
    ]}.

refused(Type, Reason) ->
    application:set_env(kura, test_det_type, Type),
    Expected =
        case Reason of
            {Atom, Key} -> {kura, {Atom, ?OPTS, value, Key}};
            Atom -> {kura, {Atom, ?OPTS, value}}
        end,
    ?assertError(Expected, kura_schema:field_types(?OPTS)),
    ?assertEqual({error, Reason}, kura_types:validate_encryption(Type)),
    ?assertEqual(unsupported, kura_types:encryption(Type)).

accepted_test_() ->
    Det = #{mode => deterministic},
    Types =
        [{encrypted, I, Det} || I <- [string, text, binary, uuid, integer, bigint, smallint]] ++
            [
                {encrypted, T, Det#{normalize => N}}
             || T <- [string, text], N <- [none, casefold, nfc, nfc_casefold]
            ] ++
            [
                {encrypted, string, #{}},
                {encrypted, jsonb, #{mode => randomized}},
                {encrypted, boolean, #{legacy => accept}},
                {encrypted, string, Det#{legacy => accept, context => {~"t", ~"c"}}},
                {encrypted, string, #{normalize => none}},
                {array, {encrypted, string, #{}}}
            ],
    {foreach, fun forget/0, fun teardown_type/1, [
        {lists:flatten(io_lib:format("~0p", [T])), fun() -> accepted(T) end}
     || T <- Types
    ]}.

accepted(Type) ->
    application:set_env(kura, test_det_type, Type),
    ?assertMatch(#{value := Type}, kura_schema:field_types(?OPTS)),
    ?assertEqual(ok, kura_types:validate_encryption(Type)),
    ?assertEqual([value], kura_schema:encrypted_fields(?OPTS)).

teardown_type(_) ->
    application:unset_env(kura, test_det_type),
    forget().

validate_plain_test() ->
    ?assertEqual(ok, kura_types:validate_encryption(string)),
    ?assertEqual(ok, kura_types:validate_encryption({array, integer})),
    ?assertEqual(ok, kura_types:validate_encryption({encrypted, string})),
    ?assertEqual(
        {error, unsupported_encrypted_type}, kura_types:validate_encryption({encrypted, float})
    ).

specs_test() ->
    Specs = kura_schema:encryption_specs(?DET),
    ?assertEqual(
        #{
            inner => string,
            mode => deterministic,
            legacy => reject,
            normalize => casefold,
            context => ?CTX
        },
        maps:get(email, Specs)
    ),
    ?assertMatch(#{mode := randomized, legacy := accept}, maps:get(note, Specs)),
    ?assertMatch(#{mode := randomized, legacy := reject}, maps:get(secret, Specs)),
    ?assertMatch(
        #{context := {~"shared", ~"code"}},
        maps:get(code, kura_schema:encryption_specs(kura_test_det_invite_schema))
    ),
    ?assertEqual(
        lists:sort([email, ssn, token, pin, note, secret]),
        lists:sort(kura_schema:encrypted_fields(?DET))
    ).

defaults_test() ->
    ?assertEqual(
        {ok, #{inner => string, mode => randomized, legacy => reject, normalize => none}},
        kura_types:encryption({encrypted, string, #{}})
    ),
    ?assertEqual(
        {ok, #{inner => string, mode => randomized, legacy => accept, normalize => none}},
        kura_types:encryption({encrypted, string})
    ).

primary_key_refused_test() ->
    application:set_env(kura, test_det_id_type, {encrypted, uuid, #{mode => deterministic}}),
    try
        ?assertError({kura, {encrypted_primary_key, ?OPTS, id}}, kura_schema:field_types(?OPTS))
    after
        application:unset_env(kura, test_det_id_type),
        forget()
    end.

%%----------------------------------------------------------------------
%% Seams
%%----------------------------------------------------------------------

seam_test_() ->
    {foreach, fun setup_keys/0, fun teardown_keys/1, [
        {"cast normalises", fun t_cast_normalises/0},
        {"cast canonicalises a uuid", fun t_cast_uuid/0},
        {"cast refuses what cannot be normalised", fun t_cast_refuses/0},
        {"dump canonicalises without a cast", fun t_dump_canonical/0},
        {"dump refuses what cannot be normalised", fun t_dump_refuses/0},
        {"dump writes v2 whatever the write format", fun t_dump_v2/0},
        {"the options form needs a context", fun t_needs_context/0},
        {"a pinned context wins", fun t_pinned_context/0},
        {"legacy reject refuses a v1 frame", fun t_legacy_reject/0},
        {"legacy accept reads a v1 frame", fun t_legacy_accept/0},
        {"load_row decrypts a deterministic field", fun t_load_row/0},
        {"to_pg_type is BYTEA", fun t_pg_type/0},
        {"the audit trail redacts deterministic fields", fun t_audit/0},
        {"lookup payloads", fun t_lookup_payload/0},
        {"lookup_params", fun t_lookup_params/0}
    ]}.

setup_keys() ->
    forget(),
    application:set_env(kura, encryption, #{
        active => 1,
        deterministic_active => 1,
        keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }).

teardown_keys(_) ->
    application:unset_env(kura, encryption),
    application:unset_env(kura, encryption_write_format),
    forget().

det(Opts) ->
    {encrypted, string, maps:merge(#{mode => deterministic}, Opts)}.

t_cast_normalises() ->
    Decomposed = <<"A", 16#CC, 16#8A>>,
    ?assertEqual(
        {ok, ~"ada@example.com"}, kura_types:cast(det(#{normalize => casefold}), ~"Ada@Example.COM")
    ),
    ?assertEqual({ok, <<16#C3, 16#85>>}, kura_types:cast(det(#{normalize => nfc}), Decomposed)),
    ?assertEqual(
        {ok, <<16#C3, 16#A5>>}, kura_types:cast(det(#{normalize => nfc_casefold}), Decomposed)
    ),
    ?assertEqual({ok, ~"Ada"}, kura_types:cast(det(#{}), ~"Ada")),
    ?assertEqual({ok, ~"Ada"}, kura_types:cast({encrypted, string, #{}}, ~"Ada")),
    ?assertEqual({ok, undefined}, kura_types:cast(det(#{normalize => casefold}), undefined)),
    ?assertEqual({ok, ~"\x{131}"}, kura_types:cast(det(#{normalize => casefold}), ~"\x{131}")),
    ?assertEqual({ok, ~"i"}, kura_types:cast(det(#{normalize => casefold}), ~"I")).

t_cast_uuid() ->
    Lower = ~"0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
    Type = {encrypted, uuid, #{mode => deterministic}},
    [
        ?assertEqual({ok, Lower}, kura_types:cast(Type, Form))
     || Form <- [
            Lower,
            string:uppercase(Lower),
            binary:replace(Lower, ~"-", <<>>, [global]),
            binary:decode_hex(binary:replace(Lower, ~"-", <<>>, [global]))
        ]
    ],
    ?assertEqual(
        {ok, string:uppercase(Lower)}, kura_types:cast({encrypted, uuid}, string:uppercase(Lower))
    ).

t_cast_refuses() ->
    Uuid = {encrypted, uuid, #{mode => deterministic}},
    ?assertEqual(
        {error, ~"is not a valid UUID"},
        kura_types:cast(Uuid, ~"0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5z")
    ),
    ?assertEqual(
        {error, ~"is not a valid UUID"},
        kura_types:cast(Uuid, ~"0190a5b2x7c3d-7e4f-8a9b-0c1d2e3f4a5b")
    ),
    ?assertEqual(
        {error, ~"is not valid UTF-8"},
        kura_types:cast(det(#{normalize => casefold}), <<"A", 255>>)
    ),
    ?assertEqual({ok, <<"A", 255>>}, kura_types:cast(det(#{}), <<"A", 255>>)).

t_dump_canonical() ->
    Type = det(#{normalize => casefold, context => ?CTX}),
    {ok, Upper} = kura_types:dump(Type, ~"ADA@EXAMPLE.COM"),
    {ok, Lower} = kura_types:dump(Type, ~"ada@example.com"),
    ?assertEqual(Upper, Lower),
    ?assertEqual({ok, ~"ada@example.com"}, kura_types:load(Type, Upper)),
    Uuid = {encrypted, uuid, #{mode => deterministic, context => ?CTX}},
    {ok, U1} = kura_types:dump(Uuid, ~"0190A5B2-7C3D-7E4F-8A9B-0C1D2E3F4A5B"),
    {ok, U2} = kura_types:dump(Uuid, ~"0190a5b27c3d7e4f8a9b0c1d2e3f4a5b"),
    ?assertEqual(U1, U2),
    Int = {encrypted, integer, #{mode => deterministic, context => ?CTX}},
    {ok, I1} = kura_types:dump(Int, 42),
    ?assertEqual({ok, I1}, kura_types:dump(Int, 42)),
    ?assertEqual({ok, 42}, kura_types:load(Int, I1)).

t_dump_refuses() ->
    ?assertError(
        {kura_crypto, malformed_plaintext},
        kura_types:dump(det(#{normalize => nfc, context => ?CTX}), <<"A", 255>>)
    ),
    ?assertError(
        {kura_crypto, malformed_plaintext},
        kura_types:dump({encrypted, uuid, #{mode => deterministic, context => ?CTX}}, ~"nope")
    ).

t_dump_v2() ->
    [
        begin
            application:set_env(kura, encryption_write_format, Format),
            ?assertMatch({ok, <<2, 1, _/binary>>}, kura_types:dump(det(#{}), ~"x", ?CTX)),
            ?assertMatch(
                {ok, <<2, 0, _/binary>>}, kura_types:dump({encrypted, string, #{}}, ~"x", ?CTX)
            )
        end
     || Format <- [v1, v2]
    ],
    ?assertEqual({ok, null}, kura_types:dump(det(#{}), undefined, ?CTX)).

t_needs_context() ->
    ?assertError({kura_crypto, no_context}, kura_types:dump(det(#{}), ~"x")),
    {ok, Frame} = kura_types:dump(det(#{}), ~"x", ?CTX),
    ?assertError({kura_crypto, no_context}, kura_types:load(det(#{}), Frame)),
    ?assertEqual({ok, ~"x"}, kura_types:load(det(#{}), Frame, ?CTX)).

t_pinned_context() ->
    Pinned = det(#{context => {~"shared", ~"code"}}),
    {ok, Frame} = kura_types:dump(Pinned, ~"x", {~"other", ~"column"}),
    ?assertEqual({ok, Frame}, kura_types:dump(det(#{}), ~"x", {~"shared", ~"code"})),
    ?assertEqual({ok, ~"x"}, kura_types:load(Pinned, Frame)).

t_legacy_reject() ->
    V1 = kura_crypto:encrypt(~"x"),
    ?assertError({kura_crypto, {refused_version, 1}}, kura_types:load(det(#{}), V1, ?CTX)),
    ?assertError(
        {kura_crypto, {refused_version, 1}}, kura_types:load({encrypted, string, #{}}, V1, ?CTX)
    ),
    {ok, Rnd} = kura_types:dump({encrypted, string, #{}}, ~"x", ?CTX),
    ?assertError({kura_crypto, {refused_mode, 0}}, kura_types:load(det(#{}), Rnd, ?CTX)).

t_legacy_accept() ->
    V1 = kura_crypto:encrypt(~"x"),
    ?assertEqual({ok, ~"x"}, kura_types:load(det(#{legacy => accept}), V1, ?CTX)),
    ?assertEqual({ok, ~"x"}, kura_types:load({encrypted, string}, V1, ?CTX)),
    {ok, Det} = kura_types:dump(det(#{}), ~"x", ?CTX),
    ?assertEqual({ok, ~"x"}, kura_types:load({encrypted, string, #{legacy => accept}}, Det, ?CTX)).

t_load_row() ->
    {ok, Email} = kura_types:dump(det(#{normalize => casefold}), ~"Ada@Example.com", ?CTX),
    ?assertEqual(
        #{id => 1, email => ~"ada@example.com"}, kura_db:load_row(?DET, #{id => 1, email => Email})
    ),
    {ok, Other} = kura_types:dump(det(#{}), ~"x", {~"det_users", ~"ssn"}),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_db:load_row(?DET, #{email => Other})).

t_pg_type() ->
    ?assertEqual(~"BYTEA", kura_types:to_pg_type(det(#{}))),
    ?assertEqual(
        ~"BYTEA", kura_dialect_pg:column_type({encrypted, uuid, #{mode => deterministic}})
    ),
    ?assertEqual(~"BYTEA[]", kura_types:to_pg_type({array, {encrypted, string, #{}}})),
    ?assertError(
        {kura_crypto, unsupported_encrypted_type}, kura_types:to_pg_type({array, det(#{})})
    ).

t_audit() ->
    Record = #{id => 1, name => ~"a", email => ~"ada@example.com", ssn => ~"123", pin => 7},
    [
        begin
            Data = kura_audit:build_audit_data(?DET, Action, Record),
            Bin = term_to_binary(Data),
            [?assertEqual(nomatch, binary:match(Bin, S)) || S <- [~"ada@example.com", ~"123"]]
        end
     || Action <- [insert, delete, update]
    ],
    ?assertMatch(
        {undefined, #{email := ~"[encrypted]", ssn := ~"[encrypted]", pin := ~"[encrypted]"},
            undefined},
        kura_audit:build_audit_data(?DET, insert, Record)
    ).

t_lookup_payload() ->
    {ok, Spec} = kura_types:encryption(det(#{normalize => casefold})),
    ?assertEqual({ok, ~"ada"}, kura_types:lookup_payload(Spec, ~"ADA")),
    ?assertEqual({ok, ~"ada"}, kura_types:lookup_payload(Spec, "ADA")),
    ?assertEqual(error, kura_types:lookup_payload(Spec, undefined)),
    ?assertEqual(error, kura_types:lookup_payload(Spec, <<"A", 255>>)),
    ?assertEqual(error, kura_types:lookup_payload(Spec, [bad, list])),
    ?assertEqual(error, kura_types:lookup_payload(Spec, #{})),
    {ok, Int} = kura_types:encryption({encrypted, integer, #{mode => deterministic}}),
    ?assertEqual({ok, ~"42"}, kura_types:lookup_payload(Int, 42)),
    ?assertEqual({ok, ~"42"}, kura_types:lookup_payload(Int, ~"42")),
    ?assertEqual(error, kura_types:lookup_payload(Int, ~"4x2")).

t_lookup_params() ->
    [Frame] = kura_encryption:lookup_params(?DET, email, ~"ADA@example.com"),
    {ok, Stored} = kura_types:dump(det(#{normalize => casefold}), ~"ada@example.com", ?CTX),
    ?assertEqual(Stored, Frame),
    ?assertError(
        {kura, {not_a_deterministic_field, note}}, kura_encryption:lookup_params(?DET, note, ~"x")
    ),
    ?assertError(
        {kura, {not_a_deterministic_field, name}}, kura_encryption:lookup_params(?DET, name, ~"x")
    ),
    ?assertError(
        {kura, {not_a_deterministic_field, email}},
        kura_encryption:lookup_params(kura_no_such_schema, email, ~"x")
    ),
    [
        ?assertError(
            {kura, {invalid_lookup_value, pin}}, kura_encryption:lookup_params(?DET, pin, Bad)
        )
     || Bad <- [nil, null, undefined, ~"x", 1.5]
    ].
