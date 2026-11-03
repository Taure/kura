-module(kura_encryption_seam_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, kura_test_encrypted_schema).
-define(SSN_CTX, {~"secrets", ~"ssn"}).

forget_test_schemas_before_test() ->
    forget_test_schemas().

seam_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"encryption specs come from table() and the field name", fun t_specs/0},
        {"encryption specs bind the column name", fun t_specs_use_the_column_name/0},
        {"a v1 frame from another column still loads after the flip",
            fun t_load_row_v1_downgrade/0},
        {"dump/3 writes v1 by default", fun t_dump3_default_v1/0},
        {"dump/3 writes v2 when configured", fun t_dump3_v2/0},
        {"dump/2 raises no_context when writing v2", fun t_dump2_no_context/0},
        {"load/2 raises no_context on a v2 frame", fun t_load2_no_context/0},
        {"dump/3 and load/3 pass other types through", fun t_other_types/0},
        {"dump/3 and load/3 on NULL and bad values", fun t_null_and_bad/0},
        {"arrays of encrypted values are bound per column", fun t_arrays/0},
        {"load_row decrypts v1 and v2 frames", fun t_load_row/0},
        {"load_row refuses a frame from another column", fun t_load_row_transplant/0},
        {"load_row of an array field", fun t_load_row_array/0},
        {"load_row keeps a value its type cannot load", fun t_load_row_unloadable/0},
        {"dump_field threads the context", fun t_dump_field/0},
        {"audit redacts a field loaded from a v2 frame", fun t_audit_redaction/0}
    ]}.

forget_test_schemas_after_test() ->
    forget_test_schemas().

setup() ->
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    application:unset_env(kura, encryption_write_format).

teardown(_) ->
    application:unset_env(kura, encryption),
    application:unset_env(kura, encryption_write_format).

write_v2() ->
    application:set_env(kura, encryption_write_format, v2).

v2(Plain, Ctx) ->
    write_v2(),
    {ok, Frame} = kura_types:dump({encrypted, string}, Plain, Ctx),
    Frame.

t_specs() ->
    ?assertEqual(#{ssn => #{context => ?SSN_CTX}}, kura_schema:encryption_specs(?SCHEMA)),
    ?assertEqual(
        #{tags => #{context => {~"enc_array", ~"tags"}}},
        kura_schema:encryption_specs(kura_test_enc_array_schema)
    ),
    ?assertEqual(#{}, kura_schema:encryption_specs(kura_test_schema)),
    ?assertEqual(#{}, kura_schema:encryption_specs(kura_no_such_schema_module)).

t_specs_use_the_column_name() ->
    Ctx = {~"enc_column", ~"ssn_ciphertext"},
    ?assertEqual(
        #{ssn => #{context => Ctx}}, kura_schema:encryption_specs(kura_test_enc_column_schema)
    ),
    Frame = v2(~"x", Ctx),
    ?assertEqual(
        #{ssn => ~"x"}, kura_db:load_row(kura_test_enc_column_schema, #{ssn => Frame})
    ),
    ?assertError(
        {kura_crypto, {bad_tag, 1}},
        kura_db:load_row(kura_test_enc_column_schema, #{ssn => v2(~"x", {~"enc_column", ~"ssn"})})
    ).

t_load_row_v1_downgrade() ->
    {ok, V1} = kura_types:dump({encrypted, string}, ~"from nickname", {~"secrets", ~"nickname"}),
    ?assertMatch(<<1, 1, _/binary>>, V1),
    write_v2(),
    ?assertEqual(#{ssn => ~"from nickname"}, kura_db:load_row(?SCHEMA, #{ssn => V1})).

t_dump3_default_v1() ->
    {ok, Frame} = kura_types:dump({encrypted, string}, ~"x", ?SSN_CTX),
    ?assertMatch(<<1, 1, _/binary>>, Frame),
    ?assertEqual({ok, ~"x"}, kura_types:load({encrypted, string}, Frame)),
    ?assertEqual({ok, ~"x"}, kura_types:load({encrypted, string}, Frame, {~"t", ~"c"})).

t_dump3_v2() ->
    write_v2(),
    {ok, Frame} = kura_types:dump({encrypted, integer}, -42, ?SSN_CTX),
    ?assertMatch(<<2, 0, 1:16, _/binary>>, Frame),
    ?assertEqual({ok, -42}, kura_types:load({encrypted, integer}, Frame, ?SSN_CTX)).

t_dump2_no_context() ->
    write_v2(),
    ?assertError({kura_crypto, no_context}, kura_types:dump({encrypted, string}, ~"x")),
    ?assertError(
        {kura_crypto, no_context}, kura_types:dump({array, {encrypted, string}}, [~"x"])
    ).

t_load2_no_context() ->
    Frame = v2(~"x", ?SSN_CTX),
    ?assertError({kura_crypto, no_context}, kura_types:load({encrypted, string}, Frame)),
    ?assertError({kura_crypto, no_context}, kura_types:load({array, {encrypted, string}}, [Frame])).

t_other_types() ->
    write_v2(),
    ?assertEqual({ok, 5}, kura_types:dump(integer, 5, ?SSN_CTX)),
    ?assertEqual({ok, [1, 2]}, kura_types:dump({array, integer}, [1, 2], ?SSN_CTX)),
    ?assertEqual({ok, ~"x"}, kura_types:load(string, ~"x", ?SSN_CTX)),
    ?assertEqual({ok, [1, 2]}, kura_types:load({array, integer}, [1, 2], ?SSN_CTX)),
    ?assertEqual({error, ~"cannot dump integer"}, kura_types:dump(integer, ~"x", ?SSN_CTX)).

t_null_and_bad() ->
    write_v2(),
    Enc = {encrypted, string},
    ?assertEqual({ok, null}, kura_types:dump(Enc, undefined, ?SSN_CTX)),
    ?assertEqual({ok, null}, kura_types:dump(Enc, null, ?SSN_CTX)),
    ?assertEqual({ok, undefined}, kura_types:load(Enc, null, ?SSN_CTX)),
    ?assertEqual({ok, undefined}, kura_types:load(Enc, undefined, ?SSN_CTX)),
    ?assertError({kura_crypto, malformed_frame}, kura_types:load(Enc, 42, ?SSN_CTX)),
    ?assertError({kura_crypto, invalid_context}, kura_types:dump(Enc, ~"x", {~"", ~"ssn"})),
    ?assertMatch({error, _}, kura_types:dump({array, Enc}, ~"x", ?SSN_CTX)),
    ?assertMatch({error, _}, kura_types:load({array, Enc}, 42, ?SSN_CTX)).

t_arrays() ->
    write_v2(),
    Type = {array, {array, {encrypted, string}}},
    Ctx = {~"enc_array", ~"tags"},
    {ok, [[A, null], [B]]} = kura_types:dump(Type, [[~"a", undefined], [~"b"]], Ctx),
    ?assertMatch(<<2, 0, _/binary>>, A),
    ?assertMatch(<<2, 0, _/binary>>, B),
    ?assertEqual(
        {ok, [[~"a", undefined], [~"b"]]}, kura_types:load(Type, [[A, null], [B]], Ctx)
    ),
    ?assertError(
        {kura_crypto, {bad_tag, 1}}, kura_types:load(Type, [[A]], {~"enc_array", ~"other"})
    ).

t_load_row() ->
    V1 = kura_crypto:encrypt(~"old"),
    V2 = v2(~"new", ?SSN_CTX),
    ?assertEqual(
        #{id => 1, name => ~"a", ssn => ~"old"},
        kura_db:load_row(?SCHEMA, #{id => 1, name => ~"a", ssn => V1})
    ),
    ?assertEqual(
        #{id => 2, name => ~"b", ssn => ~"new"},
        kura_db:load_row(?SCHEMA, #{id => 2, name => ~"b", ssn => V2})
    ),
    ?assertEqual(#{id => 3, ssn => undefined}, kura_db:load_row(?SCHEMA, #{id => 3, ssn => null})).

t_load_row_transplant() ->
    Elsewhere = v2(~"x", {~"secrets", ~"nickname"}),
    OtherTable = v2(~"x", {~"users", ~"ssn"}),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_db:load_row(?SCHEMA, #{ssn => Elsewhere})),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_db:load_row(?SCHEMA, #{ssn => OtherTable})).

t_load_row_array() ->
    write_v2(),
    Ctx = {~"enc_array", ~"tags"},
    {ok, Tags} = kura_types:dump({array, {encrypted, string}}, [~"a", ~"b"], Ctx),
    ?assertEqual(
        #{id => 1, tags => [~"a", ~"b"]},
        kura_db:load_row(kura_test_enc_array_schema, #{id => 1, tags => Tags})
    ).

t_load_row_unloadable() ->
    ?assertEqual(#{id => ~"x"}, kura_db:load_row(?SCHEMA, #{id => ~"x"})),
    ?assertEqual(#{tags => 42}, kura_db:load_row(kura_test_enc_array_schema, #{tags => 42})),
    ?assertEqual(#{age => ~"x"}, kura_db:load_row(kura_test_schema, #{age => ~"x"})).

t_dump_field() ->
    write_v2(),
    {ok, Frame} = kura_repo_worker:dump_field({encrypted, string}, ~"x", ?SSN_CTX),
    ?assertMatch(<<2, 0, _/binary>>, Frame),
    ?assertEqual({ok, ~"x"}, kura_types:load({encrypted, string}, Frame, ?SSN_CTX)),
    ?assertEqual({ok, 33}, kura_repo_worker:dump_field(integer, ~"33", ?SSN_CTX)),
    ?assertError(
        {kura_crypto, no_context},
        kura_repo_worker:dump_field({encrypted, string}, ~"x", no_context)
    ).

t_audit_redaction() ->
    Plain = ~"123-45-6789",
    Row = kura_db:load_row(?SCHEMA, #{id => 1, name => ~"a", ssn => v2(Plain, ?SSN_CTX)}),
    ?assertEqual(Plain, maps:get(ssn, Row)),
    {undefined, New, undefined} = kura_audit:build_audit_data(?SCHEMA, insert, Row),
    ?assertEqual(~"[encrypted]", maps:get(ssn, New)),
    erlang:put({kura_audit_stash, ?SCHEMA}, Row),
    {Old, New2, Changes} = kura_audit:build_audit_data(?SCHEMA, update, Row#{ssn => ~"x"}),
    ?assertEqual(nomatch, binary:match(term_to_binary({New, Old, New2, Changes}), Plain)),
    ?assertEqual(#{old => ~"[encrypted]", new => ~"[encrypted]"}, maps:get(ssn, Changes)).

forget_test_schemas() ->
    [
        kura_schema:forget(M)
     || M <- [?SCHEMA, kura_test_enc_array_schema, kura_test_schema, kura_test_enc_column_schema]
    ],
    ok.
