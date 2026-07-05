-module(kura_types_tests).
-include_lib("eunit/include/eunit.hrl").

-eqwalizer({nowarn_function, load_embed_one_from_binary_test/0}).
-eqwalizer({nowarn_function, load_embed_one_from_map_test/0}).
-eqwalizer({nowarn_function, load_embed_many_from_binary_test/0}).
-eqwalizer({nowarn_function, load_embed_many_from_list_test/0}).
-eqwalizer({nowarn_function, dump_embed_one_test/0}).
-eqwalizer({nowarn_function, dump_embed_many_test/0}).

%%----------------------------------------------------------------------
%% to_pg_type
%%----------------------------------------------------------------------

to_pg_type_test_() ->
    [
        ?_assertEqual(<<"BIGSERIAL">>, kura_types:to_pg_type(id)),
        ?_assertEqual(<<"INTEGER">>, kura_types:to_pg_type(integer)),
        ?_assertEqual(<<"DOUBLE PRECISION">>, kura_types:to_pg_type(float)),
        ?_assertEqual(<<"VARCHAR(255)">>, kura_types:to_pg_type(string)),
        ?_assertEqual(<<"TEXT">>, kura_types:to_pg_type(text)),
        ?_assertEqual(<<"BOOLEAN">>, kura_types:to_pg_type(boolean)),
        ?_assertEqual(<<"DATE">>, kura_types:to_pg_type(date)),
        ?_assertEqual(<<"TIMESTAMPTZ">>, kura_types:to_pg_type(utc_datetime)),
        ?_assertEqual(<<"UUID">>, kura_types:to_pg_type(uuid)),
        ?_assertEqual(<<"JSONB">>, kura_types:to_pg_type(jsonb)),
        ?_assertEqual(<<"VARCHAR(255)">>, kura_types:to_pg_type({enum, [a, b]})),
        ?_assertEqual(<<"INTEGER[]">>, kura_types:to_pg_type({array, integer})),
        ?_assertEqual(<<"TEXT[]">>, kura_types:to_pg_type({array, text}))
    ].

%%----------------------------------------------------------------------
%% from_pg_type
%%----------------------------------------------------------------------

from_pg_type_test_() ->
    [
        ?_assertEqual(id, kura_types:from_pg_type(<<"int8">>, #{serial => true})),
        ?_assertEqual(bigint, kura_types:from_pg_type(<<"int8">>, #{})),
        ?_assertEqual(integer, kura_types:from_pg_type(<<"int4">>, #{})),
        ?_assertEqual(smallint, kura_types:from_pg_type(<<"int2">>, #{})),
        ?_assertEqual(float, kura_types:from_pg_type(<<"float8">>, #{})),
        ?_assertEqual(float, kura_types:from_pg_type(<<"float4">>, #{})),
        ?_assertEqual(decimal, kura_types:from_pg_type(<<"numeric">>, #{})),
        ?_assertEqual(string, kura_types:from_pg_type(<<"varchar">>, #{char_max_length => 255})),
        ?_assertEqual(text, kura_types:from_pg_type(<<"varchar">>, #{char_max_length => 80})),
        ?_assertEqual(text, kura_types:from_pg_type(<<"varchar">>, #{})),
        ?_assertEqual(text, kura_types:from_pg_type(<<"bpchar">>, #{})),
        ?_assertEqual(text, kura_types:from_pg_type(<<"text">>, #{})),
        ?_assertEqual(binary, kura_types:from_pg_type(<<"bytea">>, #{})),
        ?_assertEqual(boolean, kura_types:from_pg_type(<<"bool">>, #{})),
        ?_assertEqual(date, kura_types:from_pg_type(<<"date">>, #{})),
        ?_assertEqual(time, kura_types:from_pg_type(<<"time">>, #{})),
        ?_assertEqual(utc_datetime, kura_types:from_pg_type(<<"timestamptz">>, #{})),
        ?_assertEqual(naive_datetime, kura_types:from_pg_type(<<"timestamp">>, #{})),
        ?_assertEqual(uuid, kura_types:from_pg_type(<<"uuid">>, #{})),
        ?_assertEqual(jsonb, kura_types:from_pg_type(<<"jsonb">>, #{})),
        ?_assertEqual(jsonb, kura_types:from_pg_type(<<"json">>, #{})),
        ?_assertEqual({array, integer}, kura_types:from_pg_type(<<"_int4">>, #{})),
        ?_assertEqual({array, text}, kura_types:from_pg_type(<<"_text">>, #{})),
        ?_assertEqual(
            {unsupported, <<"tsvector">>}, kura_types:from_pg_type(<<"tsvector">>, #{})
        ),
        ?_assertEqual(
            {unsupported, <<"citext">>}, kura_types:from_pg_type(<<"_citext">>, #{})
        ),
        ?_assertEqual(bigint, kura_types:from_pg_type(<<"int8">>, #{serial => false}))
    ].

%%----------------------------------------------------------------------
%% {encrypted, Inner}
%%----------------------------------------------------------------------

encrypted_test_() ->
    {setup,
        fun() ->
            application:set_env(kura, encryption, #{
                active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
            })
        end,
        fun(_) -> application:unset_env(kura, encryption) end, [
            {"encryptable_inner allow-set", fun() ->
                ?assert(kura_types:encryptable_inner(string)),
                ?assert(kura_types:encryptable_inner(jsonb)),
                ?assert(kura_types:encryptable_inner(integer)),
                ?assert(kura_types:encryptable_inner(boolean)),
                ?assertNot(kura_types:encryptable_inner(id)),
                ?assertNot(kura_types:encryptable_inner(date)),
                ?assertNot(kura_types:encryptable_inner(float))
            end},
            {"to_pg_type is BYTEA", fun() ->
                ?assertEqual(<<"BYTEA">>, kura_types:to_pg_type({encrypted, string}))
            end},
            {"round-trip string", fun() -> rt({encrypted, string}, <<"pii@example.com">>) end},
            {"round-trip text", fun() -> rt({encrypted, text}, <<"a longer secret">>) end},
            {"round-trip integer", fun() -> rt({encrypted, integer}, 123456789) end},
            {"round-trip negative integer", fun() -> rt({encrypted, integer}, -42) end},
            {"round-trip boolean", fun() ->
                rt({encrypted, boolean}, true),
                rt({encrypted, boolean}, false)
            end},
            {"round-trip jsonb", fun() ->
                {ok, E} = kura_types:dump({encrypted, jsonb}, #{<<"a">> => 1}),
                ?assertEqual({ok, #{<<"a">> => 1}}, kura_types:load({encrypted, jsonb}, E))
            end},
            {"ciphertext hides plaintext", fun() ->
                {ok, E} = kura_types:dump({encrypted, string}, <<"secret">>),
                ?assertNotEqual(<<"secret">>, E),
                ?assertEqual(nomatch, binary:match(E, <<"secret">>))
            end},
            {"random nonce: two encryptions differ", fun() ->
                {ok, E1} = kura_types:dump({encrypted, string}, <<"x">>),
                {ok, E2} = kura_types:dump({encrypted, string}, <<"x">>),
                ?assertNotEqual(E1, E2)
            end},
            {"cast validates plaintext via inner", fun() ->
                ?assertEqual({ok, 42}, kura_types:cast({encrypted, integer}, <<"42">>)),
                ?assertMatch({error, _}, kura_types:cast({encrypted, integer}, <<"notnum">>))
            end},
            {"NULL stays null, unencrypted", fun() ->
                ?assertEqual({ok, null}, kura_types:dump({encrypted, string}, undefined)),
                ?assertEqual({ok, undefined}, kura_types:load({encrypted, string}, null))
            end},
            {"reject unsupported inner at cast", fun() ->
                ?assertMatch({error, _}, kura_types:cast({encrypted, date}, {2026, 1, 1})),
                ?assertMatch({error, _}, kura_types:cast({encrypted, id}, 1))
            end},
            {"reject unsupported inner at dump raises (fail-closed)", fun() ->
                ?assertError(
                    {kura_crypto, {not_encryptable, float}},
                    kura_types:dump({encrypted, float}, 1.5)
                )
            end},
            {"decode_inner raises on corrupt integer", fun() ->
                ?assertError(
                    {kura_crypto, malformed_plaintext},
                    kura_types:decode_inner(integer, <<"xx">>)
                )
            end},
            {"decode_inner raises on corrupt boolean", fun() ->
                ?assertError(
                    {kura_crypto, malformed_plaintext},
                    kura_types:decode_inner(boolean, <<2>>)
                )
            end},
            {"tampered ciphertext raises through load, never falls open to raw", fun() ->
                {ok, E} = kura_types:dump({encrypted, string}, <<"secret">>),
                Bad = <<
                    (binary:part(E, 0, byte_size(E) - 1))/binary, ((binary:last(E)) bxor 255):8
                >>,
                ?assertError({kura_crypto, {bad_tag, _}}, kura_types:load({encrypted, string}, Bad))
            end}
        ]}.

rt(Type, Value) ->
    {ok, Enc} = kura_types:dump(Type, Value),
    ?assertEqual({ok, Value}, kura_types:load(Type, Enc)).

%%----------------------------------------------------------------------
%% cast
%%----------------------------------------------------------------------

cast_undefined_test_() ->
    [
        ?_assertEqual({ok, undefined}, kura_types:cast(integer, undefined)),
        ?_assertEqual({ok, undefined}, kura_types:cast(string, null))
    ].

cast_id_test_() ->
    [
        ?_assertEqual({ok, 42}, kura_types:cast(id, 42)),
        ?_assertEqual({ok, 42}, kura_types:cast(id, <<"42">>)),
        ?_assertMatch({error, _}, kura_types:cast(id, <<"abc">>))
    ].

cast_integer_test_() ->
    [
        ?_assertEqual({ok, 42}, kura_types:cast(integer, 42)),
        ?_assertEqual({ok, 42}, kura_types:cast(integer, <<"42">>)),
        ?_assertMatch({error, _}, kura_types:cast(integer, <<"abc">>)),
        ?_assertMatch({error, _}, kura_types:cast(integer, 3.14))
    ].

cast_float_test_() ->
    [
        ?_assertEqual({ok, 3.14}, kura_types:cast(float, 3.14)),
        ?_assertEqual({ok, 42.0}, kura_types:cast(float, 42)),
        ?_assertEqual({ok, 3.14}, kura_types:cast(float, <<"3.14">>)),
        ?_assertEqual({ok, 42.0}, kura_types:cast(float, <<"42">>)),
        ?_assertMatch({error, _}, kura_types:cast(float, <<"abc">>))
    ].

cast_string_test_() ->
    [
        ?_assertEqual({ok, <<"hello">>}, kura_types:cast(string, <<"hello">>)),
        ?_assertEqual({ok, <<"hello">>}, kura_types:cast(string, "hello")),
        ?_assertEqual({ok, <<"hello">>}, kura_types:cast(string, hello)),
        ?_assertMatch({error, _}, kura_types:cast(string, 42))
    ].

cast_text_test_() ->
    [
        ?_assertEqual({ok, <<"hello">>}, kura_types:cast(text, <<"hello">>)),
        ?_assertEqual({ok, <<"hello">>}, kura_types:cast(text, "hello")),
        ?_assertMatch({error, _}, kura_types:cast(text, 42))
    ].

cast_boolean_test_() ->
    [
        ?_assertEqual({ok, true}, kura_types:cast(boolean, true)),
        ?_assertEqual({ok, false}, kura_types:cast(boolean, false)),
        ?_assertEqual({ok, true}, kura_types:cast(boolean, <<"true">>)),
        ?_assertEqual({ok, false}, kura_types:cast(boolean, <<"false">>)),
        ?_assertEqual({ok, true}, kura_types:cast(boolean, <<"1">>)),
        ?_assertEqual({ok, false}, kura_types:cast(boolean, <<"0">>))
    ].

cast_date_test_() ->
    [
        ?_assertEqual({ok, {2024, 1, 15}}, kura_types:cast(date, {2024, 1, 15})),
        ?_assertEqual({ok, {2024, 1, 15}}, kura_types:cast(date, <<"2024-01-15">>)),
        ?_assertMatch({error, _}, kura_types:cast(date, <<"not-a-date">>)),
        ?_assertMatch({error, _}, kura_types:cast(date, <<"20XX-01-15">>))
    ].

cast_utc_datetime_test_() ->
    [
        ?_assertEqual(
            {ok, {{2024, 1, 15}, {10, 30, 0}}},
            kura_types:cast(utc_datetime, {{2024, 1, 15}, {10, 30, 0}})
        ),
        ?_assertEqual(
            {ok, {{2024, 1, 15}, {10, 30, 0}}},
            kura_types:cast(utc_datetime, <<"2024-01-15T10:30:00Z">>)
        ),
        ?_assertEqual(
            {ok, {{2024, 1, 15}, {10, 30, 45}}},
            kura_types:cast(utc_datetime, <<"2024-01-15 10:30:45">>)
        ),
        ?_assertEqual(
            {ok, {{2024, 1, 15}, {10, 30, 0}}},
            kura_types:cast(utc_datetime, <<"2024-01-15T10:30Z">>)
        ),
        ?_assertMatch({error, _}, kura_types:cast(utc_datetime, <<"garbage">>)),
        ?_assertMatch({error, _}, kura_types:cast(utc_datetime, <<"2024-01-15TXX:30:00Z">>)),
        ?_assertMatch({error, _}, kura_types:cast(utc_datetime, <<"2024-01-15TXX:YY">>))
    ].

cast_uuid_test_() ->
    [
        ?_assertEqual(
            {ok, <<"550e8400-e29b-41d4-a716-446655440000">>},
            kura_types:cast(uuid, <<"550e8400-e29b-41d4-a716-446655440000">>)
        ),
        ?_assertEqual(
            {ok, <<"550e8400-e29b-41d4-a716-446655440000">>},
            kura_types:cast(uuid, <<"550e8400e29b41d4a716446655440000">>)
        )
    ].

cast_jsonb_test_() ->
    [
        ?_assertEqual({ok, #{<<"a">> => 1}}, kura_types:cast(jsonb, #{<<"a">> => 1})),
        ?_assertEqual({ok, [1, 2]}, kura_types:cast(jsonb, [1, 2])),
        ?_assertEqual({ok, #{<<"a">> => 1}}, kura_types:cast(jsonb, <<"{\"a\":1}">>)),
        ?_assertMatch({error, _}, kura_types:cast(jsonb, <<"not json">>))
    ].

cast_enum_test_() ->
    [
        ?_assertEqual({ok, active}, kura_types:cast({enum, [active, inactive]}, active)),
        ?_assertEqual({ok, active}, kura_types:cast({enum, [active, inactive]}, <<"active">>)),
        ?_assertEqual({ok, active}, kura_types:cast({enum, [active, inactive]}, "active")),
        ?_assertEqual({ok, undefined}, kura_types:cast({enum, [active, inactive]}, undefined)),
        ?_assertMatch({error, _}, kura_types:cast({enum, [active, inactive]}, banned)),
        ?_assertMatch({error, _}, kura_types:cast({enum, [active, inactive]}, <<"banned">>)),
        ?_assertMatch(
            {error, _}, kura_types:cast({enum, [active, inactive]}, <<"nonexistent_atom_xyz">>)
        ),
        %% list enum - invalid existing atom
        ?_assertMatch({error, _}, kura_types:cast({enum, [active, inactive]}, "banned")),
        %% list enum - nonexistent atom
        ?_assertMatch(
            {error, _}, kura_types:cast({enum, [active, inactive]}, "nonexistent_atom_list_xyz")
        )
    ].

cast_embed_test_() ->
    [
        ?_assertEqual({ok, #{a => 1}}, kura_types:cast({embed, embeds_one, some_mod}, #{a => 1})),
        ?_assertEqual(
            {ok, [#{a => 1}]}, kura_types:cast({embed, embeds_many, some_mod}, [#{a => 1}])
        )
    ].

cast_array_test_() ->
    [
        ?_assertEqual({ok, [1, 2, 3]}, kura_types:cast({array, integer}, [1, 2, 3])),
        ?_assertEqual(
            {ok, [1, 2, 3]}, kura_types:cast({array, integer}, [<<"1">>, <<"2">>, <<"3">>])
        ),
        ?_assertMatch({error, _}, kura_types:cast({array, integer}, [<<"a">>]))
    ].

cast_error_unknown_type_test() ->
    ?assertMatch({error, _}, kura_types:cast(integer, make_ref())).

%%----------------------------------------------------------------------
%% dump - all types
%%----------------------------------------------------------------------

dump_null_test() ->
    ?assertEqual({ok, null}, kura_types:dump(integer, undefined)).

dump_id_test() ->
    ?assertEqual({ok, 42}, kura_types:dump(id, 42)).

dump_integer_test() ->
    ?assertEqual({ok, 42}, kura_types:dump(integer, 42)).

dump_float_test_() ->
    [
        ?_assertEqual({ok, 3.14}, kura_types:dump(float, 3.14)),
        ?_assertEqual({ok, 42.0}, kura_types:dump(float, 42))
    ].

dump_string_test() ->
    ?assertEqual({ok, <<"hello">>}, kura_types:dump(string, <<"hello">>)).

dump_text_test() ->
    ?assertEqual({ok, <<"hello">>}, kura_types:dump(text, <<"hello">>)).

dump_boolean_test_() ->
    [
        ?_assertEqual({ok, true}, kura_types:dump(boolean, true)),
        ?_assertEqual({ok, false}, kura_types:dump(boolean, false))
    ].

dump_date_test() ->
    ?assertEqual({ok, {2024, 1, 15}}, kura_types:dump(date, {2024, 1, 15})).

dump_utc_datetime_test() ->
    ?assertEqual(
        {ok, {{2024, 1, 15}, {10, 30, 0}}},
        kura_types:dump(utc_datetime, {{2024, 1, 15}, {10, 30, 0}})
    ).

dump_uuid_test() ->
    ?assertEqual(
        {ok, <<"550e8400-e29b-41d4-a716-446655440000">>},
        kura_types:dump(uuid, <<"550e8400-e29b-41d4-a716-446655440000">>)
    ).

dump_jsonb_test() ->
    {ok, Encoded} = kura_types:dump(jsonb, #{<<"key">> => <<"val">>}),
    ?assert(is_binary(Encoded)).

dump_jsonb_scalar_roots_test() ->
    {ok, N} = kura_types:dump(jsonb, 42),
    ?assertEqual(<<"42">>, iolist_to_binary(N)),
    {ok, B} = kura_types:dump(jsonb, true),
    ?assertEqual(<<"true">>, iolist_to_binary(B)),
    {ok, S} = kura_types:dump(jsonb, <<"hi">>),
    ?assertEqual(<<"\"hi\"">>, iolist_to_binary(S)).

dump_enum_test() ->
    ?assertEqual({ok, <<"active">>}, kura_types:dump({enum, [active, inactive]}, active)).

dump_enum_undefined_test() ->
    ?assertEqual({ok, null}, kura_types:dump({enum, [active, inactive]}, undefined)).

dump_array_test() ->
    ?assertEqual({ok, [1, 2, 3]}, kura_types:dump({array, integer}, [1, 2, 3])).

dump_array_error_test() ->
    ?assertMatch({error, _}, kura_types:dump({array, integer}, [<<"not_an_int">>])).

dump_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(integer, <<"not_an_int">>)).

%%----------------------------------------------------------------------
%% load - all types
%%----------------------------------------------------------------------

load_null_test() ->
    ?assertEqual({ok, undefined}, kura_types:load(integer, null)).

load_id_test() ->
    ?assertEqual({ok, 42}, kura_types:load(id, 42)).

load_integer_test() ->
    ?assertEqual({ok, 42}, kura_types:load(integer, 42)).

load_float_test_() ->
    [
        ?_assertEqual({ok, 3.14}, kura_types:load(float, 3.14)),
        ?_assertEqual({ok, 42.0}, kura_types:load(float, 42))
    ].

load_string_test() ->
    ?assertEqual({ok, <<"hello">>}, kura_types:load(string, <<"hello">>)).

load_text_test() ->
    ?assertEqual({ok, <<"hello">>}, kura_types:load(text, <<"hello">>)).

load_boolean_test_() ->
    [
        ?_assertEqual({ok, true}, kura_types:load(boolean, true)),
        ?_assertEqual({ok, false}, kura_types:load(boolean, false))
    ].

load_date_test() ->
    ?assertEqual({ok, {2024, 1, 15}}, kura_types:load(date, {2024, 1, 15})).

load_utc_datetime_test() ->
    ?assertEqual(
        {ok, {{2024, 1, 15}, {10, 30, 0}}},
        kura_types:load(utc_datetime, {{2024, 1, 15}, {10, 30, 0}})
    ).

load_uuid_test() ->
    ?assertEqual(
        {ok, <<"550e8400-e29b-41d4-a716-446655440000">>},
        kura_types:load(uuid, <<"550e8400-e29b-41d4-a716-446655440000">>)
    ).

load_jsonb_binary_test() ->
    ?assertEqual({ok, #{<<"a">> => 1}}, kura_types:load(jsonb, <<"{\"a\":1}">>)).

load_jsonb_map_test() ->
    ?assertEqual({ok, #{<<"a">> => 1}}, kura_types:load(jsonb, #{<<"a">> => 1})).

load_enum_test() ->
    ?assertEqual({ok, active}, kura_types:load({enum, [active, inactive]}, <<"active">>)).

load_enum_unknown_test() ->
    ?assertMatch({error, _}, kura_types:load({enum, [active, inactive]}, <<"banned">>)).

load_enum_nonexistent_atom_test() ->
    ?assertMatch(
        {error, _}, kura_types:load({enum, [active, inactive]}, <<"nonexistent_load_atom_xyz">>)
    ).

load_enum_null_test() ->
    ?assertEqual({ok, undefined}, kura_types:load({enum, [active, inactive]}, null)).

load_array_test() ->
    ?assertEqual({ok, [1, 2, 3]}, kura_types:load({array, integer}, [1, 2, 3])).

load_array_error_test() ->
    ?assertMatch({error, _}, kura_types:load({array, integer}, [<<"not_an_int">>])).

load_error_test() ->
    ?assertMatch({error, _}, kura_types:load(integer, <<"not_an_int">>)).

%%----------------------------------------------------------------------
%% json error paths
%%----------------------------------------------------------------------

dump_jsonb_unencodable_test() ->
    %% Must be a map (to pass the guard) with an unencodable value
    ?assertMatch({error, _}, kura_types:dump(jsonb, #{<<"f">> => fun() -> ok end})).

%%----------------------------------------------------------------------
%% date/time parsing edge cases
%%----------------------------------------------------------------------

cast_date_invalid_numbers_test() ->
    ?assertMatch({error, _}, kura_types:cast(date, <<"20XX-01-15">>)).

cast_time_only_hours_minutes_test() ->
    ?assertEqual(
        {ok, {{2024, 1, 15}, {10, 30, 0}}},
        kura_types:cast(utc_datetime, <<"2024-01-15T10:30">>)
    ).

cast_time_invalid_hm_test() ->
    ?assertMatch({error, _}, kura_types:cast(utc_datetime, <<"2024-01-15TXX:YY">>)).

cast_time_no_colon_test() ->
    ?assertMatch({error, _}, kura_types:cast(utc_datetime, <<"2024-01-15T1030">>)).

%%----------------------------------------------------------------------
%% embed load through kura_types
%%----------------------------------------------------------------------

load_embed_one_from_binary_test() ->
    Json = <<"{\"street\":\"Main St\",\"city\":\"NY\",\"zip\":\"10001\"}">>,
    {ok, Result} = kura_types:load({embed, embeds_one, kura_test_address}, Json),
    ?assertEqual(<<"Main St">>, maps:get(street, Result)),
    ?assertEqual(<<"NY">>, maps:get(city, Result)).

load_embed_one_from_map_test() ->
    Map = #{<<"street">> => <<"Main St">>, <<"city">> => <<"NY">>},
    {ok, Result} = kura_types:load({embed, embeds_one, kura_test_address}, Map),
    ?assertEqual(<<"Main St">>, maps:get(street, Result)).

load_embed_one_invalid_json_test() ->
    ?assertMatch({error, _}, kura_types:load({embed, embeds_one, kura_test_address}, <<"[1,2]">>)).

load_embed_many_from_binary_test() ->
    Json = <<"[{\"label\":\"a\",\"weight\":1},{\"label\":\"b\",\"weight\":2}]">>,
    {ok, Result} = kura_types:load({embed, embeds_many, kura_test_label}, Json),
    ?assertEqual(2, length(Result)),
    ?assertEqual(<<"a">>, maps:get(label, hd(Result))).

load_embed_many_from_list_test() ->
    List = [#{<<"label">> => <<"a">>, <<"weight">> => 1}],
    {ok, Result} = kura_types:load({embed, embeds_many, kura_test_label}, List),
    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"a">>, maps:get(label, hd(Result))).

load_embed_many_invalid_json_test() ->
    ?assertMatch(
        {error, _}, kura_types:load({embed, embeds_many, kura_test_label}, <<"{\"a\":1}">>)
    ).

load_error_unknown_type_test() ->
    ?assertMatch({error, _}, kura_types:load(integer, <<"hello">>)).

%%----------------------------------------------------------------------
%% embed dump through kura_types
%%----------------------------------------------------------------------

dump_embed_one_test() ->
    Val = #{street => <<"Main St">>, city => <<"NY">>, zip => <<"10001">>},
    {ok, Json} = kura_types:dump({embed, embeds_one, kura_test_address}, Val),
    ?assert(is_binary(Json)),
    {ok, Decoded} = kura_types:load(jsonb, Json),
    ?assertEqual(<<"Main St">>, maps:get(<<"street">>, Decoded)).

dump_embed_many_test() ->
    Val = [#{label => <<"a">>, weight => 1}, #{label => <<"b">>, weight => 2}],
    {ok, Json} = kura_types:dump({embed, embeds_many, kura_test_label}, Val),
    ?assert(is_binary(Json)),
    {ok, Decoded} = kura_types:load(jsonb, Json),
    ?assertEqual(2, length(Decoded)).
