-module(kura_types_tests).
-include_lib("eunit/include/eunit.hrl").

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
        %% list enum — invalid existing atom
        ?_assertMatch({error, _}, kura_types:cast({enum, [active, inactive]}, "banned")),
        %% list enum — nonexistent atom
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
%% dump — all types
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
%% load — all types
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
