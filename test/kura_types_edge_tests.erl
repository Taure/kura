-module(kura_types_edge_tests).
-include_lib("eunit/include/eunit.hrl").

cast_uuid_32_byte_format_test() ->
    {ok, Result} = kura_types:cast(uuid, <<"550e8400e29b41d4a716446655440000">>),
    ?assertEqual(<<"550e8400-e29b-41d4-a716-446655440000">>, Result).

cast_uuid_invalid_length_test() ->
    ?assertMatch({error, _}, kura_types:cast(uuid, <<"short">>)).

cast_uuid_36_byte_passthrough_test() ->
    UUID = <<"550e8400-e29b-41d4-a716-446655440000">>,
    ?assertEqual({ok, UUID}, kura_types:cast(uuid, UUID)).

cast_date_wrong_format_test() ->
    ?assertMatch({error, _}, kura_types:cast(date, <<"2024/01/15">>)).

cast_date_too_short_test() ->
    ?assertMatch({error, _}, kura_types:cast(date, <<"2024">>)).

cast_datetime_no_time_separator_test() ->
    ?assertMatch({error, _}, kura_types:cast(utc_datetime, <<"20240115103000">>)).

cast_datetime_with_space_separator_test() ->
    {ok, Result} = kura_types:cast(utc_datetime, <<"2024-01-15 10:30:00">>),
    ?assertEqual({{2024, 1, 15}, {10, 30, 0}}, Result).

cast_datetime_with_fractional_seconds_test() ->
    {ok, Result} = kura_types:cast(utc_datetime, <<"2024-01-15T10:30:45.123Z">>),
    ?assertEqual({{2024, 1, 15}, {10, 30, 45}}, Result).

cast_datetime_with_tz_offset_test() ->
    {ok, Result} = kura_types:cast(utc_datetime, <<"2024-01-15T10:30:00+05:00">>),
    ?assertEqual({{2024, 1, 15}, {10, 30, 0}}, Result).

cast_datetime_with_negative_tz_offset_test() ->
    {ok, Result} = kura_types:cast(utc_datetime, <<"2024-01-15T10:30:00-03:00">>),
    ?assertEqual({{2024, 1, 15}, {10, 30, 0}}, Result).

cast_datetime_invalid_date_part_test() ->
    ?assertMatch({error, _}, kura_types:cast(utc_datetime, <<"XXXX-01-15T10:30:00Z">>)).

cast_datetime_invalid_hms_test() ->
    ?assertMatch({error, _}, kura_types:cast(utc_datetime, <<"2024-01-15TXX:YY:ZZ">>)).

cast_boolean_invalid_test() ->
    ?assertMatch({error, _}, kura_types:cast(boolean, <<"maybe">>)).

cast_text_atom_test() ->
    ?assertMatch({error, _}, kura_types:cast(text, hello)).

cast_string_integer_test() ->
    ?assertMatch({error, _}, kura_types:cast(string, 42)).

cast_integer_float_test() ->
    ?assertMatch({error, _}, kura_types:cast(integer, 3.14)).

cast_float_invalid_binary_test() ->
    ?assertMatch({error, _}, kura_types:cast(float, <<"not_a_number">>)).

cast_id_non_integer_non_binary_test() ->
    ?assertMatch({error, _}, kura_types:cast(id, [1, 2, 3])).

cast_date_non_binary_non_tuple_test() ->
    ?assertMatch({error, _}, kura_types:cast(date, 42)).

cast_enum_undefined_passthrough_test() ->
    ?assertEqual({ok, undefined}, kura_types:cast({enum, [a, b]}, undefined)).

cast_array_empty_test() ->
    ?assertEqual({ok, []}, kura_types:cast({array, integer}, [])).

cast_array_with_type_error_test() ->
    ?assertMatch({error, _}, kura_types:cast({array, integer}, [1, <<"bad">>, 3])).

cast_jsonb_non_string_non_map_non_list_test() ->
    ?assertMatch({error, _}, kura_types:cast(jsonb, 42)).

dump_string_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(string, 42)).

dump_text_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(text, 42)).

dump_boolean_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(boolean, <<"yes">>)).

dump_date_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(date, <<"2024-01-15">>)).

dump_uuid_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(uuid, 42)).

dump_id_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(id, <<"not_an_id">>)).

dump_utc_datetime_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(utc_datetime, <<"2024-01-15">>)).

load_id_error_test() ->
    ?assertMatch({error, _}, kura_types:load(id, <<"hello">>)).

load_string_error_test() ->
    ?assertMatch({error, _}, kura_types:load(string, 42)).

load_text_error_test() ->
    ?assertMatch({error, _}, kura_types:load(text, 42)).

load_boolean_error_test() ->
    ?assertMatch({error, _}, kura_types:load(boolean, <<"maybe">>)).

load_date_error_test() ->
    ?assertMatch({error, _}, kura_types:load(date, <<"2024-01-15">>)).

load_utc_datetime_error_test() ->
    ?assertMatch({error, _}, kura_types:load(utc_datetime, <<"2024-01-15">>)).

load_uuid_error_test() ->
    ?assertMatch({error, _}, kura_types:load(uuid, 42)).

load_jsonb_error_test() ->
    ?assertMatch({error, _}, kura_types:load(jsonb, 42)).

load_enum_atom_test() ->
    ?assertEqual({ok, active}, kura_types:load({enum, [active, inactive]}, <<"active">>)).

load_array_empty_test() ->
    ?assertEqual({ok, []}, kura_types:load({array, integer}, [])).

dump_array_empty_test() ->
    ?assertEqual({ok, []}, kura_types:dump({array, integer}, [])).

dump_enum_undefined_test() ->
    ?assertEqual({ok, null}, kura_types:dump({enum, [a, b]}, undefined)).

format_type_embed_test() ->
    ?assertEqual(<<"JSONB">>, kura_types:to_pg_type({embed, embeds_one, some_mod})).

format_type_array_text_test() ->
    ?assertEqual(<<"TEXT[]">>, kura_types:to_pg_type({array, text})).

format_type_array_uuid_test() ->
    ?assertEqual(<<"UUID[]">>, kura_types:to_pg_type({array, uuid})).

cast_embed_one_non_map_test() ->
    ?assertMatch({error, _}, kura_types:cast({embed, embeds_one, some_mod}, [1, 2])).

cast_embed_many_non_list_test() ->
    ?assertMatch({error, _}, kura_types:cast({embed, embeds_many, some_mod}, #{a => 1})).

dump_embed_one_non_map_test() ->
    ?assertMatch({error, _}, kura_types:dump({embed, embeds_one, some_mod}, [1, 2])).

dump_embed_many_non_list_test() ->
    ?assertMatch({error, _}, kura_types:dump({embed, embeds_many, some_mod}, #{a => 1})).

load_embed_one_invalid_json_object_test() ->
    ?assertMatch(
        {error, _},
        kura_types:load({embed, embeds_one, kura_test_address}, <<"[1,2,3]">>)
    ).

load_embed_many_invalid_json_array_test() ->
    ?assertMatch(
        {error, _},
        kura_types:load({embed, embeds_many, kura_test_label}, <<"{\"a\":1}">>)
    ).

load_embed_one_bad_json_test() ->
    ?assertMatch(
        {error, _},
        kura_types:load({embed, embeds_one, kura_test_address}, <<"not json">>)
    ).

load_embed_many_bad_json_test() ->
    ?assertMatch(
        {error, _},
        kura_types:load({embed, embeds_many, kura_test_label}, <<"not json">>)
    ).

dump_jsonb_encode_error_test() ->
    ?assertMatch({error, _}, kura_types:dump(jsonb, #{<<"f">> => fun() -> ok end})).

cast_null_to_any_type_test_() ->
    Types = [id, integer, float, string, text, boolean, date, utc_datetime, uuid, jsonb],
    [?_assertEqual({ok, undefined}, kura_types:cast(T, null)) || T <- Types].

cast_undefined_to_any_type_test_() ->
    Types = [id, integer, float, string, text, boolean, date, utc_datetime, uuid, jsonb],
    [?_assertEqual({ok, undefined}, kura_types:cast(T, undefined)) || T <- Types].

dump_undefined_to_any_type_test_() ->
    Types = [id, integer, float, string, text, boolean, date, utc_datetime, uuid, jsonb],
    [?_assertEqual({ok, null}, kura_types:dump(T, undefined)) || T <- Types].

load_null_to_any_type_test_() ->
    Types = [id, integer, float, string, text, boolean, date, utc_datetime, uuid, jsonb],
    [?_assertEqual({ok, undefined}, kura_types:load(T, null)) || T <- Types].
