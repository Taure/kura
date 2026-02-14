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
        ?_assertMatch({error, _}, kura_types:cast(date, <<"not-a-date">>))
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
        ?_assertMatch({error, _}, kura_types:cast(utc_datetime, <<"garbage">>))
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
        ?_assertEqual({ok, #{<<"a">> => 1}}, kura_types:cast(jsonb, <<"{\"a\":1}">>)),
        ?_assertMatch({error, _}, kura_types:cast(jsonb, <<"not json">>))
    ].

cast_array_test_() ->
    [
        ?_assertEqual({ok, [1, 2, 3]}, kura_types:cast({array, integer}, [1, 2, 3])),
        ?_assertEqual(
            {ok, [1, 2, 3]}, kura_types:cast({array, integer}, [<<"1">>, <<"2">>, <<"3">>])
        ),
        ?_assertMatch({error, _}, kura_types:cast({array, integer}, [<<"a">>]))
    ].

%%----------------------------------------------------------------------
%% dump
%%----------------------------------------------------------------------

dump_null_test() ->
    ?assertEqual({ok, null}, kura_types:dump(integer, undefined)).

dump_integer_test() ->
    ?assertEqual({ok, 42}, kura_types:dump(integer, 42)).

dump_jsonb_test() ->
    {ok, Encoded} = kura_types:dump(jsonb, #{<<"key">> => <<"val">>}),
    ?assert(is_binary(Encoded)).

dump_array_test() ->
    ?assertEqual({ok, [1, 2, 3]}, kura_types:dump({array, integer}, [1, 2, 3])).

%%----------------------------------------------------------------------
%% load
%%----------------------------------------------------------------------

load_null_test() ->
    ?assertEqual({ok, undefined}, kura_types:load(integer, null)).

load_integer_test() ->
    ?assertEqual({ok, 42}, kura_types:load(integer, 42)).

load_jsonb_binary_test() ->
    ?assertEqual({ok, #{<<"a">> => 1}}, kura_types:load(jsonb, <<"{\"a\":1}">>)).

load_jsonb_map_test() ->
    ?assertEqual({ok, #{<<"a">> => 1}}, kura_types:load(jsonb, #{<<"a">> => 1})).

load_array_test() ->
    ?assertEqual({ok, [1, 2, 3]}, kura_types:load({array, integer}, [1, 2, 3])).
