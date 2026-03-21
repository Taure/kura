-module(kura_types_prop_tests).
-moduledoc "Property-based tests for kura_types cast/dump/load.".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqWAlizer: proper macros generate term() — suppress affected functions
-eqwalizer({nowarn_function, gen_binary_string/0}).
-eqwalizer({nowarn_function, gen_uuid_hex/0}).
-eqwalizer({nowarn_function, prop_uuid_36_roundtrip/0}).
-eqwalizer({nowarn_function, prop_uuid_32_formats/0}).
-eqwalizer({nowarn_function, prop_uuid_16_formats/0}).
-eqwalizer({nowarn_function, prop_cast_undefined/0}).
-eqwalizer({nowarn_function, prop_dump_undefined/0}).
-eqwalizer({nowarn_function, prop_load_null/0}).
-eqwalizer({nowarn_function, prop_id_cast_binary/0}).
-eqwalizer({nowarn_function, prop_float_cast_from_int/0}).

-define(NUMTESTS, 200).

%%----------------------------------------------------------------------
%% EUnit wrapper — runs PropEr tests inside EUnit
%%----------------------------------------------------------------------

property_test_() ->
    {timeout, 120, [
        {"integer cast/dump/load roundtrip", fun() -> run(prop_integer_roundtrip()) end},
        {"float cast/dump/load roundtrip", fun() -> run(prop_float_roundtrip()) end},
        {"string cast/dump/load roundtrip", fun() -> run(prop_string_roundtrip()) end},
        {"text cast/dump/load roundtrip", fun() -> run(prop_text_roundtrip()) end},
        {"boolean cast/dump/load roundtrip", fun() -> run(prop_boolean_roundtrip()) end},
        {"date cast/dump/load roundtrip", fun() -> run(prop_date_roundtrip()) end},
        {"utc_datetime cast/dump/load roundtrip", fun() -> run(prop_datetime_roundtrip()) end},
        {"uuid 36-char roundtrip", fun() -> run(prop_uuid_36_roundtrip()) end},
        {"uuid 32-char formats to 36", fun() -> run(prop_uuid_32_formats()) end},
        {"uuid 16-byte formats to 36", fun() -> run(prop_uuid_16_formats()) end},
        {"uuid cast idempotent", fun() -> run(prop_uuid_cast_idempotent()) end},
        {"enum cast/dump/load roundtrip", fun() -> run(prop_enum_roundtrip()) end},
        {"enum rejects invalid", fun() -> run(prop_enum_rejects_invalid()) end},
        {"integer array roundtrip", fun() -> run(prop_array_int_roundtrip()) end},
        {"string array roundtrip", fun() -> run(prop_array_string_roundtrip()) end},
        {"cast undefined always ok", fun() -> run(prop_cast_undefined()) end},
        {"dump undefined always null", fun() -> run(prop_dump_undefined()) end},
        {"load null always undefined", fun() -> run(prop_load_null()) end},
        {"id cast from binary", fun() -> run(prop_id_cast_binary()) end},
        {"boolean cast from binary strings", fun() -> run(prop_boolean_cast_binary()) end},
        {"float cast from integer", fun() -> run(prop_float_cast_from_int()) end}
    ]}.

run(Prop) ->
    ?assert(proper:quickcheck(Prop, [?NUMTESTS, {to_file, user}, noshrink])).

%%----------------------------------------------------------------------
%% Generators
%%----------------------------------------------------------------------

gen_binary_string() ->
    ?LET(Chars, list(range($a, $z)), list_to_binary(Chars)).

gen_date() ->
    ?LET(
        {Y, M, D},
        {range(1970, 2100), range(1, 12), range(1, 28)},
        {Y, M, D}
    ).

gen_datetime() ->
    ?LET(
        {Date, {H, Mi, S}},
        {gen_date(), {range(0, 23), range(0, 59), range(0, 59)}},
        {Date, {H, Mi, S}}
    ).

gen_uuid_hex() ->
    ?LET(
        Bytes,
        binary(16),
        binary:encode_hex(Bytes, lowercase)
    ).

gen_uuid_formatted() ->
    ?LET(
        Hex,
        gen_uuid_hex(),
        begin
            <<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>> = Hex,
            <<A/binary, "-", B/binary, "-", C/binary, "-", D/binary, "-", E/binary>>
        end
    ).

gen_simple_type() ->
    oneof([id, integer, float, string, text, boolean, date, utc_datetime, uuid]).

%%----------------------------------------------------------------------
%% Roundtrip properties: cast -> dump -> load should preserve the value
%%----------------------------------------------------------------------

prop_integer_roundtrip() ->
    ?FORALL(
        V,
        integer(),
        begin
            {ok, Cast} = kura_types:cast(integer, V),
            {ok, Dumped} = kura_types:dump(integer, Cast),
            {ok, Loaded} = kura_types:load(integer, Dumped),
            Loaded =:= V
        end
    ).

prop_float_roundtrip() ->
    ?FORALL(
        V,
        float(),
        begin
            {ok, Cast} = kura_types:cast(float, V),
            {ok, Dumped} = kura_types:dump(float, Cast),
            {ok, Loaded} = kura_types:load(float, Dumped),
            Loaded =:= V
        end
    ).

prop_string_roundtrip() ->
    ?FORALL(
        V,
        gen_binary_string(),
        begin
            {ok, Cast} = kura_types:cast(string, V),
            {ok, Dumped} = kura_types:dump(string, Cast),
            {ok, Loaded} = kura_types:load(string, Dumped),
            Loaded =:= V
        end
    ).

prop_text_roundtrip() ->
    ?FORALL(
        V,
        gen_binary_string(),
        begin
            {ok, Cast} = kura_types:cast(text, V),
            {ok, Dumped} = kura_types:dump(text, Cast),
            {ok, Loaded} = kura_types:load(text, Dumped),
            Loaded =:= V
        end
    ).

prop_boolean_roundtrip() ->
    ?FORALL(
        V,
        boolean(),
        begin
            {ok, Cast} = kura_types:cast(boolean, V),
            {ok, Dumped} = kura_types:dump(boolean, Cast),
            {ok, Loaded} = kura_types:load(boolean, Dumped),
            Loaded =:= V
        end
    ).

prop_date_roundtrip() ->
    ?FORALL(
        V,
        gen_date(),
        begin
            {ok, Cast} = kura_types:cast(date, V),
            {ok, Dumped} = kura_types:dump(date, Cast),
            {ok, Loaded} = kura_types:load(date, Dumped),
            Loaded =:= V
        end
    ).

prop_datetime_roundtrip() ->
    ?FORALL(
        V,
        gen_datetime(),
        begin
            {ok, Cast} = kura_types:cast(utc_datetime, V),
            {ok, Dumped} = kura_types:dump(utc_datetime, Cast),
            {ok, Loaded} = kura_types:load(utc_datetime, Dumped),
            Loaded =:= V
        end
    ).

%%----------------------------------------------------------------------
%% UUID properties
%%----------------------------------------------------------------------

prop_uuid_36_roundtrip() ->
    ?FORALL(
        V,
        gen_uuid_formatted(),
        begin
            {ok, Cast} = kura_types:cast(uuid, V),
            {ok, Dumped} = kura_types:dump(uuid, Cast),
            {ok, Loaded} = kura_types:load(uuid, Dumped),
            Loaded =:= V andalso byte_size(Cast) =:= 36
        end
    ).

prop_uuid_32_formats() ->
    ?FORALL(
        Hex,
        gen_uuid_hex(),
        begin
            {ok, Formatted} = kura_types:cast(uuid, Hex),
            byte_size(Formatted) =:= 36 andalso
                binary:at(Formatted, 8) =:= $- andalso
                binary:at(Formatted, 13) =:= $- andalso
                binary:at(Formatted, 18) =:= $- andalso
                binary:at(Formatted, 23) =:= $-
        end
    ).

prop_uuid_16_formats() ->
    ?FORALL(
        Raw,
        binary(16),
        begin
            {ok, Formatted} = kura_types:cast(uuid, Raw),
            byte_size(Formatted) =:= 36
        end
    ).

prop_uuid_cast_idempotent() ->
    ?FORALL(
        V,
        gen_uuid_formatted(),
        begin
            {ok, Once} = kura_types:cast(uuid, V),
            {ok, Twice} = kura_types:cast(uuid, Once),
            Once =:= Twice
        end
    ).

%%----------------------------------------------------------------------
%% Enum properties
%%----------------------------------------------------------------------

prop_enum_roundtrip() ->
    EnumType = {enum, [active, inactive, banned]},
    ?FORALL(
        V,
        oneof([active, inactive, banned]),
        begin
            {ok, Cast} = kura_types:cast(EnumType, V),
            {ok, Dumped} = kura_types:dump(EnumType, Cast),
            {ok, Loaded} = kura_types:load(EnumType, Dumped),
            Loaded =:= V
        end
    ).

prop_enum_rejects_invalid() ->
    EnumType = {enum, [active, inactive, banned]},
    ?FORALL(
        V,
        oneof([foo, bar, baz, unknown, deleted]),
        begin
            case kura_types:cast(EnumType, V) of
                {error, _} -> true;
                {ok, _} -> false
            end
        end
    ).

%%----------------------------------------------------------------------
%% Array properties
%%----------------------------------------------------------------------

prop_array_int_roundtrip() ->
    ?FORALL(
        V,
        list(integer()),
        begin
            {ok, Cast} = kura_types:cast({array, integer}, V),
            {ok, Dumped} = kura_types:dump({array, integer}, Cast),
            {ok, Loaded} = kura_types:load({array, integer}, Dumped),
            Loaded =:= V
        end
    ).

prop_array_string_roundtrip() ->
    ?FORALL(
        V,
        list(gen_binary_string()),
        begin
            {ok, Cast} = kura_types:cast({array, string}, V),
            {ok, Dumped} = kura_types:dump({array, string}, Cast),
            {ok, Loaded} = kura_types:load({array, string}, Dumped),
            Loaded =:= V
        end
    ).

%%----------------------------------------------------------------------
%% Null/undefined handling
%%----------------------------------------------------------------------

prop_cast_undefined() ->
    ?FORALL(
        Type,
        gen_simple_type(),
        begin
            {ok, undefined} =:= kura_types:cast(Type, undefined)
        end
    ).

prop_dump_undefined() ->
    ?FORALL(
        Type,
        gen_simple_type(),
        begin
            {ok, null} =:= kura_types:dump(Type, undefined)
        end
    ).

prop_load_null() ->
    ?FORALL(
        Type,
        gen_simple_type(),
        begin
            {ok, undefined} =:= kura_types:load(Type, null)
        end
    ).

%%----------------------------------------------------------------------
%% Cast coercion properties
%%----------------------------------------------------------------------

prop_id_cast_binary() ->
    ?FORALL(
        V,
        pos_integer(),
        begin
            Bin = integer_to_binary(V),
            {ok, V} =:= kura_types:cast(id, Bin)
        end
    ).

prop_boolean_cast_binary() ->
    ?FORALL(
        {Bin, Expected},
        oneof([{~"true", true}, {~"false", false}, {~"1", true}, {~"0", false}]),
        begin
            {ok, Expected} =:= kura_types:cast(boolean, Bin)
        end
    ).

prop_float_cast_from_int() ->
    ?FORALL(
        V,
        integer(),
        begin
            {ok, F} = kura_types:cast(float, V),
            is_float(F) andalso F =:= erlang:float(V)
        end
    ).
