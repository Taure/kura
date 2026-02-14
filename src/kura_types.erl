-module(kura_types).
-moduledoc """
Type system for casting, dumping, and loading values between Erlang and PostgreSQL.

Supported types: `id`, `integer`, `float`, `string`, `text`, `boolean`,
`date`, `utc_datetime`, `uuid`, `jsonb`, `{array, Type}`.

- `cast/2` — coerce external input to Erlang terms
- `dump/2` — convert Erlang terms to pgo-compatible values
- `load/2` — convert pgo results back to Erlang terms
""".

-export([to_pg_type/1, cast/2, dump/2, load/2]).

-export_type([kura_type/0]).

-type kura_type() ::
    id
    | integer
    | float
    | string
    | text
    | boolean
    | date
    | utc_datetime
    | uuid
    | jsonb
    | {enum, [atom()]}
    | {array, kura_type()}
    | {embed, embeds_one | embeds_many, module()}.

%%----------------------------------------------------------------------
%% PG DDL type strings
%%----------------------------------------------------------------------

-doc "Return the PostgreSQL DDL type string for a kura type.".
-spec to_pg_type(kura_type()) -> binary().
to_pg_type(id) -> <<"BIGSERIAL">>;
to_pg_type(integer) -> <<"INTEGER">>;
to_pg_type(float) -> <<"DOUBLE PRECISION">>;
to_pg_type(string) -> <<"VARCHAR(255)">>;
to_pg_type(text) -> <<"TEXT">>;
to_pg_type(boolean) -> <<"BOOLEAN">>;
to_pg_type(date) -> <<"DATE">>;
to_pg_type(utc_datetime) -> <<"TIMESTAMPTZ">>;
to_pg_type(uuid) -> <<"UUID">>;
to_pg_type(jsonb) -> <<"JSONB">>;
to_pg_type({enum, _}) -> <<"VARCHAR(255)">>;
to_pg_type({array, Inner}) -> <<(to_pg_type(Inner))/binary, "[]">>;
to_pg_type({embed, _, _}) -> <<"JSONB">>.

%%----------------------------------------------------------------------
%% Cast: coerce external input → Erlang term
%%----------------------------------------------------------------------

-doc "Coerce external input to an Erlang term for the given type.".
-spec cast(kura_type(), term()) -> {ok, term()} | {error, binary()}.
cast(_Type, undefined) ->
    {ok, undefined};
cast(_Type, null) ->
    {ok, undefined};
cast(id, V) when is_integer(V) ->
    {ok, V};
cast(id, V) when is_binary(V) ->
    try_parse_integer(V);
cast(integer, V) when is_integer(V) ->
    {ok, V};
cast(integer, V) when is_binary(V) ->
    try_parse_integer(V);
cast(float, V) when is_float(V) ->
    {ok, V};
cast(float, V) when is_integer(V) ->
    {ok, erlang:float(V)};
cast(float, V) when is_binary(V) ->
    try_parse_float(V);
cast(string, V) when is_binary(V) ->
    {ok, V};
cast(string, V) when is_list(V) ->
    {ok, list_to_binary(V)};
cast(string, V) when is_atom(V) ->
    {ok, atom_to_binary(V, utf8)};
cast(text, V) when is_binary(V) ->
    {ok, V};
cast(text, V) when is_list(V) ->
    {ok, list_to_binary(V)};
cast(boolean, V) when is_boolean(V) ->
    {ok, V};
cast(boolean, <<"true">>) ->
    {ok, true};
cast(boolean, <<"false">>) ->
    {ok, false};
cast(boolean, <<"1">>) ->
    {ok, true};
cast(boolean, <<"0">>) ->
    {ok, false};
cast(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
cast(date, V) when is_binary(V) ->
    parse_date(V);
cast(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
cast(utc_datetime, V) when is_binary(V) ->
    parse_datetime(V);
cast(uuid, V) when is_binary(V), byte_size(V) =:= 36 ->
    {ok, V};
cast(uuid, V) when is_binary(V), byte_size(V) =:= 32 ->
    {ok, format_uuid(V)};
cast(jsonb, V) when is_map(V) ->
    {ok, V};
cast(jsonb, V) when is_list(V) ->
    {ok, V};
cast(jsonb, V) when is_binary(V) ->
    json_decode(V);
cast({enum, Values}, V) when is_atom(V) ->
    case lists:member(V, Values) of
        true -> {ok, V};
        false -> {error, <<"is not a valid enum value">>}
    end;
cast({enum, Values}, V) when is_binary(V) ->
    try
        Atom = binary_to_existing_atom(V, utf8),
        case lists:member(Atom, Values) of
            true -> {ok, Atom};
            false -> {error, <<"is not a valid enum value">>}
        end
    catch
        error:badarg -> {error, <<"is not a valid enum value">>}
    end;
cast({enum, Values}, V) when is_list(V) ->
    try
        Atom = list_to_existing_atom(V),
        case lists:member(Atom, Values) of
            true -> {ok, Atom};
            false -> {error, <<"is not a valid enum value">>}
        end
    catch
        error:badarg -> {error, <<"is not a valid enum value">>}
    end;
cast({array, Inner}, V) when is_list(V) ->
    cast_array(Inner, V, []);
cast({embed, embeds_one, _}, V) when is_map(V) ->
    {ok, V};
cast({embed, embeds_many, _}, V) when is_list(V) ->
    {ok, V};
cast(Type, _V) ->
    {error, <<"cannot cast to ", (atom_to_binary(format_type(Type), utf8))/binary>>}.

%%----------------------------------------------------------------------
%% Dump: Erlang term → pgo-compatible value
%%----------------------------------------------------------------------

-doc "Convert an Erlang term to a pgo-compatible value for storage.".
-spec dump(kura_type(), term()) -> {ok, term()} | {error, binary()}.
dump(_Type, undefined) ->
    {ok, null};
dump(id, V) when is_integer(V) ->
    {ok, V};
dump(integer, V) when is_integer(V) ->
    {ok, V};
dump(float, V) when is_float(V) ->
    {ok, V};
dump(float, V) when is_integer(V) ->
    {ok, erlang:float(V)};
dump(string, V) when is_binary(V) ->
    {ok, V};
dump(text, V) when is_binary(V) ->
    {ok, V};
dump(boolean, V) when is_boolean(V) ->
    {ok, V};
dump(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
dump(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
dump(uuid, V) when is_binary(V) ->
    {ok, V};
dump(jsonb, V) when is_map(V); is_list(V) ->
    json_encode(V);
dump({enum, _}, V) when is_atom(V) ->
    {ok, atom_to_binary(V, utf8)};
dump({array, Inner}, V) when is_list(V) ->
    dump_array(Inner, V, []);
dump({embed, embeds_one, Mod}, V) when is_map(V) ->
    json_encode(dump_embed_to_term(Mod, V));
dump({embed, embeds_many, Mod}, V) when is_list(V) ->
    json_encode([dump_embed_to_term(Mod, Item) || Item <- V]);
dump(Type, _V) ->
    {error, <<"cannot dump ", (atom_to_binary(format_type(Type), utf8))/binary>>}.

%%----------------------------------------------------------------------
%% Load: pgo result → Erlang term
%%----------------------------------------------------------------------

-doc "Convert a pgo result value back to an Erlang term.".
-spec load(kura_type(), term()) -> {ok, term()} | {error, binary()}.
load(_Type, null) ->
    {ok, undefined};
load(id, V) when is_integer(V) ->
    {ok, V};
load(integer, V) when is_integer(V) ->
    {ok, V};
load(float, V) when is_number(V) ->
    {ok, erlang:float(V)};
load(string, V) when is_binary(V) ->
    {ok, V};
load(text, V) when is_binary(V) ->
    {ok, V};
load(boolean, V) when is_boolean(V) ->
    {ok, V};
load(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
load(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
load(uuid, V) when is_binary(V) ->
    {ok, V};
load(jsonb, V) when is_binary(V) ->
    json_decode(V);
load(jsonb, V) when is_map(V) ->
    {ok, V};
load({enum, _}, V) when is_binary(V) ->
    try
        {ok, binary_to_existing_atom(V, utf8)}
    catch
        error:badarg -> {ok, binary_to_atom(V, utf8)}
    end;
load({array, Inner}, V) when is_list(V) ->
    load_array(Inner, V, []);
load({embed, embeds_one, Mod}, V) when is_binary(V) ->
    case json_decode(V) of
        {ok, Map} when is_map(Map) -> {ok, load_embed_map(Mod, Map)};
        {ok, _} -> {error, <<"expected a JSON object">>};
        Err -> Err
    end;
load({embed, embeds_one, Mod}, V) when is_map(V) ->
    {ok, load_embed_map(Mod, V)};
load({embed, embeds_many, Mod}, V) when is_binary(V) ->
    case json_decode(V) of
        {ok, List} when is_list(List) -> {ok, [load_embed_map(Mod, M) || M <- List]};
        {ok, _} -> {error, <<"expected a JSON array">>};
        Err -> Err
    end;
load({embed, embeds_many, Mod}, V) when is_list(V) ->
    {ok, [load_embed_map(Mod, M) || M <- V]};
load(Type, _V) ->
    {error, <<"cannot load ", (atom_to_binary(format_type(Type), utf8))/binary>>}.

%%----------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------

try_parse_integer(Bin) ->
    try
        {ok, binary_to_integer(Bin)}
    catch
        error:badarg -> {error, <<"is not a valid integer">>}
    end.

try_parse_float(Bin) ->
    try
        {ok, binary_to_float(Bin)}
    catch
        error:badarg ->
            try
                {ok, erlang:float(binary_to_integer(Bin))}
            catch
                error:badarg -> {error, <<"is not a valid float">>}
            end
    end.

parse_date(<<Y:4/binary, $-, M:2/binary, $-, D:2/binary>>) ->
    try
        {ok, {binary_to_integer(Y), binary_to_integer(M), binary_to_integer(D)}}
    catch
        error:badarg -> {error, <<"is not a valid date">>}
    end;
parse_date(_) ->
    {error, <<"is not a valid date">>}.

parse_datetime(Bin) ->
    case binary:split(Bin, [<<"T">>, <<" ">>]) of
        [DatePart, TimePart] ->
            case parse_date(DatePart) of
                {ok, Date} ->
                    case parse_time(TimePart) of
                        {ok, Time} -> {ok, {Date, Time}};
                        Err -> Err
                    end;
                Err ->
                    Err
            end;
        _ ->
            {error, <<"is not a valid datetime">>}
    end.

parse_time(Bin) ->
    Stripped = strip_tz(Bin),
    case binary:split(Stripped, <<":">>, [global]) of
        [H, M, S] ->
            try
                Sec =
                    case binary:split(S, <<".">>) of
                        [SecWhole, _Frac] -> binary_to_integer(SecWhole);
                        [SecWhole] -> binary_to_integer(SecWhole)
                    end,
                {ok, {binary_to_integer(H), binary_to_integer(M), Sec}}
            catch
                error:badarg -> {error, <<"is not a valid time">>}
            end;
        [H, M] ->
            try
                {ok, {binary_to_integer(H), binary_to_integer(M), 0}}
            catch
                error:badarg -> {error, <<"is not a valid time">>}
            end;
        _ ->
            {error, <<"is not a valid time">>}
    end.

strip_tz(Bin) ->
    case binary:last(Bin) of
        $Z ->
            binary:part(Bin, 0, byte_size(Bin) - 1);
        _ ->
            case binary:match(Bin, [<<"+">>, <<"-">>], [{scope, {3, byte_size(Bin) - 3}}]) of
                {Pos, _} -> binary:part(Bin, 0, Pos);
                nomatch -> Bin
            end
    end.

format_uuid(<<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>>) ->
    <<A/binary, $-, B/binary, $-, C/binary, $-, D/binary, $-, E/binary>>;
format_uuid(V) ->
    V.

json_encode(V) ->
    try
        {ok, iolist_to_binary(json:encode(V))}
    catch
        _:_ -> {error, <<"cannot encode as JSON">>}
    end.

json_decode(Bin) ->
    try
        {ok, json:decode(Bin)}
    catch
        _:_ -> {error, <<"is not valid JSON">>}
    end.

cast_array(_Inner, [], Acc) ->
    {ok, lists:reverse(Acc)};
cast_array(Inner, [H | T], Acc) ->
    case cast(Inner, H) of
        {ok, V} -> cast_array(Inner, T, [V | Acc]);
        {error, _} = Err -> Err
    end.

dump_array(_Inner, [], Acc) ->
    {ok, lists:reverse(Acc)};
dump_array(Inner, [H | T], Acc) ->
    case dump(Inner, H) of
        {ok, V} -> dump_array(Inner, T, [V | Acc]);
        {error, _} = Err -> Err
    end.

load_array(_Inner, [], Acc) ->
    {ok, lists:reverse(Acc)};
load_array(Inner, [H | T], Acc) ->
    case load(Inner, H) of
        {ok, V} -> load_array(Inner, T, [V | Acc]);
        {error, _} = Err -> Err
    end.

format_type({enum, _}) -> enum;
format_type({array, Inner}) -> list_to_atom("array_" ++ atom_to_list(format_type(Inner)));
format_type({embed, _, Mod}) -> Mod;
format_type(T) when is_atom(T) -> T.

dump_embed_to_term(Mod, Map) ->
    Types = kura_schema:field_types(Mod),
    NonVirtual = kura_schema:non_virtual_fields(Mod),
    lists:foldl(
        fun(Field, Acc) ->
            case maps:find(Field, Map) of
                {ok, Val} ->
                    BinKey = atom_to_binary(Field, utf8),
                    case maps:find(Field, Types) of
                        {ok, {embed, embeds_one, ChildMod}} when is_map(Val) ->
                            Acc#{BinKey => dump_embed_to_term(ChildMod, Val)};
                        {ok, {embed, embeds_many, ChildMod}} when is_list(Val) ->
                            Acc#{BinKey => [dump_embed_to_term(ChildMod, I) || I <- Val]};
                        {ok, Type} ->
                            case dump(Type, Val) of
                                {ok, Dumped} -> Acc#{BinKey => Dumped};
                                {error, _} -> Acc#{BinKey => Val}
                            end;
                        error ->
                            Acc#{BinKey => Val}
                    end;
                error ->
                    Acc
            end
        end,
        #{},
        NonVirtual
    ).

load_embed_map(Mod, Map) ->
    Types = kura_schema:field_types(Mod),
    maps:fold(
        fun(K, V, Acc) ->
            AtomKey =
                if
                    is_binary(K) ->
                        try
                            binary_to_existing_atom(K, utf8)
                        catch
                            error:badarg -> K
                        end;
                    is_atom(K) ->
                        K;
                    true ->
                        K
                end,
            case maps:find(AtomKey, Types) of
                {ok, {embed, embeds_one, ChildMod}} when is_map(V) ->
                    Acc#{AtomKey => load_embed_map(ChildMod, V)};
                {ok, {embed, embeds_many, ChildMod}} when is_list(V) ->
                    Acc#{AtomKey => [load_embed_map(ChildMod, I) || I <- V]};
                {ok, Type} ->
                    case load(Type, V) of
                        {ok, Loaded} -> Acc#{AtomKey => Loaded};
                        {error, _} -> Acc#{AtomKey => V}
                    end;
                error ->
                    Acc#{AtomKey => V}
            end
        end,
        #{},
        Map
    ).
