-module(kura_embed_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Schema: embeds/1, embed/2
%%----------------------------------------------------------------------

embeds_returns_list_test() ->
    Embeds = kura_schema:embeds(kura_test_profile),
    ?assertEqual(2, length(Embeds)).

embed_found_test() ->
    {ok, Embed} = kura_schema:embed(kura_test_profile, address),
    ?assertEqual(address, Embed#kura_embed.name),
    ?assertEqual(embeds_one, Embed#kura_embed.type),
    ?assertEqual(kura_test_address, Embed#kura_embed.schema).

embed_not_found_test() ->
    ?assertEqual({error, not_found}, kura_schema:embed(kura_test_profile, nonexistent)).

field_types_includes_embeds_test() ->
    Types = kura_schema:field_types(kura_test_profile),
    ?assertEqual({embed, embeds_one, kura_test_address}, maps:get(address, Types)),
    ?assertEqual({embed, embeds_many, kura_test_label}, maps:get(labels, Types)).

column_map_includes_embeds_test() ->
    ColMap = kura_schema:column_map(kura_test_profile),
    ?assertEqual(<<"address">>, maps:get(address, ColMap)),
    ?assertEqual(<<"labels">>, maps:get(labels, ColMap)).

non_virtual_includes_embeds_test() ->
    NV = kura_schema:non_virtual_fields(kura_test_profile),
    ?assert(lists:member(address, NV)),
    ?assert(lists:member(labels, NV)).

%%----------------------------------------------------------------------
%% Changeset: cast_embed
%%----------------------------------------------------------------------

cast_embed_one_test() ->
    Params = #{
        name => <<"Alice">>,
        bio => <<"Developer">>,
        address => #{street => <<"123 Main St">>, city => <<"Portland">>, zip => <<"97201">>}
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name, bio]),
    CS1 = kura_changeset:cast_embed(CS, address),
    ?assert(CS1#kura_changeset.valid),
    #{address := Addr} = CS1#kura_changeset.changes,
    ?assertEqual(<<"123 Main St">>, maps:get(street, Addr)),
    ?assertEqual(<<"Portland">>, maps:get(city, Addr)),
    ?assertEqual(<<"97201">>, maps:get(zip, Addr)).

cast_embed_one_invalid_test() ->
    Params = #{
        name => <<"Alice">>,
        address => #{street => 123}
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    WithFun = fun(_Data, EmbedParams) ->
        ChildCS = kura_changeset:cast(kura_test_address, #{}, EmbedParams, [street, city, zip]),
        kura_changeset:validate_required(ChildCS, [street, city])
    end,
    CS1 = kura_changeset:cast_embed(CS, address, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid).

cast_embed_one_no_params_test() ->
    Params = #{name => <<"Alice">>},
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, address),
    ?assert(CS1#kura_changeset.valid),
    ?assertNot(maps:is_key(address, CS1#kura_changeset.changes)).

cast_embed_one_bad_type_test() ->
    Params = #{name => <<"Alice">>, address => [1, 2, 3]},
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, address),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{address, <<"expected a map">>}], CS1#kura_changeset.errors).

cast_embed_many_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{label => <<"important">>, weight => 10},
            #{label => <<"urgent">>, weight => 5}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, labels),
    ?assert(CS1#kura_changeset.valid),
    #{labels := Labels} = CS1#kura_changeset.changes,
    ?assertEqual(2, length(Labels)),
    [L1, L2] = Labels,
    ?assertEqual(<<"important">>, maps:get(label, L1)),
    ?assertEqual(10, maps:get(weight, L1)),
    ?assertEqual(<<"urgent">>, maps:get(label, L2)).

cast_embed_many_invalid_test() ->
    Params = #{
        name => <<"Alice">>,
        labels => [
            #{label => <<"ok">>, weight => 1},
            #{weight => <<"not_an_int">>}
        ]
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    WithFun = fun(_Data, EmbedParams) ->
        ChildCS = kura_changeset:cast(kura_test_label, #{}, EmbedParams, [label, weight]),
        kura_changeset:validate_required(ChildCS, [label])
    end,
    CS1 = kura_changeset:cast_embed(CS, labels, #{with => WithFun}),
    ?assertNot(CS1#kura_changeset.valid).

cast_embed_many_bad_type_test() ->
    Params = #{name => <<"Alice">>, labels => #{key => <<"a list">>}},
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    CS1 = kura_changeset:cast_embed(CS, labels),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{labels, <<"expected a list">>}], CS1#kura_changeset.errors).

cast_embed_unknown_test() ->
    CS = kura_changeset:cast(kura_test_profile, #{}, #{name => <<"Alice">>}, [name]),
    CS1 = kura_changeset:cast_embed(CS, nonexistent),
    ?assertNot(CS1#kura_changeset.valid),
    ?assertMatch([{nonexistent, <<"unknown embed">>}], CS1#kura_changeset.errors).

cast_embed_custom_with_test() ->
    Params = #{
        name => <<"Alice">>,
        address => #{street => <<"456 Oak Ave">>, city => <<"Seattle">>, zip => <<"98101">>}
    },
    CS = kura_changeset:cast(kura_test_profile, #{}, Params, [name]),
    WithFun = fun(_Data, EmbedParams) ->
        ChildCS = kura_changeset:cast(kura_test_address, #{}, EmbedParams, [street, city]),
        kura_changeset:validate_required(ChildCS, [street, city])
    end,
    CS1 = kura_changeset:cast_embed(CS, address, #{with => WithFun}),
    ?assert(CS1#kura_changeset.valid),
    #{address := Addr} = CS1#kura_changeset.changes,
    ?assertEqual(<<"456 Oak Ave">>, maps:get(street, Addr)),
    ?assertEqual(<<"Seattle">>, maps:get(city, Addr)),
    ?assertNot(maps:is_key(zip, Addr)).

%%----------------------------------------------------------------------
%% Types: dump/load embed
%%----------------------------------------------------------------------

dump_embed_one_test() ->
    Data = #{street => <<"123 Main">>, city => <<"Portland">>, zip => <<"97201">>},
    {ok, Json} = kura_types:dump({embed, embeds_one, kura_test_address}, Data),
    ?assert(is_binary(Json)),
    {ok, Decoded} = kura_types:load(jsonb, Json),
    ?assertEqual(<<"123 Main">>, maps:get(<<"street">>, Decoded)).

dump_embed_many_test() ->
    Data = [
        #{label => <<"important">>, weight => 10},
        #{label => <<"urgent">>, weight => 5}
    ],
    {ok, Json} = kura_types:dump({embed, embeds_many, kura_test_label}, Data),
    ?assert(is_binary(Json)),
    {ok, Decoded} = kura_types:load(jsonb, Json),
    ?assert(is_list(Decoded)),
    ?assertEqual(2, length(Decoded)).

load_embed_one_from_map_test() ->
    Map = #{<<"street">> => <<"123 Main">>, <<"city">> => <<"Portland">>, <<"zip">> => <<"97201">>},
    {ok, Loaded} = kura_types:load({embed, embeds_one, kura_test_address}, Map),
    ?assertEqual(<<"123 Main">>, maps:get(street, Loaded)),
    ?assertEqual(<<"Portland">>, maps:get(city, Loaded)),
    ?assertEqual(<<"97201">>, maps:get(zip, Loaded)).

load_embed_one_from_binary_test() ->
    Json = <<"{\"street\":\"123 Main\",\"city\":\"Portland\",\"zip\":\"97201\"}">>,
    {ok, Loaded} = kura_types:load({embed, embeds_one, kura_test_address}, Json),
    ?assertEqual(<<"123 Main">>, maps:get(street, Loaded)),
    ?assertEqual(<<"Portland">>, maps:get(city, Loaded)).

load_embed_many_test() ->
    List = [
        #{<<"label">> => <<"important">>, <<"weight">> => 10},
        #{<<"label">> => <<"urgent">>, <<"weight">> => 5}
    ],
    {ok, Loaded} = kura_types:load({embed, embeds_many, kura_test_label}, List),
    ?assertEqual(2, length(Loaded)),
    [L1, L2] = Loaded,
    ?assertEqual(<<"important">>, maps:get(label, L1)),
    ?assertEqual(10, maps:get(weight, L1)),
    ?assertEqual(<<"urgent">>, maps:get(label, L2)).

dump_load_roundtrip_test() ->
    Data = #{street => <<"123 Main">>, city => <<"Portland">>, zip => <<"97201">>},
    {ok, Dumped} = kura_types:dump({embed, embeds_one, kura_test_address}, Data),
    {ok, Loaded} = kura_types:load({embed, embeds_one, kura_test_address}, Dumped),
    ?assertEqual(<<"123 Main">>, maps:get(street, Loaded)),
    ?assertEqual(<<"Portland">>, maps:get(city, Loaded)),
    ?assertEqual(<<"97201">>, maps:get(zip, Loaded)).

to_pg_type_embed_test() ->
    ?assertEqual(<<"JSONB">>, kura_types:to_pg_type({embed, embeds_one, kura_test_address})),
    ?assertEqual(<<"JSONB">>, kura_types:to_pg_type({embed, embeds_many, kura_test_label})).
