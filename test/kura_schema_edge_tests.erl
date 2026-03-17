-module(kura_schema_edge_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, primary_key_field_found_test/0}).

association_not_found_test() ->
    ?assertEqual({error, not_found}, kura_schema:association(kura_test_schema, nonexistent)).

association_found_test() ->
    {ok, Assoc} = kura_schema:association(kura_test_schema, profile),
    ?assertEqual(profile, Assoc#kura_assoc.name),
    ?assertEqual(has_one, Assoc#kura_assoc.type).

embed_not_found_on_schema_without_embeds_test() ->
    ?assertEqual({error, not_found}, kura_schema:embed(kura_test_schema, nonexistent)).

embeds_empty_for_no_embeds_schema_test() ->
    ?assertEqual([], kura_schema:embeds(kura_test_schema)).

constraints_empty_for_no_constraints_schema_test() ->
    ?assertEqual([], kura_schema:constraints(kura_test_schema)).

indexes_empty_for_no_indexes_schema_test() ->
    ?assertEqual([], kura_schema:indexes(kura_test_schema)).

constraints_returned_for_schema_with_constraints_test() ->
    Constraints = kura_schema:constraints(kura_test_participant_schema),
    ?assertEqual([{unique, [chat_id, user_id]}], Constraints).

indexes_returned_for_schema_with_indexes_test() ->
    Indexes = kura_schema:indexes(kura_test_indexed_schema),
    ?assertEqual([{[code], #{unique => true}}], Indexes).

primary_key_field_found_test() ->
    Field = kura_schema:primary_key_field(kura_test_schema),
    ?assertEqual(id, Field#kura_field.name),
    ?assert(Field#kura_field.primary_key).

field_names_test() ->
    Names = kura_schema:field_names(kura_test_schema),
    ?assert(lists:member(id, Names)),
    ?assert(lists:member(name, Names)),
    ?assert(lists:member(email, Names)),
    ?assert(lists:member(full_name, Names)).

field_types_test() ->
    Types = kura_schema:field_types(kura_test_schema),
    ?assertEqual(id, maps:get(id, Types)),
    ?assertEqual(string, maps:get(name, Types)),
    ?assertEqual(integer, maps:get(age, Types)).

column_map_excludes_virtual_test() ->
    ColMap = kura_schema:column_map(kura_test_schema),
    ?assertNot(maps:is_key(full_name, ColMap)),
    ?assert(maps:is_key(name, ColMap)).

column_map_custom_column_name_test() ->
    ColMap = kura_schema:column_map(kura_test_schema),
    ?assertEqual(<<"name">>, maps:get(name, ColMap)).

non_virtual_fields_excludes_virtual_test() ->
    NVF = kura_schema:non_virtual_fields(kura_test_schema),
    ?assertNot(lists:member(full_name, NVF)),
    ?assert(lists:member(name, NVF)).

primary_key_test() ->
    ?assertEqual(id, kura_schema:primary_key(kura_test_schema)).

associations_returns_list_test() ->
    Assocs = kura_schema:associations(kura_test_post),
    ?assert(length(Assocs) >= 2).

associations_empty_for_no_assoc_schema_test() ->
    Assocs = kura_schema:associations(kura_test_address),
    ?assertEqual([], Assocs).
