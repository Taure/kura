-module(kura_audit_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Actor context
%%----------------------------------------------------------------------

set_actor_clears_metadata_test() ->
    try
        kura_audit:set_actor(<<"user-1">>, #{ip => <<"1.2.3.4">>}),
        ?assertEqual(#{ip => <<"1.2.3.4">>}, erlang:get(kura_audit_metadata)),
        kura_audit:set_actor(<<"user-2">>),
        ?assertEqual(<<"user-2">>, kura_audit:get_actor()),
        ?assertEqual(undefined, erlang:get(kura_audit_metadata))
    after
        kura_audit:clear_actor()
    end.

set_actor_with_metadata_test() ->
    try
        Meta = #{role => <<"admin">>},
        kura_audit:set_actor(<<"user-1">>, Meta),
        ?assertEqual(<<"user-1">>, kura_audit:get_actor()),
        ?assertEqual(Meta, erlang:get(kura_audit_metadata))
    after
        kura_audit:clear_actor()
    end.

get_actor_undefined_test() ->
    try
        kura_audit:clear_actor(),
        ?assertEqual(undefined, kura_audit:get_actor())
    after
        kura_audit:clear_actor()
    end.

clear_actor_test() ->
    try
        kura_audit:set_actor(<<"user-1">>, #{ip => <<"1.2.3.4">>}),
        kura_audit:clear_actor(),
        ?assertEqual(undefined, kura_audit:get_actor()),
        ?assertEqual(undefined, erlang:get(kura_audit_metadata))
    after
        kura_audit:clear_actor()
    end.

with_actor_restores_previous_test() ->
    try
        kura_audit:set_actor(<<"outer">>),
        Result = kura_audit:with_actor(<<"inner">>, fun() ->
            ?assertEqual(<<"inner">>, kura_audit:get_actor()),
            done
        end),
        ?assertEqual(done, Result),
        ?assertEqual(<<"outer">>, kura_audit:get_actor())
    after
        kura_audit:clear_actor()
    end.

with_actor_metadata_test() ->
    try
        Meta = #{source => <<"api">>},
        kura_audit:with_actor(<<"temp">>, Meta, fun() ->
            ?assertEqual(<<"temp">>, kura_audit:get_actor()),
            ?assertEqual(Meta, erlang:get(kura_audit_metadata)),
            ok
        end),
        ?assertEqual(undefined, kura_audit:get_actor())
    after
        kura_audit:clear_actor()
    end.

with_actor_restores_on_exception_test() ->
    try
        kura_audit:set_actor(<<"original">>),
        catch kura_audit:with_actor(<<"crasher">>, fun() -> error(boom) end),
        ?assertEqual(<<"original">>, kura_audit:get_actor())
    after
        kura_audit:clear_actor()
    end.

with_actor_restores_undefined_test() ->
    try
        kura_audit:clear_actor(),
        kura_audit:with_actor(<<"temp">>, fun() ->
            ?assertEqual(<<"temp">>, kura_audit:get_actor()),
            ok
        end),
        ?assertEqual(undefined, kura_audit:get_actor()),
        ?assertEqual(undefined, erlang:get(kura_audit_actor))
    after
        kura_audit:clear_actor()
    end.

%%----------------------------------------------------------------------
%% Stash
%%----------------------------------------------------------------------

stash_stores_data_test() ->
    try
        CS = #kura_changeset{
            schema = kura_test_schema,
            data = #{id => 1, name => <<"Alice">>}
        },
        ?assertEqual(ok, kura_audit:stash(CS)),
        ?assertEqual(
            #{id => 1, name => <<"Alice">>},
            erlang:get({kura_audit_stash, kura_test_schema})
        )
    after
        erlang:erase({kura_audit_stash, kura_test_schema})
    end.

stash_consumed_by_build_audit_data_test() ->
    try
        OldData = #{id => 1, name => <<"Alice">>, email => <<"a@b.com">>},
        CS = #kura_changeset{schema = kura_test_schema, data = OldData},
        kura_audit:stash(CS),
        NewRecord = #{id => 1, name => <<"Bob">>, email => <<"a@b.com">>},
        {Old, New, Changes} = kura_audit:build_audit_data(kura_test_schema, update, NewRecord),
        ?assertNotEqual(undefined, Old),
        ?assertNotEqual(undefined, New),
        ?assertNotEqual(undefined, Changes),
        ?assertEqual(undefined, erlang:get({kura_audit_stash, kura_test_schema}))
    after
        erlang:erase({kura_audit_stash, kura_test_schema})
    end.

%%----------------------------------------------------------------------
%% format_id
%%----------------------------------------------------------------------

format_id_integer_test() ->
    ?assertEqual(<<"42">>, kura_audit:format_id(42)).

format_id_binary_test() ->
    ?assertEqual(<<"abc-123">>, kura_audit:format_id(<<"abc-123">>)).

format_id_undefined_test() ->
    ?assertEqual(<<"unknown">>, kura_audit:format_id(undefined)).

format_id_other_test() ->
    Result = kura_audit:format_id({composite, 1, 2}),
    ?assert(is_binary(Result)),
    ?assertNotEqual(<<"unknown">>, Result).

%%----------------------------------------------------------------------
%% sanitize_value
%%----------------------------------------------------------------------

sanitize_value_datetime_test() ->
    ?assertEqual(
        <<"2024-01-15T10:30:00Z">>,
        kura_audit:sanitize_value({{2024, 1, 15}, {10, 30, 0}})
    ).

sanitize_value_date_test() ->
    ?assertEqual(
        <<"2024-01-15">>,
        kura_audit:sanitize_value({2024, 1, 15})
    ).

sanitize_value_atom_test() ->
    ?assertEqual(<<"active">>, kura_audit:sanitize_value(active)).

sanitize_value_true_passthrough_test() ->
    ?assertEqual(true, kura_audit:sanitize_value(true)).

sanitize_value_false_passthrough_test() ->
    ?assertEqual(false, kura_audit:sanitize_value(false)).

sanitize_value_null_passthrough_test() ->
    ?assertEqual(null, kura_audit:sanitize_value(null)).

sanitize_value_undefined_test() ->
    ?assertEqual(null, kura_audit:sanitize_value(undefined)).

sanitize_value_map_test() ->
    ?assertEqual(
        #{key => <<"val">>},
        kura_audit:sanitize_value(#{key => val})
    ).

sanitize_value_list_test() ->
    ?assertEqual(
        [<<"a">>, <<"b">>],
        kura_audit:sanitize_value([a, b])
    ).

sanitize_value_plain_binary_test() ->
    ?assertEqual(<<"hello">>, kura_audit:sanitize_value(<<"hello">>)).

sanitize_value_plain_integer_test() ->
    ?assertEqual(42, kura_audit:sanitize_value(42)).

%%----------------------------------------------------------------------
%% sanitize
%%----------------------------------------------------------------------

sanitize_full_map_test() ->
    Input = #{
        name => <<"Alice">>,
        status => active,
        ts => {{2024, 1, 1}, {0, 0, 0}},
        data => undefined
    },
    Result = kura_audit:sanitize(Input),
    ?assertEqual(<<"Alice">>, maps:get(name, Result)),
    ?assertEqual(<<"active">>, maps:get(status, Result)),
    ?assertEqual(<<"2024-01-01T00:00:00Z">>, maps:get(ts, Result)),
    ?assertEqual(null, maps:get(data, Result)).

sanitize_empty_map_test() ->
    ?assertEqual(#{}, kura_audit:sanitize(#{})).

%%----------------------------------------------------------------------
%% compute_diff
%%----------------------------------------------------------------------

compute_diff_finds_changes_test() ->
    Old = #{name => <<"Alice">>, email => <<"a@b.com">>},
    New = #{name => <<"Bob">>, email => <<"a@b.com">>},
    Diff = kura_audit:compute_diff(Old, New),
    ?assertEqual(#{name => #{old => <<"Alice">>, new => <<"Bob">>}}, Diff).

compute_diff_skips_unchanged_test() ->
    Same = #{name => <<"Alice">>, email => <<"a@b.com">>},
    ?assertEqual(#{}, kura_audit:compute_diff(Same, Same)).

compute_diff_multiple_changes_test() ->
    Old = #{a => 1, b => 2, c => 3},
    New = #{a => 10, b => 2, c => 30},
    Diff = kura_audit:compute_diff(Old, New),
    ?assertEqual(#{a => #{old => 1, new => 10}, c => #{old => 3, new => 30}}, Diff).

compute_diff_new_key_test() ->
    Old = #{a => 1},
    New = #{a => 1, b => 2},
    Diff = kura_audit:compute_diff(Old, New),
    ?assertEqual(#{b => #{old => undefined, new => 2}}, Diff).

%%----------------------------------------------------------------------
%% filter_virtual
%%----------------------------------------------------------------------

filter_virtual_removes_virtual_fields_test() ->
    Record = #{id => 1, name => <<"Alice">>, full_name => <<"Alice Smith">>},
    Result = kura_audit:filter_virtual(kura_test_schema, Record),
    ?assertNot(maps:is_key(full_name, Result)),
    ?assertEqual(<<"Alice">>, maps:get(name, Result)).

filter_virtual_keeps_non_virtual_test() ->
    Record = #{id => 1, name => <<"Alice">>, email => <<"a@b.com">>},
    Result = kura_audit:filter_virtual(kura_test_schema, Record),
    ?assertEqual(Record, Result).

%%----------------------------------------------------------------------
%% build_audit_data
%%----------------------------------------------------------------------

build_audit_data_insert_test() ->
    Record = #{id => 1, name => <<"Alice">>, email => <<"a@b.com">>},
    {Old, New, Changes} = kura_audit:build_audit_data(kura_test_schema, insert, Record),
    ?assertEqual(undefined, Old),
    ?assertNotEqual(undefined, New),
    ?assertEqual(undefined, Changes),
    ?assertEqual(<<"Alice">>, maps:get(name, New)).

build_audit_data_delete_test() ->
    Record = #{id => 1, name => <<"Alice">>, email => <<"a@b.com">>},
    {Old, New, Changes} = kura_audit:build_audit_data(kura_test_schema, delete, Record),
    ?assertNotEqual(undefined, Old),
    ?assertEqual(undefined, New),
    ?assertEqual(undefined, Changes),
    ?assertEqual(<<"Alice">>, maps:get(name, Old)).

build_audit_data_update_with_stash_test() ->
    try
        OldRecord = #{id => 1, name => <<"Alice">>, email => <<"a@b.com">>},
        CS = #kura_changeset{schema = kura_test_schema, data = OldRecord},
        kura_audit:stash(CS),
        NewRecord = #{id => 1, name => <<"Bob">>, email => <<"a@b.com">>},
        {Old, New, Changes} = kura_audit:build_audit_data(kura_test_schema, update, NewRecord),
        ?assertNotEqual(undefined, Old),
        ?assertNotEqual(undefined, New),
        ?assertNotEqual(undefined, Changes),
        ?assertEqual(<<"Alice">>, maps:get(name, Old)),
        ?assertEqual(<<"Bob">>, maps:get(name, New)),
        ?assert(maps:is_key(name, Changes)),
        #{name := #{old := OldName, new := NewName}} = Changes,
        ?assertEqual(<<"Alice">>, OldName),
        ?assertEqual(<<"Bob">>, NewName)
    after
        erlang:erase({kura_audit_stash, kura_test_schema})
    end.

build_audit_data_update_without_stash_test() ->
    erlang:erase({kura_audit_stash, kura_test_schema}),
    NewRecord = #{id => 1, name => <<"Bob">>},
    {Old, New, Changes} = kura_audit:build_audit_data(kura_test_schema, update, NewRecord),
    ?assertEqual(undefined, Old),
    ?assertNotEqual(undefined, New),
    ?assertEqual(undefined, Changes).

build_audit_data_insert_filters_virtual_test() ->
    Record = #{id => 1, name => <<"Alice">>, full_name => <<"Alice Smith">>},
    {_, New, _} = kura_audit:build_audit_data(kura_test_schema, insert, Record),
    ?assertNot(maps:is_key(full_name, New)).

build_audit_data_insert_sanitizes_test() ->
    Record = #{id => 1, name => <<"Alice">>, status => active},
    {_, New, _} = kura_audit:build_audit_data(kura_test_schema, insert, Record),
    ?assertEqual(<<"active">>, maps:get(status, New)).

%%----------------------------------------------------------------------
%% restore
%%----------------------------------------------------------------------

restore_puts_value_test() ->
    Key = kura_audit_test_restore,
    try
        kura_audit:restore(Key, <<"val">>),
        ?assertEqual(<<"val">>, erlang:get(Key))
    after
        erlang:erase(Key)
    end.

restore_erases_on_undefined_test() ->
    Key = kura_audit_test_restore2,
    try
        erlang:put(Key, <<"something">>),
        kura_audit:restore(Key, undefined),
        ?assertEqual(undefined, erlang:get(Key))
    after
        erlang:erase(Key)
    end.

%%----------------------------------------------------------------------
%% Migration helpers
%%----------------------------------------------------------------------

migration_up_returns_operations_test() ->
    Ops = kura_audit:migration_up(),
    ?assert(is_list(Ops)),
    ?assert(length(Ops) > 0),
    [{create_table, TableName, Columns} | Indexes] = Ops,
    ?assertEqual(<<"audit_log">>, TableName),
    ?assert(length(Columns) > 0),
    ?assert(length(Indexes) > 0).

migration_up_has_required_columns_test() ->
    [{create_table, _, Columns} | _] = kura_audit:migration_up(),
    Names = [C#kura_column.name || C <- Columns],
    ?assert(lists:member(id, Names)),
    ?assert(lists:member(table_name, Names)),
    ?assert(lists:member(record_id, Names)),
    ?assert(lists:member(action, Names)),
    ?assert(lists:member(old_data, Names)),
    ?assert(lists:member(new_data, Names)),
    ?assert(lists:member(changes, Names)),
    ?assert(lists:member(actor, Names)),
    ?assert(lists:member(metadata, Names)),
    ?assert(lists:member(inserted_at, Names)).

migration_down_drops_table_test() ->
    Ops = kura_audit:migration_down(),
    ?assertEqual([{drop_table, <<"audit_log">>}], Ops).
