-module(kura_changeset_constraint_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, build_table_constraints_skips_unknown_test/0}).

%%----------------------------------------------------------------------
%% schema_table
%%----------------------------------------------------------------------

schema_table_test() ->
    ?assertEqual(<<"participants">>, kura_changeset:schema_table(kura_test_participant_schema)).

schema_table_indexed_test() ->
    ?assertEqual(<<"indexed_items">>, kura_changeset:schema_table(kura_test_indexed_schema)).

%%----------------------------------------------------------------------
%% join_col_names
%%----------------------------------------------------------------------

join_col_names_single_test() ->
    ?assertEqual(<<"chat_id">>, kura_changeset:join_col_names([chat_id], <<"_">>)).

join_col_names_multiple_test() ->
    ?assertEqual(
        <<"chat_id_user_id">>,
        kura_changeset:join_col_names([chat_id, user_id], <<"_">>)
    ).

join_col_names_different_sep_test() ->
    ?assertEqual(<<"a, b">>, kura_changeset:join_col_names([a, b], <<", ">>)).

%%----------------------------------------------------------------------
%% join_bins
%%----------------------------------------------------------------------

join_bins_empty_test() ->
    ?assertEqual(<<>>, kura_changeset:join_bins([], <<"_">>)).

join_bins_single_test() ->
    ?assertEqual(<<"hello">>, kura_changeset:join_bins([<<"hello">>], <<"_">>)).

join_bins_multiple_test() ->
    ?assertEqual(
        <<"a_b_c">>,
        kura_changeset:join_bins([<<"a">>, <<"b">>, <<"c">>], <<"_">>)
    ).

%%----------------------------------------------------------------------
%% build_table_constraints
%%----------------------------------------------------------------------

build_table_constraints_empty_test() ->
    ?assertEqual([], kura_changeset:build_table_constraints([], <<"t">>)).

build_table_constraints_unique_test() ->
    Result = kura_changeset:build_table_constraints(
        [{unique, [chat_id, user_id]}], <<"participants">>
    ),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(<<"participants_chat_id_user_id_key">>, C#kura_constraint.constraint),
    ?assertEqual(chat_id, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

build_table_constraints_check_test() ->
    Result = kura_changeset:build_table_constraints([{check, <<"qty > 0">>}], <<"orders">>),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(check, C#kura_constraint.type),
    ?assertEqual(<<"orders_check">>, C#kura_constraint.constraint),
    ?assertEqual(base, C#kura_constraint.field),
    ?assertEqual(<<"is invalid">>, C#kura_constraint.message).

build_table_constraints_multiple_test() ->
    Constraints = [{unique, [a, b]}, {check, <<"a > 0">>}],
    Result = kura_changeset:build_table_constraints(Constraints, <<"t">>),
    ?assertEqual(2, length(Result)),
    [Unique, Check] = Result,
    ?assertEqual(unique, Unique#kura_constraint.type),
    ?assertEqual(check, Check#kura_constraint.type).

build_table_constraints_skips_unknown_test() ->
    Result = kura_changeset:build_table_constraints([{foreign_key, something}], <<"t">>),
    ?assertEqual([], Result).

%%----------------------------------------------------------------------
%% build_index_constraints
%%----------------------------------------------------------------------

build_index_constraints_empty_test() ->
    ?assertEqual([], kura_changeset:build_index_constraints([], <<"t">>)).

build_index_constraints_unique_index_test() ->
    Result = kura_changeset:build_index_constraints(
        [{[code], #{unique => true}}], <<"indexed_items">>
    ),
    ?assertEqual(1, length(Result)),
    [C] = Result,
    ?assertEqual(unique, C#kura_constraint.type),
    ?assertEqual(code, C#kura_constraint.field),
    ?assertEqual(<<"has already been taken">>, C#kura_constraint.message).

build_index_constraints_non_unique_skipped_test() ->
    Result = kura_changeset:build_index_constraints([{[code], #{unique => false}}], <<"t">>),
    ?assertEqual([], Result).

build_index_constraints_no_unique_key_skipped_test() ->
    Result = kura_changeset:build_index_constraints([{[code], #{}}], <<"t">>),
    ?assertEqual([], Result).

%%----------------------------------------------------------------------
%% cast on schema with constraints produces constraint records
%%----------------------------------------------------------------------

cast_participant_has_constraints_test() ->
    CS = kura_changeset:cast(
        kura_test_participant_schema,
        #{},
        #{chat_id => <<"abc">>, user_id => <<"def">>},
        [chat_id, user_id]
    ),
    Constraints = CS#kura_changeset.constraints,
    ?assert(length(Constraints) > 0),
    [C] = [X || X <- Constraints, X#kura_constraint.type =:= unique],
    ?assertEqual(chat_id, C#kura_constraint.field).

cast_indexed_has_constraints_test() ->
    CS = kura_changeset:cast(
        kura_test_indexed_schema,
        #{},
        #{code => <<"ABC">>},
        [code]
    ),
    Constraints = CS#kura_changeset.constraints,
    ?assert(length(Constraints) > 0),
    [C] = [X || X <- Constraints, X#kura_constraint.type =:= unique],
    ?assertEqual(code, C#kura_constraint.field).

cast_plain_schema_no_constraints_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"Alice">>}, [name]),
    ?assertEqual([], CS#kura_changeset.constraints).
