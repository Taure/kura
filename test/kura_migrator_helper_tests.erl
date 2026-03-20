-module(kura_migrator_helper_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% sort_integers
%%----------------------------------------------------------------------

sort_integers_empty_test() ->
    ?assertEqual([], kura_migrator:sort_integers([])).

sort_integers_single_test() ->
    ?assertEqual([1], kura_migrator:sort_integers([1])).

sort_integers_already_sorted_test() ->
    ?assertEqual([1, 2, 3], kura_migrator:sort_integers([1, 2, 3])).

sort_integers_reverse_test() ->
    ?assertEqual([1, 2, 3], kura_migrator:sort_integers([3, 2, 1])).

sort_integers_unsorted_test() ->
    ?assertEqual([1, 2, 3, 4, 5], kura_migrator:sort_integers([3, 1, 4, 5, 2])).

sort_integers_duplicates_test() ->
    Sorted = kura_migrator:sort_integers([3, 1, 3, 2, 1]),
    ?assertEqual([1, 1, 2, 3, 3], Sorted).

%%----------------------------------------------------------------------
%% reverse_integers
%%----------------------------------------------------------------------

reverse_integers_empty_test() ->
    ?assertEqual([], kura_migrator:reverse_integers([], [])).

reverse_integers_single_test() ->
    ?assertEqual([1], kura_migrator:reverse_integers([1], [])).

reverse_integers_multiple_test() ->
    ?assertEqual([3, 2, 1], kura_migrator:reverse_integers([1, 2, 3], [])).

reverse_integers_with_initial_acc_test() ->
    ?assertEqual([2, 1, 10, 20], kura_migrator:reverse_integers([1, 2], [10, 20])).

%%----------------------------------------------------------------------
%% take_integers
%%----------------------------------------------------------------------

take_integers_zero_test() ->
    ?assertEqual([], kura_migrator:take_integers([1, 2, 3], 0)).

take_integers_empty_test() ->
    ?assertEqual([], kura_migrator:take_integers([], 5)).

take_integers_fewer_than_n_test() ->
    ?assertEqual([1, 2], kura_migrator:take_integers([1, 2], 5)).

take_integers_exact_test() ->
    ?assertEqual([1, 2, 3], kura_migrator:take_integers([1, 2, 3], 3)).

take_integers_partial_test() ->
    ?assertEqual([1, 2], kura_migrator:take_integers([1, 2, 3, 4], 2)).

%%----------------------------------------------------------------------
%% partition_ints
%%----------------------------------------------------------------------

partition_ints_empty_test() ->
    ?assertEqual({[], []}, kura_migrator:partition_ints(5, [], [], [])).

partition_ints_all_less_test() ->
    {Less, Greater} = kura_migrator:partition_ints(10, [1, 2, 3], [], []),
    ?assertEqual([3, 2, 1], Less),
    ?assertEqual([], Greater).

partition_ints_all_greater_test() ->
    {Less, Greater} = kura_migrator:partition_ints(0, [1, 2, 3], [], []),
    ?assertEqual([], Less),
    ?assertEqual([3, 2, 1], Greater).

partition_ints_mixed_test() ->
    {Less, Greater} = kura_migrator:partition_ints(5, [3, 7, 1, 9, 5], [], []),
    ?assert(lists:all(fun(X) -> X =< 5 end, Less)),
    ?assert(lists:all(fun(X) -> X > 5 end, Greater)).

partition_ints_equal_goes_to_less_test() ->
    {Less, Greater} = kura_migrator:partition_ints(5, [5], [], []),
    ?assertEqual([5], Less),
    ?assertEqual([], Greater).

%%----------------------------------------------------------------------
%% sort_migrations
%%----------------------------------------------------------------------

sort_migrations_empty_test() ->
    ?assertEqual([], kura_migrator:sort_migrations([])).

sort_migrations_single_test() ->
    ?assertEqual([{1, m1}], kura_migrator:sort_migrations([{1, m1}])).

sort_migrations_sorted_test() ->
    Input = [{20250101120000, m1}, {20250101130000, m2}, {20250101140000, m3}],
    ?assertEqual(Input, kura_migrator:sort_migrations(Input)).

sort_migrations_unsorted_test() ->
    Input = [{20250101140000, m3}, {20250101120000, m1}, {20250101130000, m2}],
    Expected = [{20250101120000, m1}, {20250101130000, m2}, {20250101140000, m3}],
    ?assertEqual(Expected, kura_migrator:sort_migrations(Input)).

%%----------------------------------------------------------------------
%% partition_migrations
%%----------------------------------------------------------------------

partition_migrations_empty_test() ->
    ?assertEqual({[], []}, kura_migrator:partition_migrations(5, [], [], [])).

partition_migrations_split_test() ->
    {Less, Greater} = kura_migrator:partition_migrations(
        20250101130000,
        [{20250101120000, m1}, {20250101140000, m3}],
        [],
        []
    ),
    ?assertEqual([{20250101120000, m1}], Less),
    ?assertEqual([{20250101140000, m3}], Greater).

%%----------------------------------------------------------------------
%% build_rollback_pairs
%%----------------------------------------------------------------------

build_rollback_pairs_empty_test() ->
    ?assertEqual([], kura_migrator:build_rollback_pairs([], #{})).

build_rollback_pairs_found_test() ->
    Map = #{1 => m1, 2 => m2, 3 => m3},
    ?assertEqual([{2, m2}, {1, m1}], kura_migrator:build_rollback_pairs([2, 1], Map)).

build_rollback_pairs_missing_skipped_test() ->
    Map = #{1 => m1, 3 => m3},
    ?assertEqual([{3, m3}, {1, m1}], kura_migrator:build_rollback_pairs([3, 2, 1], Map)).

build_rollback_pairs_all_missing_test() ->
    ?assertEqual([], kura_migrator:build_rollback_pairs([10, 20], #{1 => m1})).

%%----------------------------------------------------------------------
%% narrow_migration_result
%%----------------------------------------------------------------------

narrow_migration_result_error_test() ->
    ?assertEqual({error, timeout}, kura_migrator:narrow_migration_result({error, timeout})).

narrow_migration_result_list_test() ->
    ?assertEqual({ok, [1, 2, 3]}, kura_migrator:narrow_migration_result([1, 2, 3])).

narrow_migration_result_empty_list_test() ->
    ?assertEqual({ok, []}, kura_migrator:narrow_migration_result([])).

narrow_migration_result_non_list_test() ->
    ?assertEqual({error, something}, kura_migrator:narrow_migration_result(something)).

%%----------------------------------------------------------------------
%% parse_version
%%----------------------------------------------------------------------

parse_version_test() ->
    ?assertEqual(20250101120000, kura_migrator:parse_version("20250101120000")).

parse_version_short_test() ->
    ?assertEqual(123, kura_migrator:parse_version("123")).

%%----------------------------------------------------------------------
%% get_app_modules
%%----------------------------------------------------------------------

get_app_modules_unknown_app_test() ->
    ?assertEqual([], kura_migrator:get_app_modules(nonexistent_app_xyz)).

get_app_modules_kernel_test() ->
    Mods = kura_migrator:get_app_modules(kernel),
    ?assert(is_list(Mods)),
    ?assert(length(Mods) > 0).

%%----------------------------------------------------------------------
%% to_module_list
%%----------------------------------------------------------------------

to_module_list_list_test() ->
    ?assertEqual([foo, bar], kura_migrator:to_module_list([foo, bar])).

to_module_list_non_list_test() ->
    ?assertEqual([], kura_migrator:to_module_list(not_a_list)).

to_module_list_filters_non_atoms_test() ->
    ?assertEqual([foo], kura_migrator:to_module_list([foo, "bar", 123])).

%%----------------------------------------------------------------------
%% check_alter_ops
%%----------------------------------------------------------------------

check_alter_ops_empty_test() ->
    ?assertEqual([], kura_migrator:check_alter_ops([], <<"t">>, [])).

check_alter_ops_multiple_test() ->
    AlterOps = [{drop_column, email}, {rename_column, name, full_name}],
    Warnings = kura_migrator:check_alter_ops(AlterOps, <<"users">>, []),
    ?assertEqual(2, length(Warnings)).

check_alter_ops_with_suppression_test() ->
    AlterOps = [{drop_column, email}, {drop_column, phone}],
    Warnings = kura_migrator:check_alter_ops(AlterOps, <<"users">>, [{drop_column, email}]),
    ?assertEqual(1, length(Warnings)),
    [W] = Warnings,
    ?assertEqual(phone, maps:get(target, W)).
