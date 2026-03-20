-module(kura_migrator_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% DDL compilation tests
%%----------------------------------------------------------------------

create_table_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"users">>, [
            #kura_column{name = id, type = id, primary_key = true, nullable = false},
            #kura_column{name = name, type = string, nullable = false},
            #kura_column{name = email, type = string, nullable = false},
            #kura_column{name = age, type = integer},
            #kura_column{name = active, type = boolean, default = true}
        ]}
    ),
    ?assert(binary:match(SQL, <<"CREATE TABLE \"users\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"id\" BIGSERIAL PRIMARY KEY NOT NULL">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"name\" VARCHAR(255) NOT NULL">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"email\" VARCHAR(255) NOT NULL">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"age\" INTEGER">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"active\" BOOLEAN DEFAULT TRUE">>) =/= nomatch).

drop_table_test() ->
    SQL = kura_migrator:compile_operation({drop_table, <<"users">>}),
    ?assertEqual(<<"DROP TABLE \"users\"">>, SQL).

alter_table_add_column_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = bio, type = text}}
        ]}
    ),
    ?assert(binary:match(SQL, <<"ALTER TABLE \"users\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ADD COLUMN \"bio\" TEXT">>) =/= nomatch).

alter_table_drop_column_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"users">>, [
            {drop_column, bio}
        ]}
    ),
    ?assert(binary:match(SQL, <<"DROP COLUMN \"bio\"">>) =/= nomatch).

alter_table_rename_column_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"users">>, [
            {rename_column, name, full_name}
        ]}
    ),
    ?assert(binary:match(SQL, <<"RENAME COLUMN \"name\" TO \"full_name\"">>) =/= nomatch).

alter_table_modify_column_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"users">>, [
            {modify_column, name, text}
        ]}
    ),
    ?assert(binary:match(SQL, <<"ALTER COLUMN \"name\" TYPE TEXT">>) =/= nomatch).

create_index_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users_email_index">>, <<"users">>, [email], []}
    ),
    ?assertEqual(<<"CREATE INDEX \"users_email_index\" ON \"users\" (\"email\")">>, SQL).

create_unique_index_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users_email_index">>, <<"users">>, [email], [unique]}
    ),
    ?assert(binary:match(SQL, <<"CREATE UNIQUE INDEX">>) =/= nomatch).

create_index_with_where_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users_active_email">>, <<"users">>, [email], [
            unique, {where, <<"active = true">>}
        ]}
    ),
    ?assert(binary:match(SQL, <<"WHERE active = true">>) =/= nomatch).

create_composite_index_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users_name_email">>, <<"users">>, [name, email], []}
    ),
    ?assert(binary:match(SQL, <<"(\"name\", \"email\")">>) =/= nomatch).

drop_index_test() ->
    SQL = kura_migrator:compile_operation({drop_index, <<"users_email_index">>}),
    ?assertEqual(<<"DROP INDEX \"users_email_index\"">>, SQL).

execute_raw_sql_test() ->
    SQL = kura_migrator:compile_operation({execute, <<"CREATE EXTENSION IF NOT EXISTS citext">>}),
    ?assertEqual(<<"CREATE EXTENSION IF NOT EXISTS citext">>, SQL).

%%----------------------------------------------------------------------
%% Column type tests
%%----------------------------------------------------------------------

column_types_test_() ->
    Types = [
        {id, <<"BIGSERIAL">>},
        {integer, <<"INTEGER">>},
        {float, <<"DOUBLE PRECISION">>},
        {string, <<"VARCHAR(255)">>},
        {text, <<"TEXT">>},
        {boolean, <<"BOOLEAN">>},
        {date, <<"DATE">>},
        {utc_datetime, <<"TIMESTAMPTZ">>},
        {uuid, <<"UUID">>},
        {jsonb, <<"JSONB">>},
        {{array, integer}, <<"INTEGER[]">>}
    ],
    [
        ?_assert(begin
            SQL = kura_migrator:compile_operation(
                {create_table, <<"test">>, [
                    #kura_column{name = col, type = T}
                ]}
            ),
            binary:match(SQL, Expected) =/= nomatch
        end)
     || {T, Expected} <- Types
    ].

%%----------------------------------------------------------------------
%% Default values
%%----------------------------------------------------------------------

default_integer_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{name = x, type = integer, default = 0}
        ]}
    ),
    ?assert(binary:match(SQL, <<"DEFAULT 0">>) =/= nomatch).

default_string_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{name = x, type = string, default = <<"hello">>}
        ]}
    ),
    ?assert(binary:match(SQL, <<"DEFAULT 'hello'">>) =/= nomatch).

default_boolean_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{name = x, type = boolean, default = false}
        ]}
    ),
    ?assert(binary:match(SQL, <<"DEFAULT FALSE">>) =/= nomatch).

%%----------------------------------------------------------------------
%% Unsafe operation detection tests
%%----------------------------------------------------------------------

detect_drop_column_test() ->
    Ops = [{alter_table, <<"users">>, [{drop_column, email}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(drop_column, maps:get(op, W)),
    ?assertEqual(email, maps:get(target, W)),
    ?assertEqual(<<"users">>, maps:get(table, W)).

detect_rename_column_test() ->
    Ops = [{alter_table, <<"users">>, [{rename_column, name, full_name}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(rename_column, maps:get(op, W)),
    ?assertEqual(name, maps:get(target, W)).

detect_modify_column_test() ->
    Ops = [{alter_table, <<"users">>, [{modify_column, age, text}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(modify_column, maps:get(op, W)),
    ?assertEqual(age, maps:get(target, W)).

detect_drop_table_test() ->
    Ops = [{drop_table, <<"sessions">>}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(drop_table, maps:get(op, W)),
    ?assertEqual(<<"sessions">>, maps:get(target, W)).

detect_add_column_not_null_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = role, type = string, nullable = false}}
        ]}
    ],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(add_column_not_null, maps:get(op, W)),
    ?assertEqual(role, maps:get(target, W)).

%%----------------------------------------------------------------------
%% Safe operation tests (no warnings)
%%----------------------------------------------------------------------

safe_nullable_add_column_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = bio, type = text, nullable = true}}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

safe_add_column_with_default_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{
                name = role, type = string, nullable = false, default = <<"user">>
            }}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

safe_create_table_test() ->
    Ops = [
        {create_table, <<"posts">>, [
            #kura_column{name = id, type = id, primary_key = true}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

safe_create_index_test() ->
    Ops = [{create_index, <<"idx">>, <<"users">>, [email], [unique]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

safe_execute_test() ->
    Ops = [{execute, <<"CREATE EXTENSION IF NOT EXISTS citext">>}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

%%----------------------------------------------------------------------
%% Suppression tests (safe/0 entries suppress warnings)
%%----------------------------------------------------------------------

suppress_drop_column_test() ->
    Ops = [{alter_table, <<"users">>, [{drop_column, email}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{drop_column, email}])).

suppress_rename_column_test() ->
    Ops = [{alter_table, <<"users">>, [{rename_column, name, full_name}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{rename_column, name}])).

suppress_modify_column_test() ->
    Ops = [{alter_table, <<"users">>, [{modify_column, age, text}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{modify_column, age}])).

suppress_drop_table_test() ->
    Ops = [{drop_table, <<"sessions">>}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [drop_table])).

suppress_add_column_not_null_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = role, type = string, nullable = false}}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{add_column, role}])).

%%----------------------------------------------------------------------
%% Partial suppression test
%%----------------------------------------------------------------------

partial_suppression_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {drop_column, email},
            {drop_column, phone}
        ]}
    ],
    Warnings = kura_migrator:check_unsafe_operations(Ops, [{drop_column, email}]),
    ?assertEqual(1, length(Warnings)),
    [W] = Warnings,
    ?assertEqual(phone, maps:get(target, W)).

%%----------------------------------------------------------------------
%% Foreign key constraint tests
%%----------------------------------------------------------------------

fk_inline_create_table_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"posts">>, [
            #kura_column{name = id, type = id, primary_key = true, nullable = false},
            #kura_column{
                name = user_id,
                type = integer,
                nullable = false,
                references = {<<"users">>, id},
                on_delete = cascade
            }
        ]}
    ),
    ?assert(
        binary:match(
            SQL, <<"\"user_id\" INTEGER NOT NULL REFERENCES \"users\"(\"id\") ON DELETE CASCADE">>
        ) =/= nomatch
    ).

fk_on_delete_cascade_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id, type = integer, references = {<<"refs">>, id}, on_delete = cascade
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"REFERENCES \"refs\"(\"id\") ON DELETE CASCADE">>) =/= nomatch).

fk_on_delete_restrict_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id, type = integer, references = {<<"refs">>, id}, on_delete = restrict
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"ON DELETE RESTRICT">>) =/= nomatch).

fk_on_delete_set_null_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id, type = integer, references = {<<"refs">>, id}, on_delete = set_null
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"ON DELETE SET NULL">>) =/= nomatch).

fk_on_delete_no_action_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id, type = integer, references = {<<"refs">>, id}, on_delete = no_action
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"ON DELETE NO ACTION">>) =/= nomatch).

fk_on_update_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id, type = integer, references = {<<"refs">>, id}, on_update = cascade
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"ON UPDATE CASCADE">>) =/= nomatch).

fk_on_delete_and_on_update_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{
                name = ref_id,
                type = integer,
                references = {<<"refs">>, id},
                on_delete = cascade,
                on_update = restrict
            }
        ]}
    ),
    ?assert(binary:match(SQL, <<"ON DELETE CASCADE">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"ON UPDATE RESTRICT">>) =/= nomatch).

fk_alter_table_add_column_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"posts">>, [
            {add_column, #kura_column{
                name = author_id,
                type = integer,
                nullable = false,
                references = {<<"users">>, id},
                on_delete = cascade
            }}
        ]}
    ),
    ?assert(
        binary:match(
            SQL,
            <<"ADD COLUMN \"author_id\" INTEGER NOT NULL REFERENCES \"users\"(\"id\") ON DELETE CASCADE">>
        ) =/= nomatch
    ).

%%----------------------------------------------------------------------
%% Table-level constraints
%%----------------------------------------------------------------------

create_table_unique_constraint_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"participant">>,
            [
                #kura_column{name = id, type = id, primary_key = true, nullable = false},
                #kura_column{name = chat_id, type = uuid, nullable = false},
                #kura_column{name = user_id, type = uuid, nullable = false}
            ],
            [{unique, [chat_id, user_id]}]}
    ),
    ?assert(binary:match(SQL, <<"UNIQUE (\"chat_id\", \"user_id\")">>) =/= nomatch).

create_table_check_constraint_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"orders">>,
            [
                #kura_column{name = id, type = id, primary_key = true},
                #kura_column{name = quantity, type = integer, nullable = false}
            ],
            [{check, <<"quantity > 0">>}]}
    ),
    ?assert(binary:match(SQL, <<"CHECK (quantity > 0)">>) =/= nomatch).

create_table_multiple_constraints_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>,
            [
                #kura_column{name = id, type = id, primary_key = true},
                #kura_column{name = a, type = integer},
                #kura_column{name = b, type = integer},
                #kura_column{name = c, type = integer}
            ],
            [{unique, [a, b]}, {unique, [b, c]}, {check, <<"a > 0">>}]}
    ),
    ?assert(binary:match(SQL, <<"UNIQUE (\"a\", \"b\")">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"UNIQUE (\"b\", \"c\")">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"CHECK (a > 0)">>) =/= nomatch).

create_table_no_constraints_same_as_3tuple_test() ->
    Cols = [
        #kura_column{name = id, type = id, primary_key = true},
        #kura_column{name = name, type = string}
    ],
    SQL3 = kura_migrator:compile_operation({create_table, <<"t">>, Cols}),
    SQL4 = kura_migrator:compile_operation({create_table, <<"t">>, Cols, []}),
    ?assertEqual(SQL3, SQL4).

%%----------------------------------------------------------------------
%% Map-based create_index (Ecto-style, auto-generated name)
%%----------------------------------------------------------------------

create_index_map_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [email], #{}}
    ),
    ?assertEqual(<<"CREATE INDEX \"users_email_index\" ON \"users\" (\"email\")">>, SQL).

create_unique_index_map_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [email], #{unique => true}}
    ),
    ?assertEqual(
        <<"CREATE UNIQUE INDEX \"users_email_index\" ON \"users\" (\"email\")">>, SQL
    ).

create_index_map_with_where_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [email], #{unique => true, where => <<"email IS NOT NULL">>}}
    ),
    ?assert(binary:match(SQL, <<"CREATE UNIQUE INDEX">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"users_email_index\"">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"WHERE email IS NOT NULL">>) =/= nomatch).

create_composite_index_map_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [first_name, last_name], #{}}
    ),
    ?assertEqual(
        <<"CREATE INDEX \"users_first_name_last_name_index\" ON \"users\" (\"first_name\", \"last_name\")">>,
        SQL
    ).

index_name_test() ->
    ?assertEqual(<<"users_email_index">>, kura_migration:index_name(<<"users">>, [email])),
    ?assertEqual(
        <<"posts_user_id_title_index">>,
        kura_migration:index_name(<<"posts">>, [user_id, title])
    ).

%%----------------------------------------------------------------------
%% FK without on_delete/on_update
%%----------------------------------------------------------------------

fk_no_on_delete_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{name = ref_id, type = integer, references = {<<"refs">>, id}}
        ]}
    ),
    ?assert(binary:match(SQL, <<"REFERENCES \"refs\"(\"id\")">>) =/= nomatch),
    ?assertEqual(nomatch, binary:match(SQL, <<"ON DELETE">>)),
    ?assertEqual(nomatch, binary:match(SQL, <<"ON UPDATE">>)).

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
