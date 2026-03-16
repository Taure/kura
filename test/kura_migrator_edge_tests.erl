-module(kura_migrator_edge_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

drop_table_unsafe_test() ->
    Ops = [{drop_table, <<"sessions">>}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(drop_table, maps:get(op, W)),
    ?assertEqual(<<"sessions">>, maps:get(target, W)),
    ?assert(maps:is_key(risk, W)),
    ?assert(maps:is_key(safe_alt, W)).

drop_table_safe_when_suppressed_test() ->
    Ops = [{drop_table, <<"sessions">>}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [drop_table])).

drop_column_unsafe_test() ->
    Ops = [{alter_table, <<"users">>, [{drop_column, email}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(drop_column, maps:get(op, W)),
    ?assertEqual(email, maps:get(target, W)),
    ?assertEqual(<<"users">>, maps:get(table, W)).

drop_column_safe_when_suppressed_test() ->
    Ops = [{alter_table, <<"users">>, [{drop_column, email}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{drop_column, email}])).

rename_column_unsafe_test() ->
    Ops = [{alter_table, <<"users">>, [{rename_column, name, full_name}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(rename_column, maps:get(op, W)),
    ?assertEqual(name, maps:get(target, W)),
    ?assertEqual(<<"users">>, maps:get(table, W)).

rename_column_safe_when_suppressed_test() ->
    Ops = [{alter_table, <<"users">>, [{rename_column, name, full_name}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{rename_column, name}])).

modify_column_unsafe_test() ->
    Ops = [{alter_table, <<"users">>, [{modify_column, age, text}]}],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(modify_column, maps:get(op, W)),
    ?assertEqual(age, maps:get(target, W)),
    ?assertEqual(<<"users">>, maps:get(table, W)).

modify_column_safe_when_suppressed_test() ->
    Ops = [{alter_table, <<"users">>, [{modify_column, age, text}]}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{modify_column, age}])).

add_not_null_column_unsafe_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = role, type = string, nullable = false}}
        ]}
    ],
    [W] = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(add_column_not_null, maps:get(op, W)),
    ?assertEqual(role, maps:get(target, W)),
    ?assertEqual(<<"users">>, maps:get(table, W)).

add_not_null_column_safe_when_suppressed_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = role, type = string, nullable = false}}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [{add_column, role}])).

add_nullable_column_always_safe_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = bio, type = text, nullable = true}}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

add_not_null_column_with_default_safe_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {add_column, #kura_column{
                name = role, type = string, nullable = false, default = <<"user">>
            }}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

create_table_always_safe_test() ->
    Ops = [
        {create_table, <<"posts">>, [
            #kura_column{name = id, type = id, primary_key = true}
        ]}
    ],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

create_index_always_safe_test() ->
    Ops = [{create_index, <<"users">>, [email], #{unique => true}}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

drop_index_always_safe_test() ->
    Ops = [{drop_index, <<"users_email_index">>}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

execute_always_safe_test() ->
    Ops = [{execute, <<"SELECT 1">>}],
    ?assertEqual([], kura_migrator:check_unsafe_operations(Ops, [])).

multiple_unsafe_ops_test() ->
    Ops = [
        {drop_table, <<"sessions">>},
        {alter_table, <<"users">>, [
            {drop_column, email},
            {rename_column, name, full_name},
            {modify_column, age, text},
            {add_column, #kura_column{name = role, type = string, nullable = false}}
        ]}
    ],
    Warnings = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(5, length(Warnings)).

mixed_safe_unsafe_ops_test() ->
    Ops = [
        {alter_table, <<"users">>, [
            {drop_column, email},
            {add_column, #kura_column{name = bio, type = text, nullable = true}},
            {rename_column, name, full_name}
        ]}
    ],
    Warnings = kura_migrator:check_unsafe_operations(Ops, []),
    ?assertEqual(2, length(Warnings)).

compile_default_float_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>, [
            #kura_column{name = x, type = float, default = 3.14}
        ]}
    ),
    ?assert(binary:match(SQL, <<"DEFAULT">>) =/= nomatch).

compile_create_table_with_constraints_test() ->
    SQL = kura_migrator:compile_operation(
        {create_table, <<"t">>,
            [
                #kura_column{name = id, type = id, primary_key = true},
                #kura_column{name = a, type = integer},
                #kura_column{name = b, type = integer}
            ],
            [{unique, [a, b]}, {check, <<"a > 0">>}]}
    ),
    ?assert(binary:match(SQL, <<"UNIQUE (\"a\", \"b\")">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"CHECK (a > 0)">>) =/= nomatch).

compile_create_index_no_unique_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [email], #{}}
    ),
    ?assertEqual(nomatch, binary:match(SQL, <<"UNIQUE">>)).

compile_create_index_with_where_map_test() ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"users">>, [email], #{where => <<"active = true">>}}
    ),
    ?assert(binary:match(SQL, <<"WHERE active = true">>) =/= nomatch).

compile_alter_table_multiple_ops_test() ->
    SQL = kura_migrator:compile_operation(
        {alter_table, <<"users">>, [
            {add_column, #kura_column{name = bio, type = text}},
            {drop_column, phone}
        ]}
    ),
    ?assert(binary:match(SQL, <<"ADD COLUMN">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"DROP COLUMN">>) =/= nomatch).
