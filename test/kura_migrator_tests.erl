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
