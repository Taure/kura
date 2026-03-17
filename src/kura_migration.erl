-module(kura_migration).
-moduledoc """
Behaviour for defining database migrations.

Implement `up/0` and `down/0` returning a list of DDL operations.
Migration modules must be named `m<YYYYMMDDHHMMSS>_<name>`.

```erlang
-module(m20250101120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

up() ->
    [{create_table, <<"users">>, [
        #kura_column{name = id, type = id, primary_key = true},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false}
    ]},
    {create_index, <<"users">>, [email], #{unique => true}}].

down() ->
    [{drop_index, <<"users_email_index">>},
     {drop_table, <<"users">>}].
```
""".

-include("kura.hrl").

-callback up() -> [operation()].
-callback down() -> [operation()].
-callback safe() -> [safe_entry()].
-optional_callbacks([safe/0]).

-type safe_entry() ::
    {drop_column, atom()}
    | {rename_column, atom()}
    | {modify_column, atom()}
    | {add_column, atom()}
    | drop_table.

-type column_def() :: #kura_column{}.
-type alter_op() ::
    {add_column, column_def()}
    | {drop_column, atom()}
    | {rename_column, atom(), atom()}
    | {modify_column, atom(), kura_types:kura_type()}.

-type index_opts() :: [unique | {where, binary()}].
-type index_opts_map() :: #{unique => boolean(), where => binary()}.
-type index_def() :: {[atom()], index_opts_map()}.

-type table_constraint() ::
    {unique, [atom()]}
    | {check, binary()}.

-type operation() ::
    {create_table, binary(), [column_def()]}
    | {create_table, binary(), [column_def()], [table_constraint()]}
    | {drop_table, binary()}
    | {alter_table, binary(), [alter_op()]}
    | {create_index, binary(), [atom()], index_opts_map()}
    | {create_index, binary(), binary(), [atom()], index_opts()}
    | {drop_index, binary()}
    | {execute, binary()}.

-export([index_name/2]).

-export_type([
    operation/0,
    column_def/0,
    alter_op/0,
    index_opts/0,
    index_opts_map/0,
    index_def/0,
    table_constraint/0,
    safe_entry/0
]).

-eqwalizer({nowarn_function, index_name/2}).

-doc "Generate an Ecto-style index name: `{table}_{cols}_index`.".
-spec index_name(binary(), [atom()]) -> binary().
index_name(Table, Cols) ->
    ColsBin = lists:join(<<"_">>, [atom_to_binary(C, utf8) || C <- Cols]),
    iolist_to_binary([Table, <<"_">>, ColsBin, <<"_index">>]).
