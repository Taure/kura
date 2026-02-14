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
        #kura_column{name = name, type = string, nullable = false}
    ]}].

down() ->
    [{drop_table, <<"users">>}].
```
""".

-include("kura.hrl").

-callback up() -> [operation()].
-callback down() -> [operation()].

-type column_def() :: #kura_column{}.
-type alter_op() ::
    {add_column, column_def()}
    | {drop_column, atom()}
    | {rename_column, atom(), atom()}
    | {modify_column, atom(), kura_types:kura_type()}.

-type index_opts() :: [unique | {where, binary()}].

-type operation() ::
    {create_table, binary(), [column_def()]}
    | {drop_table, binary()}
    | {alter_table, binary(), [alter_op()]}
    | {create_index, binary(), binary(), [atom()], index_opts()}
    | {drop_index, binary()}
    | {execute, binary()}.

-export_type([operation/0, column_def/0, alter_op/0, index_opts/0]).
