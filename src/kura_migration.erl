-module(kura_migration).

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
