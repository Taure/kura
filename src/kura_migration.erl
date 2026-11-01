-module(kura_migration).
-moduledoc """
Behaviour for defining database migrations.

Implement `up/0` and `down/0` returning a list of DDL operations.
Migration modules must be named `m<YYYYMMDDHHMMSS>_<name>`. `up/0` and
`down/0` may be called more than once per run, so they must be pure.

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

## Non-transactional migrations

The migrator runs a migration's operations inside a transaction, so a
failing statement leaves nothing behind. Some statements cannot run in
a transaction at all; `CREATE INDEX CONCURRENTLY` is the usual one. A
migration opts out by exporting `transactional/0` returning `false`:

```erlang
-module(m20250102120000_index_users_email).
-behaviour(kura_migration).
-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() ->
    [{create_index, <<"users">>, [email], #{concurrently => true}}].

down() ->
    [{drop_index, <<"users_email_index">>}].
```

Each statement then commits on its own. The version is recorded only
after the last one succeeds, so a failure part-way leaves the earlier
statements committed and the migration pending. Keep such a migration
to one statement, or make every statement safe to re-run.

`concurrently => true` is refused in a migration that is still
transactional. See `m:kura_migrator` for how the run is split and how
the advisory lock is held.
""".

-include("kura.hrl").

-callback up() -> [operation()].
-callback down() -> [operation()].
-callback safe() -> [safe_entry()].
-doc """
Optional. Return `false` to run the migration outside a transaction.

A missing callback, or any other value, keeps the migration
transactional.
""".
-callback transactional() -> boolean().
-optional_callbacks([safe/0, transactional/0]).

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

-type index_opts() :: [unique | concurrently | {where, binary()}].
-type index_opts_map() :: #{unique => boolean(), where => binary(), concurrently => boolean()}.
-type index_def() :: {[atom()], index_opts_map()}.

-type table_constraint() ::
    {primary_key, [atom()]}
    | {unique, [atom()]}
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

-doc "Generate an Ecto-style index name: `{table}_{cols}_index`.".
-spec index_name(binary(), [atom()]) -> binary().
index_name(Table, Cols) ->
    ColsBin = join_with_sep([atom_to_binary(C, utf8) || C <- Cols], ~"_"),
    iolist_to_binary([Table, ~"_", ColsBin, ~"_index"]).

-spec join_with_sep([binary()], binary()) -> [binary()].
join_with_sep([], _Sep) -> [];
join_with_sep([H], _Sep) -> [H];
join_with_sep([H | T], Sep) -> [H, Sep | join_with_sep(T, Sep)].
