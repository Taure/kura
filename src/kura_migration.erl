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
    [{execute, <<"DROP INDEX CONCURRENTLY \"users_email_index\"">>}].
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

-doc """
A column in an operation: an atom, or the column's name as a binary. Both
are quoted the same way, so `email` and `~"email"` name one column. A
binary carries a name that is not an atom anywhere, such as a schema
field's `column`.
""".
-type column_name() :: atom() | binary().

-type safe_entry() ::
    {drop_column, column_name()}
    | {rename_column, column_name()}
    | {modify_column, column_name()}
    | {add_column, column_name()}
    | drop_table.

-type column_def() :: #kura_column{}.
-type alter_op() ::
    {add_column, column_def()}
    | {drop_column, column_name()}
    | {rename_column, column_name(), column_name()}
    | {modify_column, column_name(), kura_types:kura_type()}.

-type index_opts() :: [unique | concurrently | {where, binary()}].
-type index_opts_map() :: #{
    unique => boolean(),
    where => binary(),
    concurrently => boolean(),
    name => binary(),
    using => atom(),
    opclass => atom(),
    with => #{atom() => integer()}
}.
-type index_def() :: {[atom()], index_opts_map()}.

-type table_constraint() ::
    {primary_key, [atom()]}
    | {unique, [atom()]}
    | {check, binary()}.

-doc "A table constraint as an operation carries it.".
-type ddl_constraint() ::
    {primary_key, [column_name()]}
    | {unique, [column_name()]}
    | {check, binary()}.

-type operation() ::
    {create_table, binary(), [column_def()]}
    | {create_table, binary(), [column_def()], [ddl_constraint()]}
    | {drop_table, binary()}
    | {alter_table, binary(), [alter_op()]}
    | {create_index, binary(), [column_name()], index_opts_map()}
    | {create_index, binary(), binary(), [column_name()], index_opts()}
    | {drop_index, binary()}
    | {execute, binary()}.

-export([index_name/2, index_name/3, column_text/1]).

-export_type([
    operation/0,
    column_def/0,
    alter_op/0,
    index_opts/0,
    index_opts_map/0,
    index_def/0,
    table_constraint/0,
    ddl_constraint/0,
    column_name/0,
    safe_entry/0
]).

-doc "Generate an Ecto-style index name: `{table}_{cols}_index`.".
-spec index_name(binary(), [column_name()]) -> binary().
index_name(Table, Cols) ->
    ColsBin = join_with_sep([column_text(C) || C <- Cols], ~"_"),
    iolist_to_binary([Table, ~"_", ColsBin, ~"_index"]).

-doc """
The name of an index declared with the map options `Opts`: its `name`
when given, otherwise `index_name/2`. A `name` that is not a binary of 1
to 63 bytes (PostgreSQL's identifier limit) raises
`{invalid_index_option, name, Name}`.
""".
-spec index_name(binary(), [column_name()], index_opts_map()) -> binary().
index_name(_Table, _Cols, #{name := Name}) when
    is_binary(Name), byte_size(Name) > 0, byte_size(Name) =< 63
->
    Name;
index_name(_Table, _Cols, #{name := Name}) ->
    error({invalid_index_option, name, Name});
index_name(Table, Cols, #{}) ->
    index_name(Table, Cols).

-doc "The name of `Column` as text.".
-spec column_text(column_name()) -> binary().
column_text(Column) when is_binary(Column) -> Column;
column_text(Column) when is_atom(Column) -> atom_to_binary(Column, utf8).

-spec join_with_sep([binary()], binary()) -> [binary()].
join_with_sep([], _Sep) -> [];
join_with_sep([H], _Sep) -> [H];
join_with_sep([H | T], Sep) -> [H, Sep | join_with_sep(T, Sep)].
