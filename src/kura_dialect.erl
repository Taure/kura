-module(kura_dialect).
-moduledoc """
SQL dialect behaviour. Translates a portable `#kura_query{}` AST into
backend-specific SQL.

`#kura_query{}` is the portable AST: it describes operations as atoms
and tuples, not as SQL strings. The dialect callbacks turn that AST
into SQL bytes appropriate for a given backend.

## Today

The canonical implementation is `kura_dialect_pg`, which targets
PostgreSQL with its `$N` placeholder convention.

## Tomorrow

Adding SQLite or MySQL is a matter of providing another implementation
of this behaviour. Differences (placeholder syntax `?` vs `$N`,
identifier quoting `"foo"` vs `` `foo` ``, `RETURNING` vs lastrowid,
`ON CONFLICT` vs `ON DUPLICATE KEY UPDATE`, `LIMIT/OFFSET` vs `FETCH
FIRST`) all live in the dialect impl. The AST stays the same.

Many-to-many join tables are read and written through the same callbacks:
`to_sql/1` for the join read, `delete_all/1` and `insert_all/3` for the
rows `put_assoc` and `cast_assoc` replace. They are called on the dialect
directly, without the attribute tenant condition, since a join table has no
tenant column; a prefix tenant still qualifies the table.

## Bound values

`kura_query_compiler:to_sql_cached/2` compiles a query's shape once, with
every bound value replaced by a marker (see `kura_query_shape`), and
reuses the result for every call that differs only in its values. An
implementation must therefore pass a value in a bound position through to
`params()` unchanged, and never inspect it or write it into the SQL text.
That includes `limit` and `offset`: in a shape they hold a marker, not
the integer the record type names.

A template whose markers are missing, repeated or nested is detected and
not cached, and so is a shape the implementation raises on, but SQL that
varies with a value's content cannot be. `nil` and `undefined` are never
replaced by a marker, so an implementation may compile them specially.

## Public API entry point

Consumers go through `kura_query_compiler`, which delegates to the
configured dialect module. They do not call this behaviour's
callbacks directly.
""".

-include("kura.hrl").

-export_type([sql/0, params/0, counter/0, table/0]).

-doc """
The table a statement targets: a schema module, whose `table/0` names it, or
a table name as an atom or a binary. A query's `from` takes the same forms.
A many-to-many `join_through` given as a binary reaches the dialect as one.
""".
-type table() :: atom() | module() | binary().

-doc "Compiled SQL bytes. Implementations are free to return iodata or a binary.".
-type sql() :: iodata().

-doc "Parameter list in the order the placeholders appear in `sql()`.".
-type params() :: [term()].

-doc """
Next placeholder index. Used when composing sub-queries (CTEs,
sub-selects in WHERE) so placeholders stay contiguous across the
entire compiled statement.
""".
-type counter() :: pos_integer().

-doc "Compile a query AST into `{SQL, Params}`.".
-callback to_sql(#kura_query{}) -> {sql(), params()}.

-doc """
Compile a query AST starting placeholder numbering at `StartCounter`.
Returns the next free counter so callers can chain sub-compilations.
""".
-callback to_sql_from(#kura_query{}, counter()) -> {sql(), params(), counter()}.

-doc "Compile a single-row INSERT (no options).".
-callback insert(table(), [atom()], map()) -> {sql(), params()}.

-doc """
Compile a single-row INSERT with options (e.g. `on_conflict`).

`on_conflict` may carry `#{guard => [Field]}` as a third element. The update
must then only apply where each field of the existing row equals the
incoming one. A dialect that cannot express that must raise, never drop the
guard: it is what keeps an upsert inside its tenant.
""".
-callback insert(table(), [atom()], map(), map()) -> {sql(), params()}.

-doc "Compile an UPDATE-by-primary-key. The key is one clause per column (single-key is a one-element list).".
-callback update(table(), [atom()], map(), [{atom(), term()}]) -> {sql(), params()}.

-doc "Compile a DELETE-by-primary-key. The key is one clause per column (single-key is a one-element list).".
-callback delete(table(), [{atom(), term()}]) -> {sql(), params()}.

-doc "Compile a bulk UPDATE (`UPDATE ... WHERE ...`) from a query AST and a SET map.".
-callback update_all(#kura_query{}, map()) -> {sql(), params()}.

-doc "Compile a bulk DELETE (`DELETE ... WHERE ...`) from a query AST.".
-callback delete_all(#kura_query{}) -> {sql(), params()}.

-doc "Compile a bulk INSERT (multi-row).".
-callback insert_all(table(), [atom()], [map()]) -> {sql(), params()}.

-doc "Compile a bulk INSERT with options (`returning`, and `on_conflict` as for `insert/4`, guard included).".
-callback insert_all(table(), [atom()], [map()], map()) -> {sql(), params()}.

-doc "Optional. SQL bytes for a kura column type, used by `kura_migrator` DDL.".
-callback column_type(kura_types:kura_type()) -> binary().

-doc "Optional. SQL bytes for a column default value, used by `kura_migrator` DDL.".
-callback format_default(term()) -> binary().

-optional_callbacks([column_type/1, format_default/1]).
