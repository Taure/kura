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

## Vector distance

A `{distance, Field, Metric, Vector}` node from `kura_query:distance/3`
can appear in a select expression, in `order_by`, and as the left side
of a `{Distance, Op, Threshold}` condition in a where or a having, `Op`
being `'<'`, `'<='`, `'>'` or `'>='`. `Vector` and `Threshold` are bound
values like any other. `kura_dialect_pg` emits pgvector's `<->`, `<=>`
and `<#>`. A dialect for a backend without vector distance must raise
`{unsupported, vector_distance}` on such a node, and must never fall back
to the PG spelling: in MySQL `<=>` is NULL-safe equality, so a query
that should fail would instead return rows in a meaningless order. A
dialect that delegates to `kura_dialect_pg` has to catch the node before
it does.

## Deterministic lookups

`kura_query:where/2` and `kura_query:having/2` rewrite a comparison on a
deterministic encrypted field into `{Field, encrypted_in, Frames}` or
`{Field, encrypted_not_in, Frames}`, meaning the column is `IN` or
`NOT IN` the values. The values are bound like any other (the shape cache
replaces them with markers), so an implementation passes them through to
`params()` unchanged. They are ciphertext bytes: bind each in the column's
byte type, never as text. The list holds one value per lookup key per
looked-up value, so never assume it has one element. The operator atoms
are kura's own and must never appear in the SQL. A dialect that delegates
to `kura_dialect_pg` but binds binaries as text, as SQLite does, has to
catch the node before it delegates and cast each placeholder to bytes.

## Public API entry point

Consumers go through `kura_query_compiler`, which delegates to the
configured dialect module. They do not call this behaviour's
callbacks directly.
""".

-include("kura.hrl").

-export([qualified_columns/1]).

-export_type([sql/0, params/0, counter/0, table/0]).

-doc """
The table a statement targets: a schema module, whose `table/0` names it, or
a table name as an atom or a binary. The `from` of a query handed to a dialect
takes the same forms; `kura_query:from/1` takes a schema module or an atom.
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

-doc """
Optional. Convert a query's bound values before it is compiled.
`kura_query_compiler` calls it on every query it compiles: before it splits
the query into a cached shape, so it sees the values, and again on the shape
once the tenant condition is added. Return every value you do not convert
unchanged; the result must keep the query's shape. It must be idempotent.
`kura_dialect_pg` converts where values to the field's type here. A dialect
without it binds every value as the caller gave it.

It runs before the encrypted filter guard on the cached path, and a query
built without `kura_query:where/2` never meets that guard's first check, so
an implementation may see the plaintext value of an encrypted field. It must
not raise on a value or log one.
""".
-callback convert_values(#kura_query{}) -> #kura_query{}.

-optional_callbacks([column_type/1, format_default/1, convert_values/1]).

-doc """
Which bare fields of a query name a column of its `from` table.

Returns `undefined` when nothing is qualified: the query has no join, or
its `from` is a bare table or CTE name rather than a schema module.
Otherwise returns `{Table, Fields}`: the `from` schema's `table/0` and the
fields, with their types, that a dialect writes as `"Table"."field"`. Any
other name stays bare, for the server to find among the joined tables.

Under attribute tenancy (`kura_tenant`) the tenant field is left out of
`Fields`. `kura_tenant:scope_query/2` adds the tenant condition for the
`from` table only, so a qualified tenant field would confine the `from`
rows and read the joined table's rows of every tenant. Bare, the field is
ambiguous in a join of two tables that both carry it, and the server
refuses the query.
""".
-spec qualified_columns(#kura_query{}) ->
    undefined | {binary(), #{atom() => kura_types:kura_type()}}.
qualified_columns(#kura_query{joins = []}) ->
    undefined;
qualified_columns(#kura_query{from = From}) when is_atom(From), From =/= undefined ->
    case kura_schema:is_schema(From) of
        true -> {From:table(), unqualified_tenant(kura_schema:field_types(From))};
        false -> undefined
    end;
qualified_columns(#kura_query{}) ->
    undefined.

-spec unqualified_tenant(#{atom() => kura_types:kura_type()}) ->
    #{atom() => kura_types:kura_type()}.
unqualified_tenant(Types) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, _Value}} -> maps:remove(Field, Types);
        _ -> Types
    end.
