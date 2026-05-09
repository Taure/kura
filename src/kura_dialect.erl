-module(kura_dialect).
-moduledoc """
SQL dialect behaviour. Translates a portable `#kura_query{}` AST into
backend-specific SQL.

`#kura_query{}` is the portable AST: it describes operations as atoms
and tuples, not as SQL strings. The dialect callbacks turn that AST
into SQL bytes appropriate for a given backend.

## Today

The canonical implementation is `kura_dialect_pg`, which targets
PostgreSQL via the pgo driver's `$N` placeholder convention.

## Tomorrow

Adding SQLite or MySQL is a matter of providing another implementation
of this behaviour. Differences (placeholder syntax `?` vs `$N`,
identifier quoting `"foo"` vs `` `foo` ``, `RETURNING` vs lastrowid,
`ON CONFLICT` vs `ON DUPLICATE KEY UPDATE`, `LIMIT/OFFSET` vs `FETCH
FIRST`) all live in the dialect impl. The AST stays the same.

## Public API entry point

Consumers go through `kura_query_compiler`, which delegates to the
configured dialect module. They do not call this behaviour's
callbacks directly.
""".

-include("kura.hrl").

-export_type([sql/0, params/0, counter/0]).

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
-callback insert(atom() | module(), [atom()], map()) -> {sql(), params()}.

-doc "Compile a single-row INSERT with options (e.g. `on_conflict`).".
-callback insert(atom() | module(), [atom()], map(), map()) -> {sql(), params()}.

-doc "Compile an UPDATE-by-primary-key.".
-callback update(atom() | module(), [atom()], map(), {atom(), term()}) -> {sql(), params()}.

-doc "Compile a DELETE-by-primary-key.".
-callback delete(atom() | module(), atom(), term()) -> {sql(), params()}.

-doc "Compile a bulk UPDATE (`UPDATE ... WHERE ...`) from a query AST and a SET map.".
-callback update_all(#kura_query{}, map()) -> {sql(), params()}.

-doc "Compile a bulk DELETE (`DELETE ... WHERE ...`) from a query AST.".
-callback delete_all(#kura_query{}) -> {sql(), params()}.

-doc "Compile a bulk INSERT (multi-row).".
-callback insert_all(atom() | module(), [atom()], [map()]) -> {sql(), params()}.

-doc "Compile a bulk INSERT with options (`returning => true | [Field]`).".
-callback insert_all(atom() | module(), [atom()], [map()], map()) -> {sql(), params()}.
