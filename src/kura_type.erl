-module(kura_type).
-moduledoc """
Behaviour for user-defined types.

Implement this behaviour to create custom types that integrate with
Kura's cast/dump/load pipeline.

## Example: Money type (integer cents)

```erlang
-module(money_type).
-behaviour(kura_type).
-export([cast/1, dump/1, load/1, pg_type/0]).

pg_type() -> ~"INTEGER".

cast({dollars, D}) when is_number(D) -> {ok, round(D * 100)};
cast(V) when is_integer(V)          -> {ok, V};
cast(_)                              -> {error, ~"expected integer cents or {dollars, N}"}.

dump(V) when is_integer(V) -> {ok, V};
dump(_)                    -> {error, ~"expected integer"}.

load(V) when is_integer(V) -> {ok, V};
load(_)                    -> {error, ~"unexpected value"}.
```

Then use in your schema:

```erlang
fields() ->
    [#kura_field{name = price, type = {custom, money_type}}].
```
""".

-doc "Cast an external value to the internal representation.".
-callback cast(term()) -> {ok, term()} | {error, binary()}.

-doc "Convert the internal value to a format suitable for PostgreSQL.".
-callback dump(term()) -> {ok, term()} | {error, binary()}.

-doc "Convert a PostgreSQL value back to the internal representation.".
-callback load(term()) -> {ok, term()} | {error, binary()}.

-doc "Return the PostgreSQL DDL type string (e.g. ~\"INTEGER\", ~\"TEXT\").".
-callback pg_type() -> binary().
