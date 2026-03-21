-module(kura_test_money_type).
-behaviour(kura_type).

-export([cast/1, dump/1, load/1, pg_type/0]).

pg_type() -> ~"INTEGER".

cast({dollars, D}) when is_number(D) -> {ok, round(D * 100)};
cast(V) when is_integer(V) -> {ok, V};
cast(_) -> {error, ~"expected integer cents or {dollars, N}"}.

dump(V) when is_integer(V) -> {ok, V};
dump(_) -> {error, ~"expected integer"}.

load(V) when is_integer(V) -> {ok, V};
load(_) -> {error, ~"unexpected value"}.
