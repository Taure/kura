-module(kura_test_raising_type).
-behaviour(kura_type).

-export([cast/1, dump/1, load/1, pg_type/0]).

%% Deliberately non-total: no catch-all clause, so a binary raises
%% function_clause rather than returning {error, _}. This is what a custom
%% type written the natural way looks like.
cast(V) when is_integer(V) -> {ok, V}.

dump(V) when is_integer(V) -> {ok, integer_to_binary(V)};
dump(_) -> {error, ~"cannot dump"}.

load(V) -> {ok, V}.

pg_type() -> ~"TEXT".
