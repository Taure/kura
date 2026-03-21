-module(kura_test_email_type).
-behaviour(kura_type).

-export([cast/1, dump/1, load/1, pg_type/0]).

pg_type() -> ~"VARCHAR(255)".

cast(V) when is_binary(V) ->
    case binary:match(V, ~"@") of
        nomatch -> {error, ~"must contain @"};
        _ -> {ok, string:lowercase(V)}
    end;
cast(_) ->
    {error, ~"must be a binary"}.

dump(V) when is_binary(V) -> {ok, V};
dump(_) -> {error, ~"expected binary"}.

load(V) when is_binary(V) -> {ok, V};
load(_) -> {error, ~"unexpected value"}.
