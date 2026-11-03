-module(kura_test_lenient_keyring).
-behaviour(kura_keyring).

-export([active/0, fetch/1]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    {1, list_to_binary(lists:seq(1, 32))}.

-spec fetch(kura_keyring:key_id()) -> {ok, kura_keyring:key()} | error.
fetch(_KeyId) ->
    {ok, list_to_binary(lists:seq(1, 32))}.
