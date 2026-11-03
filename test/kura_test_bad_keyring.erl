-module(kura_test_bad_keyring).
-behaviour(kura_keyring).

-export([active/0, fetch/1]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    {1, binary:copy(<<7>>, 16)}.

-spec fetch(kura_keyring:key_id()) -> {ok, kura_keyring:key()} | error.
fetch(_KeyId) ->
    {ok, binary:copy(<<7>>, 16)}.
