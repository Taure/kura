-module(kura_test_det_only_keyring).
-behaviour(kura_keyring).

-export([active/0, fetch/1, deterministic_active/0]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    {1, key()}.

-spec fetch(kura_keyring:key_id()) -> {ok, kura_keyring:key()} | error.
fetch(4) ->
    {ok, key()};
fetch(_KeyId) ->
    error.

-spec deterministic_active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
deterministic_active() ->
    {4, key()}.

key() ->
    binary:copy(<<4>>, 32).
