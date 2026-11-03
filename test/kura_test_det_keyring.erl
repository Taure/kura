-module(kura_test_det_keyring).
-behaviour(kura_keyring).

-export([active/0, fetch/1, deterministic_active/0, deterministic_key_ids/0]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    {1, key(1)}.

-spec fetch(kura_keyring:key_id()) -> {ok, kura_keyring:key()} | error.
fetch(KeyId) when KeyId >= 1, KeyId =< 20 ->
    {ok, key(KeyId)};
fetch(_KeyId) ->
    error.

-spec deterministic_active() -> dynamic().
deterministic_active() ->
    application:get_env(kura, test_det_active, {1, key(1)}).

-spec deterministic_key_ids() -> dynamic().
deterministic_key_ids() ->
    application:get_env(kura, test_det_key_ids, [1]).

key(KeyId) ->
    binary:copy(<<KeyId>>, 32).
