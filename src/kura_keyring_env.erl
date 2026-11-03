-module(kura_keyring_env).
-moduledoc """
Default `kura_keyring` - reads keys from the `kura` application environment.

```erlang
[{kura, [
    {encryption, #{
        active => 1,
        deterministic_active => 3,
        deterministic_keys => [2],
        keys => [
            {1, <<"S2V5T25lLi4uMzIgcmF3IGJ5dGVzLi4uYmFzZTY0ISE=">>},
            {2, <<"S2V5VHdvLi4uMzIgcmF3IGJ5dGVzLi4uYmFzZTY0ISE=">>},
            {3, <<"S2V5VGhyZWUuLjMyIHJhdyBieXRlcy4uLmJhc2U2NCE=">>}
        ]
    }}
]}].
```

Each key is a base64-encoded 32-byte value. `active` is the `key_id` used
for new writes; rotate by adding a key and moving `active` to it. Keys
that do not base64-decode to exactly 32 bytes are treated as absent.

`deterministic_active` is the `key_id` deterministic fields are written
under. It is separate from `active`, and without it deterministic writes
and lookups raise. `deterministic_keys` lists the earlier deterministic
key ids a lookup must still find rows under (default `[]`, so a lookup
searches under `deterministic_active` alone); at most 8 ids in all,
`deterministic_active` included. Keys used only by `active` never enter a
lookup. `deterministic_key_ids/0` raises `{kura_crypto,
invalid_deterministic_keys}` when `deterministic_keys` is not a list of key
ids and `{kura_crypto, {unknown_key_id, Id}}` when an id is not in `keys`.

Rotating a deterministic key: add the new key to `keys`, set
`deterministic_active` to it and add the old id to `deterministic_keys`.
Drop the old id once no stored value is encrypted under it. Until then a
`!=` or `not_in` lookup is correct only while every key a stored value is
encrypted under is in `deterministic_keys` or is `deterministic_active`.
""".

-behaviour(kura_keyring).

-export([active/0, fetch/1, deterministic_active/0, deterministic_key_ids/0]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    configured_key(active).

-spec deterministic_active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
deterministic_active() ->
    configured_key(deterministic_active).

-spec deterministic_key_ids() -> [kura_keyring:key_id()].
deterministic_key_ids() ->
    Cfg = cfg(),
    resolved_ids(maps:get(deterministic_keys, Cfg, []), Cfg).

resolved_ids([], _Cfg) ->
    [];
resolved_ids([Id | Rest], Cfg) when is_integer(Id), Id >= 0, Id =< 255 ->
    case key_bytes(Id, Cfg) of
        {ok, _Key} -> [Id | resolved_ids(Rest, Cfg)];
        error -> erlang:error({kura_crypto, {unknown_key_id, Id}})
    end;
resolved_ids(_Ids, _Cfg) ->
    erlang:error({kura_crypto, invalid_deterministic_keys}).

configured_key(Name) ->
    Cfg = cfg(),
    case maps:get(Name, Cfg, undefined) of
        undefined ->
            undefined;
        Id ->
            case key_bytes(Id, Cfg) of
                {ok, Key} -> {Id, Key};
                error -> undefined
            end
    end.

-spec fetch(kura_keyring:key_id()) -> {ok, kura_keyring:key()} | error.
fetch(KeyId) ->
    key_bytes(KeyId, cfg()).

key_bytes(KeyId, Cfg) ->
    case lists:keyfind(KeyId, 1, maps:get(keys, Cfg, [])) of
        {KeyId, B64} when is_binary(B64) ->
            case decode_key(B64) of
                {ok, Key} -> {ok, Key};
                error -> error
            end;
        _ ->
            error
    end.

decode_key(B64) ->
    try base64:decode(B64) of
        Key when byte_size(Key) =:= 32 -> {ok, Key};
        _ -> error
    catch
        error:_ -> error
    end.

cfg() ->
    application:get_env(kura, encryption, #{}).
