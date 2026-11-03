-module(kura_keyring_env).
-moduledoc """
Default `kura_keyring` - reads keys from the `kura` application environment.

```erlang
[{kura, [
    {encryption, #{
        active => 1,
        deterministic_active => 2,
        keys => [
            {1, <<"S2V5T25lLi4uMzIgcmF3IGJ5dGVzLi4uYmFzZTY0ISE=">>},
            {2, <<"S2V5VHdvLi4uMzIgcmF3IGJ5dGVzLi4uYmFzZTY0ISE=">>}
        ]
    }}
]}].
```

Each key is a base64-encoded 32-byte value. `active` is the `key_id` used
for new writes; rotate by adding a key and moving `active` to it. Keys
that do not base64-decode to exactly 32 bytes are treated as absent.

`deterministic_active` is the `key_id` deterministic fields are written
under. It is separate from `active`, and without it deterministic writes
and lookups raise. `key_ids/0` returns the id of every key that resolves,
so a lookup finds rows written under any of them; keep at most 8 keys in
the ring while a deterministic field is in use.
""".

-behaviour(kura_keyring).

-export([active/0, fetch/1, deterministic_active/0, key_ids/0]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    configured_key(active).

-spec deterministic_active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
deterministic_active() ->
    configured_key(deterministic_active).

-spec key_ids() -> [kura_keyring:key_id()].
key_ids() ->
    Cfg = cfg(),
    [
        Id
     || {Id, _} <- maps:get(keys, Cfg, []),
        is_integer(Id),
        Id >= 0,
        Id =< 255,
        key_bytes(Id, Cfg) =/= error
    ].

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
