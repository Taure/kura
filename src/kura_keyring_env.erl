-module(kura_keyring_env).
-moduledoc """
Default `kura_keyring` - reads keys from the `kura` application environment.

```erlang
[{kura, [
    {encryption, #{
        active => 1,
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
""".

-behaviour(kura_keyring).

-export([active/0, fetch/1]).

-spec active() -> {kura_keyring:key_id(), kura_keyring:key()} | undefined.
active() ->
    Cfg = cfg(),
    case maps:get(active, Cfg, undefined) of
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
