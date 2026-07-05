-module(kura_crypto).
-moduledoc """
AES-256-GCM authenticated encryption for kura's at-rest field encryption
(`{encrypted, InnerType}` fields).

A ciphertext is a self-framed binary:

```
<<Version:8, KeyId:8, Nonce:12/binary, Tag:16/binary, CipherText/binary>>
```

`Version` and `KeyId` are authenticated as additional data (AAD), so they
cannot be flipped without failing the tag check. Each value gets a fresh
random 96-bit nonce. Keys come from a `kura_keyring` (default
`kura_keyring_env`); `KeyId` is embedded so decryption is an O(1) key
lookup, never a trial-decrypt.

Every failure **raises** `error({kura_crypto, Reason})` - it never returns
a value or an `{error, _}` tuple. This is deliberate: the dump/load seams
fall open (substitute the raw value on `{error, _}`), so a raise is the
only way to guarantee a crypto failure never writes plaintext to an
encrypted column or returns ciphertext as if it were the value. `Reason`
carries only atoms and the (non-secret) `KeyId` - never key material,
plaintext, or ciphertext, which would leak into crash reports and logs.
""".

-export([encrypt/1, decrypt/1]).

-define(VERSION, 1).
-define(NONCE_BYTES, 12).
-define(TAG_BYTES, 16).

-doc "Encrypt a plaintext binary with the keyring's active key. Raises on failure.".
-spec encrypt(binary()) -> binary().
encrypt(Plain) when is_binary(Plain) ->
    {KeyId, Key} = active_key(),
    Nonce = crypto:strong_rand_bytes(?NONCE_BYTES),
    Aad = <<?VERSION:8, KeyId:8>>,
    {CipherText, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, Key, Nonce, Plain, Aad, true),
    <<?VERSION:8, KeyId:8, Nonce/binary, Tag/binary, CipherText/binary>>.

-doc "Decrypt a framed ciphertext. Raises on a bad tag, unknown key, or bad frame.".
-spec decrypt(binary()) -> binary().
decrypt(
    <<?VERSION:8, KeyId:8, Nonce:?NONCE_BYTES/binary, Tag:?TAG_BYTES/binary, CipherText/binary>>
) ->
    Key = fetch_key(KeyId),
    Aad = <<?VERSION:8, KeyId:8>>,
    case crypto:crypto_one_time_aead(aes_256_gcm, Key, Nonce, CipherText, Aad, Tag, false) of
        error -> erlang:error({kura_crypto, {bad_tag, KeyId}});
        Plain when is_binary(Plain) -> Plain
    end;
decrypt(<<Version:8, _:8, _:?NONCE_BYTES/binary, _:?TAG_BYTES/binary, _/binary>>) ->
    erlang:error({kura_crypto, {unsupported_version, Version}});
decrypt(_) ->
    erlang:error({kura_crypto, malformed_frame}).

active_key() ->
    case (keyring_module()):active() of
        {KeyId, Key} when is_integer(KeyId), KeyId >= 0, KeyId =< 255, is_binary(Key) ->
            {KeyId, Key};
        _ ->
            erlang:error({kura_crypto, no_active_key})
    end.

fetch_key(KeyId) ->
    case (keyring_module()):fetch(KeyId) of
        {ok, Key} when is_binary(Key) -> Key;
        _ -> erlang:error({kura_crypto, {unknown_key_id, KeyId}})
    end.

keyring_module() ->
    application:get_env(kura, keyring, kura_keyring_env).
