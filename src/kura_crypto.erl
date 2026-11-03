-module(kura_crypto).
-moduledoc """
AES-256-GCM authenticated encryption for kura's at-rest field encryption
(`{encrypted, InnerType}` fields).

A ciphertext is a self-framed binary. Its first byte is the frame version,
and decryption dispatches on it, so every release reads every version it
has ever written.

**v1** (`encrypt/1`, and `encrypt/2` while the write format is `v1`):

```
<<1:8, KeyId:8, Nonce:12/binary, Tag:16/binary, CipherText/binary>>
```

Encrypted under the keyring's root key with AAD `<<1, KeyId>>`. It is not
bound to a column: a v1 ciphertext copied into another encrypted column
decrypts there.

**v2** (`encrypt/2` while the write format is `v2`):

```
<<2:8, Mode:8, KeyId:16, IV:12/binary, Tag:16/binary, CipherText/binary>>
```

`Mode` is `0` (randomized: a fresh random 96-bit IV per value). `KeyId` is
16 bits wide, although the keyring holds ids 0-255. A v2 ciphertext is bound
to its column context `{Table, Column}` in two ways:

- the AES key is a per-column subkey, never the root key `R`:
  `PRK = HKDF-Extract("kura/enc/v2", R)` and
  `K = HKDF-Expand(PRK, "kura/v2/rnd/gcm" || 0x00 || Ctx, 32)`;
- the AAD is `<<"kura", 2, Mode:8, KeyId:16, Ctx/binary>>`,

where `Ctx = <<TLen:16, Table/binary, CLen:16, Column/binary>>` (HKDF-SHA256,
RFC 5869). A v2 ciphertext moved to another column or table, or with its
mode or key id changed, fails the tag check. Neither the row nor the tenant
is bound, so a database-level writer can still swap two ciphertexts of the
same column between rows. No key material is cached: subkeys are derived
per operation.

The write format comes from the `kura` application environment,
`{encryption_write_format, v1 | v2}` (default `v1`). Reading accepts both.

Every failure **raises** `error({kura_crypto, Reason})` - it never returns
a value or an `{error, _}` tuple. This is deliberate: the write path treats
an `{error, _}` from `kura_types:dump/2` as recoverable and retries it
through `kura_types:cast/2`, and the load seam still falls open, so a raise
is the only way to guarantee a crypto failure never writes plaintext to an
encrypted column or returns ciphertext as if it were the value. `Reason`
carries only atoms and the (non-secret) `KeyId` - never key material,
plaintext, or ciphertext, which would leak into crash reports and logs.
Writing or reading a v2 frame without a column context raises
`no_context`.
""".

-export([encrypt/1, encrypt/2, decrypt/1, decrypt/2, write_format/0]).

-export_type([context/0, write_format/0]).

-ifdef(TEST).
-export([subkey/2]).
-endif.

-doc "The column a v2 ciphertext is bound to: `{Table, Column}`, each 1-65535 bytes.".
-type context() :: {binary(), binary()}.

-doc "The frame version new ciphertexts are written in.".
-type write_format() :: v1 | v2.

-define(V1, 1).
-define(V2, 2).
-define(MODE_RANDOMIZED, 0).
-define(NONCE_BYTES, 12).
-define(TAG_BYTES, 16).
-define(KEY_BYTES, 32).
-define(MAX_NAME_BYTES, 65535).
-define(V2_AAD_PREFIX, ~"kura").
-define(V2_SALT, ~"kura/enc/v2").
-define(V2_RANDOMIZED_INFO, ~"kura/v2/rnd/gcm").

-doc """
Encrypt a plaintext binary with the keyring's active key as a v1 frame.
Raises `no_context` when the write format is `v2`, which needs a column
context (`encrypt/2`).
""".
-spec encrypt(binary()) -> binary().
encrypt(Plain) when is_binary(Plain) ->
    case write_format() of
        v1 -> encrypt_v1(Plain);
        v2 -> erlang:error({kura_crypto, no_context})
    end.

-doc """
Encrypt a plaintext binary for the column `Context`, in the configured write
format (`write_format/0`). Raises on failure.
""".
-spec encrypt(binary(), context()) -> binary().
encrypt(Plain, Context) when is_binary(Plain) ->
    Ctx = context_bytes(Context),
    case write_format() of
        v1 -> encrypt_v1(Plain);
        v2 -> encrypt_v2(Plain, Ctx)
    end.

-doc """
Decrypt a v1 frame. Raises on a bad tag, unknown key or bad frame, and with
`no_context` on a v2 frame, which can only be read with its column context
(`decrypt/2`).
""".
-spec decrypt(binary()) -> binary().
decrypt(Frame) ->
    decrypt_frame(Frame, no_context).

-doc "Decrypt a v1 or v2 frame read from the column `Context`. Raises on any failure.".
-spec decrypt(binary(), context()) -> binary().
decrypt(Frame, Context) ->
    decrypt_frame(Frame, context_bytes(Context)).

-doc """
The frame version new ciphertexts are written in: the `kura` application
environment's `encryption_write_format`, `v1` (the default) or `v2`. Any
other value raises `invalid_write_format`.
""".
-spec write_format() -> write_format().
write_format() ->
    case application:get_env(kura, encryption_write_format, v1) of
        v1 -> v1;
        v2 -> v2;
        _ -> erlang:error({kura_crypto, invalid_write_format})
    end.

encrypt_v1(Plain) ->
    {KeyId, Key} = active_key(),
    Nonce = crypto:strong_rand_bytes(?NONCE_BYTES),
    Aad = <<?V1:8, KeyId:8>>,
    {CipherText, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, Key, Nonce, Plain, Aad, true),
    <<?V1:8, KeyId:8, Nonce/binary, Tag/binary, CipherText/binary>>.

encrypt_v2(Plain, Ctx) ->
    {KeyId, Key} = active_key(),
    IV = crypto:strong_rand_bytes(?NONCE_BYTES),
    Aad = v2_aad(?MODE_RANDOMIZED, KeyId, Ctx),
    {CipherText, Tag} = crypto:crypto_one_time_aead(
        aes_256_gcm, randomized_subkey(Key, Ctx), IV, Plain, Aad, true
    ),
    <<?V2:8, ?MODE_RANDOMIZED:8, KeyId:16, IV/binary, Tag/binary, CipherText/binary>>.

decrypt_frame(
    <<?V1:8, KeyId:8, Nonce:?NONCE_BYTES/binary, Tag:?TAG_BYTES/binary, CipherText/binary>>, _Ctx
) ->
    open(fetch_key(KeyId), Nonce, CipherText, <<?V1:8, KeyId:8>>, Tag, KeyId);
decrypt_frame(<<?V2:8, _/binary>>, no_context) ->
    erlang:error({kura_crypto, no_context});
decrypt_frame(
    <<?V2:8, Mode:8, KeyId:16, IV:?NONCE_BYTES/binary, Tag:?TAG_BYTES/binary, CipherText/binary>>,
    Ctx
) ->
    ok = check_mode(Mode),
    Key = randomized_subkey(fetch_key(KeyId), Ctx),
    open(Key, IV, CipherText, v2_aad(Mode, KeyId, Ctx), Tag, KeyId);
decrypt_frame(<<Version:8, _/binary>>, _Ctx) when Version =/= ?V1, Version =/= ?V2 ->
    erlang:error({kura_crypto, {unsupported_version, Version}});
decrypt_frame(_Frame, _Ctx) ->
    erlang:error({kura_crypto, malformed_frame}).

check_mode(?MODE_RANDOMIZED) -> ok;
check_mode(Mode) -> erlang:error({kura_crypto, {unsupported_mode, Mode}}).

open(Key, IV, CipherText, Aad, Tag, KeyId) ->
    case crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, CipherText, Aad, Tag, false) of
        error -> erlang:error({kura_crypto, {bad_tag, KeyId}});
        Plain when is_binary(Plain) -> Plain
    end.

v2_aad(Mode, KeyId, Ctx) ->
    <<?V2_AAD_PREFIX/binary, ?V2:8, Mode:8, KeyId:16, Ctx/binary>>.

-ifdef(TEST).
-spec subkey(binary(), context()) -> binary().
subkey(RootKey, Context) ->
    randomized_subkey(RootKey, context_bytes(Context)).
-endif.

randomized_subkey(RootKey, Ctx) ->
    PRK = kura_hkdf:extract(?V2_SALT, RootKey),
    kura_hkdf:expand(PRK, <<?V2_RANDOMIZED_INFO/binary, 0, Ctx/binary>>, ?KEY_BYTES).

context_bytes({Table, Column}) when is_binary(Table), is_binary(Column) ->
    <<(name_bytes(Table))/binary, (name_bytes(Column))/binary>>;
context_bytes(_Context) ->
    erlang:error({kura_crypto, invalid_context}).

name_bytes(Name) when byte_size(Name) >= 1, byte_size(Name) =< ?MAX_NAME_BYTES ->
    <<(byte_size(Name)):16, Name/binary>>;
name_bytes(_Name) ->
    erlang:error({kura_crypto, invalid_context}).

active_key() ->
    case (keyring_module()):active() of
        {KeyId, Key} when
            is_integer(KeyId),
            KeyId >= 0,
            KeyId =< 255,
            is_binary(Key),
            byte_size(Key) =:= ?KEY_BYTES
        ->
            {KeyId, Key};
        _ ->
            erlang:error({kura_crypto, no_active_key})
    end.

fetch_key(KeyId) when KeyId > 255 ->
    erlang:error({kura_crypto, {unknown_key_id, KeyId}});
fetch_key(KeyId) ->
    case (keyring_module()):fetch(KeyId) of
        {ok, Key} when is_binary(Key), byte_size(Key) =:= ?KEY_BYTES -> Key;
        _ -> erlang:error({kura_crypto, {unknown_key_id, KeyId}})
    end.

keyring_module() ->
    application:get_env(kura, keyring, kura_keyring_env).
