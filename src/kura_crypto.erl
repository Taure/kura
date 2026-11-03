-module(kura_crypto).
-moduledoc """
AES-256-GCM authenticated encryption for kura's at-rest field encryption
(`{encrypted, Inner}` and `{encrypted, Inner, Opts}` fields).

A ciphertext is a self-framed binary. Its first byte is the frame version,
and decryption dispatches on it, so by default a release reads every
version earlier releases wrote.

**v1** (`encrypt/1`, and `encrypt/2` while the write format is `v1`):

```
<<1:8, KeyId:8, Nonce:12/binary, Tag:16/binary, CipherText/binary>>
```

Encrypted under the keyring's root key with AAD `<<1, KeyId>>`. It is not
bound to a column: a v1 ciphertext copied into another encrypted column
that accepts v1 frames decrypts there.

**v2** (`encrypt/2` while the write format is `v2`, and always `encrypt/3`):

```
<<2:8, Mode:8, KeyId:16, IV:12/binary, Tag:16/binary, CipherText/binary>>
```

`KeyId` is 16 bits wide, although the keyring holds ids 0-255. A v2
ciphertext is bound to its column context `{Table, Column}`: its key is a
per-column subkey of the root key `R`, never `R` itself, and its AAD is
`<<"kura", 2, Mode:8, KeyId:16, Ctx/binary>>`, where
`Ctx = <<TLen:16, Table/binary, CLen:16, Column/binary>>` and
`PRK = HKDF-Extract("kura/enc/v2", R)` (HKDF-SHA256, RFC 5869). A v2
ciphertext moved to another column or table, or with its mode or key id
changed, fails to decrypt. Neither the row nor the tenant (prefix or
attribute) is bound, so a database-level writer can still swap two
ciphertexts of the same column between rows or tenants, and reorder or
splice the elements of encrypted arrays within the column. No key material
is cached: subkeys are derived per operation.

`Mode` is one of:

- `0`, randomized: `K = HKDF-Expand(PRK, "kura/v2/rnd/gcm" || 0x00 || Ctx, 32)`
  and a fresh random 96-bit IV per value, so equal values encrypt to
  different ciphertexts.
- `1`, deterministic (HMAC-SIV-GCM):
  `K_mac = HKDF-Expand(PRK, "kura/v2/det/siv-hmac" || 0x00 || Ctx, 32)`,
  `K_enc = HKDF-Expand(PRK, "kura/v2/det/gcm" || 0x00 || Ctx, 32)` and
  `IV = HMAC-SHA256(K_mac, AAD || Plaintext)`, first 12 bytes. Equal values
  of one column encrypt to equal ciphertexts under one key, so the column
  can be compared for equality and carry a unique index. On decryption the
  IV is recomputed from the plaintext and compared in constant time: a frame
  whose GCM tag verifies but whose IV is not the synthetic one raises
  `{noncanonical, KeyId}`, so exactly one frame is valid per key, column and
  value. Deterministic frames are written under the keyring's
  `deterministic_active/0` key, never `active/0`.

Deterministic encryption reveals which rows hold equal values (frequency
analysis) and its ciphertext is a stable pseudonym wherever it appears,
including telemetry and logged query parameters. Both modes reveal the
length of a value and whether it is `NULL`. A synthetic IV collision
between two distinct values of one column under one key breaks GCM's
confidentiality for that pair, so keep a deterministic column under 2^32
distinct values per key (a collision probability of about 2^-33).

The write format of `encrypt/1` and `encrypt/2` comes from the `kura`
application environment, `{encryption_write_format, v1 | v2}` (default
`v1`). `encrypt/3` always writes v2.

A column reads with a `t:policy/0`: its mode and its `legacy` setting.
With `legacy => accept`, a v1 frame and a v2 frame of the other mode are
read too; with `legacy => reject` they raise `{refused_version, 1}` and
`{refused_mode, Mode}`. `decrypt/2` reads as
`#{mode => randomized, legacy => accept}`.

Every failure **raises** `error({kura_crypto, Reason})` - it never returns
a value or an `{error, _}` tuple. This is deliberate: the write path treats
an `{error, _}` from `kura_types:dump/2` as recoverable and retries it
through `kura_types:cast/2`, and the load seam still falls open, so a raise
is the only way to guarantee a crypto failure never writes plaintext to an
encrypted column or returns ciphertext as if it were the value. `Reason`
carries only atoms, small integers and the (non-secret) `KeyId` - never key
material, plaintext, or ciphertext, which would leak into crash reports and
logs. Writing or reading a v2 frame without a column context raises
`no_context`.
""".

-export([
    encrypt/1,
    encrypt/2,
    encrypt/3,
    decrypt/1,
    decrypt/2,
    decrypt/3,
    lookup/2,
    is_deterministic_frame/1,
    write_format/0
]).

-export_type([context/0, write_format/0, mode/0, legacy/0, policy/0]).

-ifdef(TEST).
-export([subkey/2, deterministic_subkeys/2]).
-endif.

-doc """
The column a v2 ciphertext is bound to: the table name and the column name,
each 1-65535 bytes.
""".
-type context() :: {Table :: binary(), Column :: binary()}.

-doc "The frame version `encrypt/1` and `encrypt/2` write.".
-type write_format() :: v1 | v2.

-doc "How a v2 value is encrypted: a fresh random IV, or a synthetic one.".
-type mode() :: randomized | deterministic.

-doc "Whether a column reads v1 frames and v2 frames of the other mode.".
-type legacy() :: accept | reject.

-doc "What a column accepts on read.".
-type policy() :: #{mode := mode(), legacy := legacy()}.

-define(V1, 1).
-define(V2, 2).
-define(MODE_RANDOMIZED, 0).
-define(MODE_DETERMINISTIC, 1).
-define(NONCE_BYTES, 12).
-define(TAG_BYTES, 16).
-define(KEY_BYTES, 32).
-define(MAX_NAME_BYTES, 65535).
-define(MAX_LOOKUP_KEYS, 8).
-define(V2_AAD_PREFIX, ~"kura").
-define(V2_SALT, ~"kura/enc/v2").
-define(V2_RANDOMIZED_INFO, ~"kura/v2/rnd/gcm").
-define(V2_SIV_MAC_INFO, ~"kura/v2/det/siv-hmac").
-define(V2_SIV_ENC_INFO, ~"kura/v2/det/gcm").
-define(LEGACY_POLICY, #{mode => randomized, legacy => accept}).

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
    end;
encrypt(_Plain) ->
    erlang:error({kura_crypto, malformed_plaintext}).

-doc """
Encrypt a plaintext binary for the column `Context`, randomized, in the
configured write format (`write_format/0`). Raises on failure.
Schema-aware callers reach it through `kura_types:dump/3`.
""".
-spec encrypt(binary(), context()) -> binary().
encrypt(Plain, Context) when is_binary(Plain) ->
    Ctx = context_bytes(Context),
    case write_format() of
        v1 -> encrypt_v1(Plain);
        v2 -> encrypt_v2(Plain, Ctx)
    end;
encrypt(_Plain, _Context) ->
    erlang:error({kura_crypto, malformed_plaintext}).

-doc """
Encrypt a plaintext binary for the column `Context` as a v2 frame of
`Mode`, whatever the write format. A `deterministic` frame is written under
the keyring's `deterministic_active/0` key and raises
`no_deterministic_key` when there is none. Raises on failure.
""".
-spec encrypt(binary(), context(), mode()) -> binary().
encrypt(Plain, Context, Mode) when is_binary(Plain) ->
    Ctx = context_bytes(Context),
    case Mode of
        randomized -> encrypt_v2(Plain, Ctx);
        deterministic -> encrypt_deterministic(Plain, Ctx);
        _ -> erlang:error({kura_crypto, invalid_mode})
    end;
encrypt(_Plain, _Context, _Mode) ->
    erlang:error({kura_crypto, malformed_plaintext}).

-doc """
Decrypt a v1 frame. Raises on a bad tag, unknown key or bad frame, and with
`no_context` on a v2 frame, which can only be read with its column context
(`decrypt/2`).
""".
-spec decrypt(binary()) -> binary().
decrypt(Frame) ->
    decrypt_frame(Frame, no_context, ?LEGACY_POLICY).

-doc """
Decrypt a v1 or v2 frame read from the column `Context`, as
`decrypt(Frame, Context, #{mode => randomized, legacy => accept})`. Raises
on any failure.
""".
-spec decrypt(binary(), context()) -> binary().
decrypt(Frame, Context) ->
    decrypt_frame(Frame, context_bytes(Context), ?LEGACY_POLICY).

-doc """
Decrypt a frame read from the column `Context`, which accepts what `Policy`
allows (see `t:policy/0`). Raises on any failure, and with
`{refused_version, 1}` or `{refused_mode, Mode}` on a frame the policy does
not accept.
""".
-spec decrypt(binary(), context(), policy()) -> binary().
decrypt(Frame, Context, Policy) ->
    decrypt_frame(Frame, context_bytes(Context), valid_policy(Policy)).

-doc """
The deterministic frames a lookup of each plaintext in `Plains` compares a
column `Context` against: one per plaintext under each key a stored value
may be encrypted under. That is the keyring's `deterministic_active/0` key
first, then every other id of the optional `key_ids/0` callback. More than
8 keys raise `{too_many_lookup_keys, Count}`, so a lookup binds at most
8 values per plaintext. Raises on any failure.
""".
-spec lookup([binary()], context()) -> [binary()].
lookup(Plains, Context) ->
    ok = check_plaintexts(Plains),
    Ctx = context_bytes(Context),
    lookup_frames(lookup_keys(), Plains, Ctx).

-doc """
Return `true` for a binary laid out as a deterministic v2 frame: version 2,
mode 1 and at least the 32-byte header. It says nothing about whether the
frame decrypts.
""".
-spec is_deterministic_frame(term()) -> boolean().
is_deterministic_frame(
    <<?V2:8, ?MODE_DETERMINISTIC:8, _KeyId:16, _IV:?NONCE_BYTES/binary, _Tag:?TAG_BYTES/binary,
        _/binary>>
) ->
    true;
is_deterministic_frame(_Term) ->
    false.

-doc """
The frame version `encrypt/1` and `encrypt/2` write: the `kura` application
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

encrypt_deterministic(Plain, Ctx) ->
    {KeyId, Key} = deterministic_key(),
    deterministic_frame(Plain, Ctx, KeyId, deterministic_subkeys_of(Key, Ctx)).

deterministic_frame(Plain, Ctx, KeyId, {MacKey, EncKey}) ->
    Aad = v2_aad(?MODE_DETERMINISTIC, KeyId, Ctx),
    IV = synthetic_iv(MacKey, Aad, Plain),
    {CipherText, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, EncKey, IV, Plain, Aad, true),
    <<?V2:8, ?MODE_DETERMINISTIC:8, KeyId:16, IV/binary, Tag/binary, CipherText/binary>>.

synthetic_iv(MacKey, Aad, Plain) ->
    <<IV:?NONCE_BYTES/binary, _/binary>> = crypto:mac(hmac, sha256, MacKey, [Aad, Plain]),
    IV.

lookup_frames([], _Plains, _Ctx) ->
    [];
lookup_frames([{KeyId, Key} | Rest], Plains, Ctx) ->
    Subkeys = deterministic_subkeys_of(Key, Ctx),
    [deterministic_frame(Plain, Ctx, KeyId, Subkeys) || Plain <- Plains] ++
        lookup_frames(Rest, Plains, Ctx).

check_plaintexts([]) -> ok;
check_plaintexts([Plain | Rest]) when is_binary(Plain) -> check_plaintexts(Rest);
check_plaintexts(_) -> erlang:error({kura_crypto, malformed_plaintext}).

decrypt_frame(<<?V1:8, _/binary>>, _Ctx, #{legacy := reject}) ->
    erlang:error({kura_crypto, {refused_version, ?V1}});
decrypt_frame(
    <<?V1:8, KeyId:8, Nonce:?NONCE_BYTES/binary, Tag:?TAG_BYTES/binary, CipherText/binary>>,
    _Ctx,
    _Policy
) ->
    open(fetch_key(KeyId), Nonce, CipherText, <<?V1:8, KeyId:8>>, Tag, KeyId);
decrypt_frame(<<?V2:8, _/binary>>, no_context, _Policy) ->
    erlang:error({kura_crypto, no_context});
decrypt_frame(
    <<?V2:8, ModeByte:8, KeyId:16, IV:?NONCE_BYTES/binary, Tag:?TAG_BYTES/binary,
        CipherText/binary>>,
    Ctx,
    Policy
) ->
    Mode = frame_mode(ModeByte),
    ok = check_mode(Mode, ModeByte, Policy),
    open_v2(Mode, KeyId, IV, Tag, CipherText, Ctx);
decrypt_frame(<<Version:8, _/binary>>, _Ctx, _Policy) when Version =/= ?V1, Version =/= ?V2 ->
    erlang:error({kura_crypto, {unsupported_version, Version}});
decrypt_frame(_Frame, _Ctx, _Policy) ->
    erlang:error({kura_crypto, malformed_frame}).

frame_mode(?MODE_RANDOMIZED) -> randomized;
frame_mode(?MODE_DETERMINISTIC) -> deterministic;
frame_mode(ModeByte) -> erlang:error({kura_crypto, {unsupported_mode, ModeByte}}).

check_mode(Mode, _ModeByte, #{mode := Mode}) -> ok;
check_mode(_Mode, _ModeByte, #{legacy := accept}) -> ok;
check_mode(_Mode, ModeByte, _Policy) -> erlang:error({kura_crypto, {refused_mode, ModeByte}}).

open_v2(randomized, KeyId, IV, Tag, CipherText, Ctx) ->
    Key = randomized_subkey(fetch_key(KeyId), Ctx),
    open(Key, IV, CipherText, v2_aad(?MODE_RANDOMIZED, KeyId, Ctx), Tag, KeyId);
open_v2(deterministic, KeyId, IV, Tag, CipherText, Ctx) ->
    {MacKey, EncKey} = deterministic_subkeys_of(fetch_key(KeyId), Ctx),
    Aad = v2_aad(?MODE_DETERMINISTIC, KeyId, Ctx),
    Plain = open(EncKey, IV, CipherText, Aad, Tag, KeyId),
    case crypto:hash_equals(synthetic_iv(MacKey, Aad, Plain), IV) of
        true -> Plain;
        false -> erlang:error({kura_crypto, {noncanonical, KeyId}})
    end.

valid_policy(#{mode := Mode, legacy := Legacy} = Policy) when
    (Mode =:= randomized orelse Mode =:= deterministic),
    (Legacy =:= accept orelse Legacy =:= reject)
->
    Policy;
valid_policy(_Policy) ->
    erlang:error({kura_crypto, invalid_policy}).

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

-spec deterministic_subkeys(binary(), context()) -> {binary(), binary()}.
deterministic_subkeys(RootKey, Context) ->
    deterministic_subkeys_of(RootKey, context_bytes(Context)).
-endif.

randomized_subkey(RootKey, Ctx) ->
    PRK = kura_hkdf:extract(?V2_SALT, RootKey),
    kura_hkdf:expand(PRK, <<?V2_RANDOMIZED_INFO/binary, 0, Ctx/binary>>, ?KEY_BYTES).

deterministic_subkeys_of(RootKey, Ctx) ->
    PRK = kura_hkdf:extract(?V2_SALT, RootKey),
    {
        kura_hkdf:expand(PRK, <<?V2_SIV_MAC_INFO/binary, 0, Ctx/binary>>, ?KEY_BYTES),
        kura_hkdf:expand(PRK, <<?V2_SIV_ENC_INFO/binary, 0, Ctx/binary>>, ?KEY_BYTES)
    }.

context_bytes({Table, Column}) when is_binary(Table), is_binary(Column) ->
    <<(name_bytes(Table))/binary, (name_bytes(Column))/binary>>;
context_bytes(_Context) ->
    erlang:error({kura_crypto, invalid_context}).

name_bytes(Name) when byte_size(Name) >= 1, byte_size(Name) =< ?MAX_NAME_BYTES ->
    <<(byte_size(Name)):16, Name/binary>>;
name_bytes(_Name) ->
    erlang:error({kura_crypto, invalid_context}).

active_key() ->
    case valid_key((keyring_module()):active()) of
        {ok, IdKey} -> IdKey;
        error -> erlang:error({kura_crypto, no_active_key})
    end.

deterministic_key() ->
    Keyring = keyring_module(),
    case optional_callback(Keyring, deterministic_active) of
        true -> deterministic_key(valid_key(Keyring:deterministic_active()));
        false -> erlang:error({kura_crypto, no_deterministic_key})
    end.

deterministic_key({ok, IdKey}) -> IdKey;
deterministic_key(error) -> erlang:error({kura_crypto, no_deterministic_key}).

valid_key({KeyId, Key}) when
    is_integer(KeyId), KeyId >= 0, KeyId =< 255, is_binary(Key), byte_size(Key) =:= ?KEY_BYTES
->
    {ok, {KeyId, Key}};
valid_key(_) ->
    error.

%% The deterministic_active key first, so the most likely match is the first
%% parameter; a ring without key_ids/0 is searched under that key alone.
lookup_keys() ->
    {ActiveId, _Key} = Active = deterministic_key(),
    Others = [Id || Id <- lists:uniq(ring_key_ids()), Id =/= ActiveId],
    case length(Others) + 1 of
        Count when Count > ?MAX_LOOKUP_KEYS ->
            erlang:error({kura_crypto, {too_many_lookup_keys, Count}});
        _ ->
            [Active | [{Id, fetch_key(Id)} || Id <- Others]]
    end.

ring_key_ids() ->
    Keyring = keyring_module(),
    case optional_callback(Keyring, key_ids) of
        true -> valid_key_ids(Keyring:key_ids());
        false -> []
    end.

valid_key_ids([]) ->
    [];
valid_key_ids([Id | Rest]) when is_integer(Id), Id >= 0, Id =< 255 ->
    [Id | valid_key_ids(Rest)];
valid_key_ids(_Ids) ->
    erlang:error({kura_crypto, invalid_key_ids}).

optional_callback(Keyring, Name) ->
    _ = code:ensure_loaded(Keyring),
    erlang:function_exported(Keyring, Name, 0).

fetch_key(KeyId) when KeyId > 255 ->
    erlang:error({kura_crypto, {unknown_key_id, KeyId}});
fetch_key(KeyId) ->
    case (keyring_module()):fetch(KeyId) of
        {ok, Key} when is_binary(Key), byte_size(Key) =:= ?KEY_BYTES -> Key;
        _ -> erlang:error({kura_crypto, {unknown_key_id, KeyId}})
    end.

keyring_module() ->
    application:get_env(kura, keyring, kura_keyring_env).
