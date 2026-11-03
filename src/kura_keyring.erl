-module(kura_keyring).
-moduledoc """
Behaviour for supplying the AES-256 keys `kura_crypto` uses for
at-rest field encryption.

A keyring holds one or more 32-byte keys, each tagged with a `key_id`
(0-255). New values are encrypted with the `active` key; any value can
be decrypted by the key whose `key_id` is embedded in its ciphertext, so
rotation is a matter of adding a new key and flipping `active` - old rows
keep decrypting under their original key with no backfill.

Deterministic encryption (`{encrypted, Inner, #{mode => deterministic}}`)
writes under a separate key, `deterministic_active/0`, so rotating `active`
never changes the ciphertext of a value a deterministic column is looked up
by. A lookup encrypts its value under `deterministic_active/0` and under
every other id of `key_ids/0`, at most 8 in all, so rows written under an
earlier deterministic key are still found. Both callbacks are optional:
without `deterministic_active/0` every deterministic write and lookup
raises `{kura_crypto, no_deterministic_key}`, and without `key_ids/0` a
lookup searches under `deterministic_active/0` alone.

The default implementation is `kura_keyring_env`. A regulated deployment
can supply its own module (e.g. reading from a KMS or Vault) by setting
`{kura, [{keyring, my_keyring}]}` in the application environment.
""".

-export_type([key_id/0, key/0]).

-doc "A key identifier, 0-255, embedded in each ciphertext for O(1) lookup.".
-type key_id() :: 0..255.

-doc "A raw 32-byte AES-256 key.".
-type key() :: binary().

-doc """
Return the active key (used to encrypt new writes) as `{KeyId, Key}`, or
`undefined` if none is configured (encryption then fails loudly).
""".
-callback active() -> {key_id(), key()} | undefined.

-doc "Return `{ok, Key}` for `KeyId`, or `error` if it is not in the ring.".
-callback fetch(key_id()) -> {ok, key()} | error.

-doc """
Return the key deterministic values are encrypted under, as `{KeyId, Key}`,
or `undefined` if none is configured (deterministic writes and lookups then
raise).
""".
-callback deterministic_active() -> {key_id(), key()} | undefined.

-doc """
Return the ids of every key in the ring that `fetch/1` resolves. A
deterministic lookup is encrypted under each of them.
""".
-callback key_ids() -> [key_id()].

-optional_callbacks([deterministic_active/0, key_ids/0]).
