-module(kura_keyring).
-moduledoc """
Behaviour for supplying the AES-256 keys `kura_crypto` uses for
at-rest field encryption.

A keyring holds one or more 32-byte keys, each tagged with a `key_id`
(0-255). New values are encrypted with the `active` key; any value can
be decrypted by the key whose `key_id` is embedded in its ciphertext, so
rotation is a matter of adding a new key and flipping `active` - old rows
keep decrypting under their original key with no backfill.

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
