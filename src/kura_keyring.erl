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
every other id of `deterministic_key_ids/0`, at most 8 in all, so rows
written under an earlier deterministic key are still found. An `in` of N
values binds N x K ciphertexts for K lookup keys, so PostgreSQL's limit of
65535 parameters is reached at N > 65535 / K. Keys used only
for randomized writes never enter a lookup. Both callbacks are optional:
without `deterministic_active/0` every deterministic write and lookup
raises `{kura_crypto, no_deterministic_key}`, and without
`deterministic_key_ids/0` a lookup searches under `deterministic_active/0`
alone.

To rotate a deterministic key, add the new key to the ring, make it
`deterministic_active/0` and keep the old id in `deterministic_key_ids/0`
until no stored value is encrypted under it, then drop it. A `!=` or
`not_in` lookup is correct only while every key a stored value is encrypted
under is in that set: a row under a key outside it never matches the
excluded value, so it is returned.

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
Return the ids of the earlier deterministic keys whose values a lookup must
still find. A deterministic lookup is encrypted under each of them and under
`deterministic_active/0`, which may be listed or not. Each id is fetched
through `fetch/1`, and a lookup raises `{kura_crypto, {unknown_key_id, Id}}`
when one does not resolve, and `{kura_crypto, invalid_key_ids}` when the
result is not a list of key ids (0-255).
""".
-callback deterministic_key_ids() -> [key_id()].

-optional_callbacks([deterministic_active/0, deterministic_key_ids/0]).
