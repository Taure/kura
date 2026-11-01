-module(kura_hkdf).
-moduledoc """
HKDF-SHA256 (RFC 5869), built on `crypto:mac/4` since OTP's `crypto` has
no HKDF. Intended for deriving per-purpose subkeys from a keyring root key.

`extract/2` turns input keying material into a pseudorandom key, and
`expand/3` stretches a pseudorandom key into `Length` bytes bound to
`Info`. An empty `Salt` is the RFC's default of 32 zero bytes, since HMAC
pads a short key with zeros.

Raises `{kura_hkdf, badarg}` for a salt, input keying material, PRK or
info that is not a binary, or a length that is not an integer;
`{kura_hkdf, short_prk}` for a PRK shorter than 32 bytes, the hash length
RFC 5869 requires; and `{kura_hkdf, {invalid_length, Length}}` for a
length outside `1..8160` (255 blocks of 32 bytes). The reason never
carries key material, and neither does the stacktrace.
""".

-export([extract/2, expand/3]).

-define(HASH_LEN, 32).
-define(MAX_LENGTH, (255 * ?HASH_LEN)).

-doc "HKDF-Extract: `PRK = HMAC-SHA256(Salt, IKM)`.".
-spec extract(binary(), binary()) -> binary().
extract(Salt, IKM) when is_binary(Salt), is_binary(IKM) ->
    crypto:mac(hmac, sha256, Salt, IKM);
extract(_Salt, _IKM) ->
    erlang:error({kura_hkdf, badarg}).

-doc "HKDF-Expand: `Length` bytes of output keying material from `PRK` and `Info`.".
-spec expand(binary(), binary(), pos_integer()) -> binary().
expand(PRK, Info, Length) when is_binary(PRK), is_binary(Info), is_integer(Length) ->
    expand_checked(PRK, Info, Length);
expand(_PRK, _Info, _Length) ->
    erlang:error({kura_hkdf, badarg}).

expand_checked(PRK, _Info, _Length) when byte_size(PRK) < ?HASH_LEN ->
    erlang:error({kura_hkdf, short_prk});
expand_checked(PRK, Info, Length) when Length >= 1, Length =< ?MAX_LENGTH ->
    Blocks = (Length + ?HASH_LEN - 1) div ?HASH_LEN,
    binary:part(expand_blocks(PRK, Info, 1, Blocks, <<>>, []), 0, Length);
expand_checked(_PRK, _Info, Length) ->
    erlang:error({kura_hkdf, {invalid_length, Length}}).

expand_blocks(_PRK, _Info, N, Blocks, _Prev, Acc) when N > Blocks ->
    iolist_to_binary(lists:reverse(Acc));
expand_blocks(PRK, Info, N, Blocks, Prev, Acc) ->
    T = crypto:mac(hmac, sha256, PRK, <<Prev/binary, Info/binary, N:8>>),
    expand_blocks(PRK, Info, N + 1, Blocks, T, [T | Acc]).
