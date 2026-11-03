-module(kura_encryption).
-moduledoc """
Equality lookups on deterministic encrypted fields
(`{encrypted, Inner, #{mode => deterministic}}`, see `kura_types:encryption/1`).

`kura_query:where/2` and `having/2` turn `{Field, Value}`, `{Field, '=', Value}`
and `{Field, in, Values}` on a deterministic field into a lookup that compares
the column to ciphertexts, and `{Field, '!=', Value}` and
`{Field, not_in, Values}` into its negation. A value is cast and normalised as
a write of it would be, then encrypted under every key a stored value may be
under (see `kura_crypto:lookup/2`), so the plaintext never enters the query.

`lookup_params/3` returns the same ciphertexts for a `{fragment, SQL, Params}`
condition, which `where/2` does not inspect:

```erlang
Params = kura_encryption:lookup_params(my_user, email, ~"ada@example.com"),
Marks = lists:join(~", ", [~"?" || _ <- Params]),
SQL = iolist_to_binary([~"email IN (", Marks, ~")"]),
Q = kura_query:where(kura_query:from(my_user), {fragment, SQL, Params}).
```

The number of ciphertexts is the number of lookup keys in the keyring (one to
eight), so build the placeholders from the list rather than assuming one.

A lookup reveals which rows hold the looked-up value, and its ciphertexts are
a stable pseudonym of the value wherever the query's parameters are recorded,
such as telemetry and logs.
""".

-export([lookup_params/3]).
-export([lookup/3]).

-doc """
The ciphertexts a lookup of `Value` in the deterministic field `Field` of
`Schema` compares the column against, one per lookup key. Raises
`{kura, {not_a_deterministic_field, Field}}` when `Field` is not a
deterministic field of `Schema`, `{kura, {invalid_lookup_value, Field}}`
when `Value` does not cast to a non-null value of the field, and
`{kura_crypto, Reason}` on a keyring failure.
""".
-spec lookup_params(module(), atom(), term()) -> [binary()].
lookup_params(Schema, Field, Value) when is_atom(Schema) ->
    case kura_schema:encryption_specs(Schema) of
        #{Field := #{mode := deterministic} = Spec} -> lookup(Field, Spec, [Value]);
        #{} -> erlang:error({kura, {not_a_deterministic_field, Field}})
    end.

-doc false.
-spec lookup(atom(), kura_schema:field_encryption(), [term()]) -> [binary()].
lookup(Field, #{context := Context} = Spec, Values) ->
    kura_crypto:lookup(payloads(Field, Spec, Values), Context).

payloads(_Field, _Spec, []) ->
    [];
payloads(Field, Spec, [Value | Rest]) ->
    [payload(Field, Spec, Value) | payloads(Field, Spec, Rest)];
payloads(_Field, _Spec, _Improper) ->
    erlang:error({kura, unrecognised_condition}).

%% nil is a value to cast/2, which would look up the string "nil".
payload(Field, _Spec, Value) when Value =:= nil; Value =:= null; Value =:= undefined ->
    erlang:error({kura, {invalid_lookup_value, Field}});
payload(Field, Spec, Value) ->
    case kura_types:lookup_payload(Spec, Value) of
        {ok, Payload} -> Payload;
        error -> erlang:error({kura, {invalid_lookup_value, Field}})
    end.
