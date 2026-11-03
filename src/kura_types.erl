-module(kura_types).
-moduledoc """
Type system for casting, dumping, and loading values between Erlang and PostgreSQL.

Supported types: `id`, `integer`, `float`, `string`, `text`, `boolean`,
`date`, `utc_datetime`, `uuid`, `jsonb`, `{array, Type}`.

- `cast/2` - coerce external input to Erlang terms
- `dump/2` - convert Erlang terms to values the driver can bind
- `load/2` - convert driver results back to Erlang terms
- `dump/3`, `load/3` - the same for a value of a known column
  `{Table, Column}`, which an encrypted value needs to be written or read
  as a column-bound v2 frame (see `kura_crypto`)

`{encrypted, Inner}` and `{encrypted, Inner, Opts}` store `Inner`
encrypted (see `encryption/1`). With `#{mode => deterministic}` equal values
encrypt equally, so the field can be looked up by equality and carry a
unique index, but which rows share a value is revealed, and the
ciphertext bound in a lookup is a stable pseudonym in telemetry and logs.
Both modes reveal a value's length and whether it is `NULL`, and the value
stays personal data under the GDPR.
""".

-export([
    to_pg_type/1,
    from_pg_type/2,
    cast/2,
    dump/2,
    dump/3,
    load/2,
    load/3,
    encryption/1,
    validate_encryption/1,
    encryptable_inner/1,
    deterministic_inner/1,
    lookup_payload/2
]).

-export_type([
    kura_type/0,
    encryption/0,
    encryption_spec/0,
    encryption_opts/0,
    normalize/0
]).

-type kura_type() ::
    id
    | integer
    | bigint
    | smallint
    | float
    | decimal
    | string
    | text
    | binary
    | boolean
    | date
    | time
    | utc_datetime
    | naive_datetime
    | uuid
    | jsonb
    | {enum, [atom()]}
    | {array, kura_type()}
    | {encrypted, kura_type()}
    | {encrypted, kura_type(), encryption_opts()}
    | vector
    | {vector, pos_integer()}
    | {embed, embeds_one | embeds_many, module()}
    | {custom, module()}.

-doc """
The options of an `{encrypted, Inner, Opts}` field. Every key is optional:

- `mode` - `randomized` (the default) or `deterministic`;
- `normalize` - `none` (the default), `casefold`, `nfc` or `nfc_casefold`,
  applied to a deterministic `string` or `text` value before it is
  encrypted or compared. `casefold` is Unicode full case folding
  (`string:casefold/1`) without the Turkic mappings, so dotless `ı` and `I`
  stay distinct;
- `context` - the `{Table, Column}` the ciphertext is bound to, in place of
  the schema's table and the field's column;
- `legacy` - `reject` (the default) or `accept`: whether the column reads a
  v1 frame or a v2 frame of the other mode.
""".
-type encryption_opts() :: #{
    mode => kura_crypto:mode(),
    normalize => normalize(),
    context => kura_crypto:context(),
    legacy => kura_crypto:legacy()
}.
-doc "How a deterministic `string` or `text` value is normalised.".
-type normalize() :: none | casefold | nfc | nfc_casefold.
-type encryption() :: none | {ok, encryption_spec()} | unsupported.
-type encryption_spec() :: #{
    inner := kura_type(),
    mode := kura_crypto:mode(),
    legacy := kura_crypto:legacy(),
    normalize := normalize(),
    context => kura_crypto:context(),
    array => pos_integer()
}.

-define(OPTION_KEYS, [mode, normalize, context, legacy]).

%% Any tuple tagged `encrypted`, whatever its arity, so a form this release
%% does not support is still treated as encrypted and fails closed.
-define(IS_ENCRYPTED_TAG(T), (is_tuple(T) andalso element(1, T) =:= encrypted)).

-ifdef(TEST).
-export([encode_inner/2, decode_inner/2]).
-endif.

%%----------------------------------------------------------------------
%% PG DDL type strings
%%----------------------------------------------------------------------

-doc "Return the PostgreSQL DDL type string for a kura type.".
-spec to_pg_type(kura_type()) -> binary().
to_pg_type(id) -> ~"BIGSERIAL";
to_pg_type(integer) -> ~"INTEGER";
to_pg_type(bigint) -> ~"BIGINT";
to_pg_type(smallint) -> ~"SMALLINT";
to_pg_type(float) -> ~"DOUBLE PRECISION";
to_pg_type(decimal) -> ~"NUMERIC";
to_pg_type(string) -> ~"VARCHAR(255)";
to_pg_type(text) -> ~"TEXT";
to_pg_type(binary) -> ~"BYTEA";
to_pg_type(boolean) -> ~"BOOLEAN";
to_pg_type(date) -> ~"DATE";
to_pg_type(time) -> ~"TIME";
to_pg_type(utc_datetime) -> ~"TIMESTAMPTZ";
to_pg_type(naive_datetime) -> ~"TIMESTAMP";
to_pg_type(uuid) -> ~"UUID";
to_pg_type(jsonb) -> ~"JSONB";
to_pg_type({enum, _}) -> ~"VARCHAR(255)";
to_pg_type({array, Inner} = Type) -> array_pg_type(encryption(Type), Inner);
to_pg_type(Type) when ?IS_ENCRYPTED_TAG(Type) -> encrypted_pg_type(encryption(Type));
to_pg_type(vector) -> ~"VECTOR";
to_pg_type({vector, N}) -> <<"VECTOR(", (integer_to_binary(N))/binary, ")">>;
to_pg_type({embed, _, _}) -> ~"JSONB";
to_pg_type({custom, Mod}) -> Mod:pg_type().

-doc """
Map a PostgreSQL catalog type name (`udt_name`, e.g. `~"int4"`) to a kura
field type - the inverse of `to_pg_type/1`. Used by schema introspection
(`rebar3 kura gen_schemas`) to bootstrap schemas from an existing database.

`Opts` supplies context the type name alone does not carry:

- `char_max_length` - distinguishes `~"varchar"` at 255 (`string`) from
  any other length (`text`).
- `serial` - an `~"int8"` column with a `nextval` default is kura's `id`
  (BIGSERIAL); a plain `~"int8"` is `bigint`.

`float4` (REAL) widens to `float` since kura models a single float type
(DOUBLE PRECISION). Returns `{unsupported, TypeName}` for a PG type kura
does not model (including native `CREATE TYPE ... AS ENUM` types), so the
caller can skip the field and warn rather than guessing.
""".
-spec from_pg_type(
    binary(), #{serial => boolean(), char_max_length => non_neg_integer()}
) -> kura_type() | {unsupported, binary()}.
from_pg_type(~"int8", #{serial := true}) ->
    id;
from_pg_type(~"int8", _) ->
    bigint;
from_pg_type(~"int4", _) ->
    integer;
from_pg_type(~"int2", _) ->
    smallint;
from_pg_type(~"float4", _) ->
    float;
from_pg_type(~"float8", _) ->
    float;
from_pg_type(~"numeric", _) ->
    decimal;
from_pg_type(~"varchar", #{char_max_length := 255}) ->
    string;
from_pg_type(~"varchar", _) ->
    text;
from_pg_type(~"bpchar", _) ->
    text;
from_pg_type(~"text", _) ->
    text;
from_pg_type(~"bytea", _) ->
    binary;
from_pg_type(~"bool", _) ->
    boolean;
from_pg_type(~"date", _) ->
    date;
from_pg_type(~"time", _) ->
    time;
from_pg_type(~"timestamptz", _) ->
    utc_datetime;
from_pg_type(~"timestamp", _) ->
    naive_datetime;
from_pg_type(~"uuid", _) ->
    uuid;
from_pg_type(~"json", _) ->
    jsonb;
from_pg_type(~"jsonb", _) ->
    jsonb;
from_pg_type(~"vector", _) ->
    %% pgvector: dimension is not in the catalog Opts, so introspection
    %% yields a dimensionless vector (annotate {vector, N} by hand).
    vector;
from_pg_type(<<"_", Elem/binary>>, Opts) ->
    case from_pg_type(Elem, Opts) of
        {unsupported, _} = Unsupported -> Unsupported;
        Inner -> {array, Inner}
    end;
from_pg_type(Other, _) when is_binary(Other) ->
    {unsupported, Other}.

%%----------------------------------------------------------------------
%% Cast: coerce external input → Erlang term
%%----------------------------------------------------------------------

-doc "Coerce external input to an Erlang term for the given type.".
-spec cast(kura_type(), dynamic()) -> {ok, term()} | {error, binary()}.
cast(_Type, undefined) ->
    {ok, undefined};
cast(_Type, null) ->
    {ok, undefined};
cast(id, V) when is_integer(V) ->
    {ok, V};
cast(id, V) when is_binary(V) ->
    try_parse_integer(V);
cast(integer, V) when is_integer(V) ->
    {ok, V};
cast(integer, V) when is_binary(V) ->
    try_parse_integer(V);
cast(bigint, V) when is_integer(V) ->
    {ok, V};
cast(bigint, V) when is_binary(V) ->
    try_parse_integer(V);
cast(smallint, V) when is_integer(V) ->
    {ok, V};
cast(smallint, V) when is_binary(V) ->
    try_parse_integer(V);
cast(decimal, V) when is_number(V) ->
    {ok, V};
cast(decimal, V) when is_binary(V) ->
    try_parse_float(V);
cast(binary, V) when is_binary(V) ->
    {ok, V};
cast(float, V) when is_float(V) ->
    {ok, V};
cast(float, V) when is_integer(V) ->
    {ok, erlang:float(V)};
cast(float, V) when is_binary(V) ->
    try_parse_float(V);
cast(string, V) when is_binary(V) ->
    {ok, V};
cast(string, V) when is_list(V) ->
    {ok, list_to_binary(V)};
cast(string, V) when is_atom(V) ->
    {ok, atom_to_binary(V, utf8)};
cast(text, V) when is_binary(V) ->
    {ok, V};
cast(text, V) when is_list(V) ->
    {ok, list_to_binary(V)};
cast(boolean, V) when is_boolean(V) ->
    {ok, V};
cast(boolean, 1) ->
    {ok, true};
cast(boolean, 0) ->
    {ok, false};
cast(boolean, ~"true") ->
    {ok, true};
cast(boolean, ~"false") ->
    {ok, false};
cast(boolean, ~"1") ->
    {ok, true};
cast(boolean, ~"0") ->
    {ok, false};
cast(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
cast(date, V) when is_binary(V) ->
    parse_date(V);
cast(time, {H, M, S} = V) when is_integer(H), is_integer(M), is_integer(S) ->
    {ok, V};
cast(time, V) when is_binary(V) ->
    parse_time(V);
cast(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
cast(utc_datetime, V) when is_binary(V) ->
    parse_datetime(V);
cast(naive_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
cast(naive_datetime, V) when is_binary(V) ->
    parse_datetime(V);
cast(uuid, V) when is_binary(V), byte_size(V) =:= 36 ->
    {ok, V};
cast(uuid, V) when is_binary(V), byte_size(V) =:= 32 ->
    {ok, format_uuid(V)};
cast(uuid, V) when is_binary(V), byte_size(V) =:= 16 ->
    {ok, format_uuid(binary:encode_hex(V, lowercase))};
cast(jsonb, V) when is_map(V) ->
    {ok, V};
cast(jsonb, V) when is_list(V) ->
    {ok, V};
cast(jsonb, V) when is_binary(V) ->
    json_decode(V);
%% Postgres jsonb columns are happy with any valid JSON at the root, not
%% just objects/arrays. Accept scalar numbers, booleans, and `null` so
%% callers can store e.g. a single counter without a wrapper map.
cast(jsonb, V) when is_number(V); is_boolean(V) ->
    {ok, V};
cast({enum, Values}, V) when is_atom(V) ->
    case lists:member(V, Values) of
        true -> {ok, V};
        false -> {error, ~"is not a valid enum value"}
    end;
cast({enum, Values}, V) when is_binary(V) ->
    try
        Atom = binary_to_existing_atom(V, utf8),
        case lists:member(Atom, Values) of
            true -> {ok, Atom};
            false -> {error, ~"is not a valid enum value"}
        end
    catch
        error:badarg -> {error, ~"is not a valid enum value"}
    end;
cast({enum, Values}, V) when is_list(V) ->
    try
        Atom = list_to_existing_atom(V),
        case lists:member(Atom, Values) of
            true -> {ok, Atom};
            false -> {error, ~"is not a valid enum value"}
        end
    catch
        error:badarg -> {error, ~"is not a valid enum value"}
    end;
cast({array, Inner}, V) when is_list(V) ->
    cast_array(Inner, V, []);
cast({embed, embeds_one, _}, V) when is_map(V) ->
    {ok, V};
cast({embed, embeds_many, _}, V) when is_list(V) ->
    {ok, V};
cast(Type, V) when ?IS_ENCRYPTED_TAG(Type) ->
    cast_encrypted(encryption(Type), V);
cast(vector, V) when is_list(V) ->
    cast_vector(V, undefined);
cast({vector, N}, V) when is_list(V) ->
    cast_vector(V, N);
cast({custom, Mod}, V) ->
    Mod:cast(V);
cast(Type, _V) ->
    {error, <<"cannot cast to ", (format_type(Type))/binary>>}.

%%----------------------------------------------------------------------
%% Dump: Erlang term → driver value
%%----------------------------------------------------------------------

-doc "Convert an Erlang term to a value the driver can bind for storage.".
-spec dump(kura_type(), term()) -> {ok, term()} | {error, binary()}.
dump(_Type, undefined) ->
    {ok, null};
dump(_Type, null) ->
    {ok, null};
dump(id, V) when is_integer(V) ->
    {ok, V};
dump(integer, V) when is_integer(V) ->
    {ok, V};
dump(bigint, V) when is_integer(V) ->
    {ok, V};
dump(smallint, V) when is_integer(V) ->
    {ok, V};
dump(decimal, V) when is_number(V) ->
    {ok, V};
dump(binary, V) when is_binary(V) ->
    {ok, V};
dump(float, V) when is_float(V) ->
    {ok, V};
dump(float, V) when is_integer(V) ->
    {ok, erlang:float(V)};
dump(string, V) when is_binary(V) ->
    {ok, V};
dump(text, V) when is_binary(V) ->
    {ok, V};
dump(boolean, V) when is_boolean(V) ->
    {ok, V};
dump(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
dump(time, {H, M, S} = V) when is_integer(H), is_integer(M), is_integer(S) ->
    {ok, V};
dump(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
dump(naive_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
dump(uuid, V) when is_binary(V) ->
    {ok, V};
dump(jsonb, V) when is_map(V); is_list(V) ->
    json_encode(V);
%% Postgres jsonb at the wire level is just a JSON document - scalars,
%% booleans, and null are all valid roots.
dump(jsonb, V) when is_number(V); is_boolean(V) ->
    json_encode(V);
%% A binary is ambiguous: an already-serialised document from a bulk-write
%% caller, or a JSON string scalar that `load/2` or `cast/2` decoded out of
%% one. Encoding the first turns a document into a quoted string; passing the
%% second through turns the stored string "123" into the number 123 on the
%% next read-modify-write. Only an object or an array is unambiguously a
%% document - every scalar has an unambiguous non-binary spelling.
dump(jsonb, V) when is_binary(V) ->
    case json_decode(V) of
        {ok, Doc} when is_map(Doc); is_list(Doc) -> {ok, V};
        _ -> json_encode(V)
    end;
dump({enum, _}, V) when is_atom(V) ->
    {ok, atom_to_binary(V, utf8)};
dump({array, Inner}, V) when is_list(V) ->
    dump_array(Inner, V, no_context, []);
dump({embed, embeds_one, Mod}, V) when is_map(V) ->
    json_encode(dump_embed_to_term(Mod, V));
dump({embed, embeds_many, Mod}, V) when is_list(V) ->
    json_encode([dump_embed_to_term(Mod, Item) || Item <- V]);
dump(Type, V) when ?IS_ENCRYPTED_TAG(Type) ->
    dump_encrypted(Type, V, no_context);
dump(vector, V) when is_list(V) ->
    {ok, vector_to_text(V)};
dump({vector, _}, V) when is_list(V) ->
    {ok, vector_to_text(V)};
dump({custom, Mod}, V) ->
    Mod:dump(V);
dump(Type, _V) ->
    {error, <<"cannot dump ", (format_type(Type))/binary>>}.

-doc """
Like `dump/2`, for a value of the column `Context` (`{Table, Column}`). An
encrypted value is encrypted for that column, so it can be written in either
frame version (see `kura_crypto`); `dump/2` writes v1 only and raises
`{kura_crypto, no_context}` when the write format is `v2`. Any other type
dumps as with `dump/2`.
""".
-spec dump(kura_type(), term(), kura_crypto:context()) -> {ok, term()} | {error, binary()}.
dump(Type, V, Context) ->
    dump_value(Type, V, Context).

dump_value(_Type, V, _Context) when V =:= undefined; V =:= null ->
    {ok, null};
dump_value(Type, V, Context) when ?IS_ENCRYPTED_TAG(Type) ->
    dump_encrypted(Type, V, Context);
dump_value({array, Inner}, V, Context) when is_list(V) ->
    dump_array(Inner, V, Context, []);
dump_value(Type, V, _Context) ->
    dump(Type, V).

%% Raise (never return {error}) so no caller can substitute plaintext for an
%% encrypted field by treating a dump failure as recoverable.
dump_encrypted(Type, V, Context) ->
    #{inner := Inner} = Spec = encryptable(encryption(Type)),
    {ok, Wire} = dump(Inner, V),
    Payload = encode_inner(Inner, canonical_wire(Spec, Wire)),
    {ok, encrypt_payload(Type, Spec, Payload, Context)}.

%% The two-element form keeps its v1-era semantics: the write format decides
%% the frame. The options form is always written as v2 under its own mode.
encrypt_payload({encrypted, _Inner}, _Spec, Payload, no_context) ->
    kura_crypto:encrypt(Payload);
encrypt_payload({encrypted, _Inner}, _Spec, Payload, Context) ->
    kura_crypto:encrypt(Payload, Context);
encrypt_payload(_Type, #{mode := Mode} = Spec, Payload, Context) ->
    kura_crypto:encrypt(Payload, spec_context(Spec, Context), Mode).

spec_context(#{context := Context}, _Context) -> Context;
spec_context(_Spec, no_context) -> erlang:error({kura_crypto, no_context});
spec_context(_Spec, Context) -> Context.

%%----------------------------------------------------------------------
%% Load: driver result → Erlang term
%%----------------------------------------------------------------------

-doc "Convert a driver result value back to an Erlang term.".
-spec load(kura_type(), term()) -> {ok, term()} | {error, binary()}.
load(_Type, null) ->
    {ok, undefined};
load(id, V) when is_integer(V) ->
    {ok, V};
load(integer, V) when is_integer(V) ->
    {ok, V};
load(bigint, V) when is_integer(V) ->
    {ok, V};
load(smallint, V) when is_integer(V) ->
    {ok, V};
load(decimal, V) when is_number(V) ->
    {ok, V};
load(binary, V) when is_binary(V) ->
    {ok, V};
load(float, V) when is_number(V) ->
    {ok, erlang:float(V)};
load(string, V) when is_binary(V) ->
    {ok, V};
load(text, V) when is_binary(V) ->
    {ok, V};
load(boolean, V) when is_boolean(V) ->
    {ok, V};
load(date, {Y, M, D} = V) when is_integer(Y), is_integer(M), is_integer(D) ->
    {ok, V};
load(time, {H, M, S} = V) when is_integer(H), is_integer(M), is_integer(S) ->
    {ok, V};
load(utc_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
load(naive_datetime, {{_, _, _}, {_, _, _}} = V) ->
    {ok, V};
load(uuid, V) when is_binary(V), byte_size(V) =:= 16 ->
    {ok, format_uuid(binary:encode_hex(V, lowercase))};
load(uuid, V) when is_binary(V) ->
    {ok, V};
load(jsonb, V) when is_binary(V) ->
    json_decode(V);
load(jsonb, V) when is_map(V) ->
    {ok, V};
%% A jsonb column row whose root is a scalar/array decodes into the
%% matching Erlang term once kura_postgres has json-decoded the wire
%% bytes — keep them on the way back to the caller. (Null jsonb roots
%% land in the generic `load(_, null)` clause above and become
%% `undefined`, matching the column convention.)
load(jsonb, V) when is_list(V); is_number(V); is_boolean(V) ->
    {ok, V};
load({enum, Values}, V) when is_binary(V) ->
    try
        Atom = binary_to_existing_atom(V, utf8),
        case lists:member(Atom, Values) of
            true -> {ok, Atom};
            false -> {error, ~"unknown enum value"}
        end
    catch
        error:badarg -> {error, ~"unknown enum value"}
    end;
load({array, Inner}, V) when is_list(V) ->
    load_array(Inner, V, no_context, []);
load({embed, embeds_one, Mod}, V) when is_binary(V) ->
    case json_decode(V) of
        {ok, Map} when is_map(Map) -> {ok, load_embed_map(Mod, Map)};
        {ok, _} -> {error, ~"expected a JSON object"};
        Err -> Err
    end;
load({embed, embeds_one, Mod}, V) when is_map(V) ->
    {ok, load_embed_map(Mod, V)};
load({embed, embeds_many, Mod}, V) when is_binary(V) ->
    case json_decode(V) of
        {ok, List} when is_list(List) -> {ok, [load_embed_map(Mod, M) || M <- List]};
        {ok, _} -> {error, ~"expected a JSON array"};
        Err -> Err
    end;
load({embed, embeds_many, Mod}, V) when is_list(V) ->
    {ok, [load_embed_map(Mod, M) || M <- V]};
load(Type, V) when ?IS_ENCRYPTED_TAG(Type) ->
    load_encrypted(Type, V, no_context);
load(vector, V) when is_binary(V) ->
    {ok, parse_vector(V)};
load({vector, _}, V) when is_binary(V) ->
    {ok, parse_vector(V)};
load({custom, Mod}, V) ->
    Mod:load(V);
load({array, Inner} = Type, _V) ->
    case encryption(Inner) of
        none -> cannot_load(Type);
        _ -> erlang:error({kura_crypto, malformed_frame})
    end;
load(Type, _V) ->
    cannot_load(Type).

cannot_load(Type) ->
    {error, <<"cannot load ", (format_type(Type))/binary>>}.

-doc """
Like `load/2`, for a value read from the column `Context` (`{Table,
Column}`). An encrypted value may be a v1 or a v2 frame; a v2 frame only
decrypts for the column it was written for. `load/2` raises
`{kura_crypto, no_context}` on a v2 frame. Any other type loads as with
`load/2`.
""".
-spec load(kura_type(), term(), kura_crypto:context()) -> {ok, term()} | {error, binary()}.
load(Type, V, Context) ->
    load_value(Type, V, Context).

load_value(_Type, null, _Context) ->
    {ok, undefined};
load_value(Type, V, Context) when ?IS_ENCRYPTED_TAG(Type) ->
    load_encrypted(Type, V, Context);
load_value({array, Inner}, V, Context) when is_list(V) ->
    load_array(Inner, V, Context, []);
load_value(Type, V, _Context) ->
    load(Type, V).

%% Raise (never return {error}) so the fail-open load seam can't substitute
%% raw ciphertext for the field value.
load_encrypted(Type, Bytes, Context) when is_binary(Bytes) ->
    #{inner := Inner} = Spec = encryptable(encryption(Type)),
    {ok, Loaded} = load(Inner, decode_inner(Inner, decrypt_payload(Type, Spec, Bytes, Context))),
    {ok, Loaded};
load_encrypted(_Type, undefined, _Context) ->
    {ok, undefined};
load_encrypted(_Type, _V, _Context) ->
    erlang:error({kura_crypto, malformed_frame}).

decrypt_payload({encrypted, _Inner}, _Spec, Bytes, no_context) ->
    kura_crypto:decrypt(Bytes);
decrypt_payload({encrypted, _Inner}, _Spec, Bytes, Context) ->
    kura_crypto:decrypt(Bytes, Context);
decrypt_payload(_Type, #{mode := Mode, legacy := Legacy} = Spec, Bytes, Context) ->
    kura_crypto:decrypt(Bytes, spec_context(Spec, Context), #{mode => Mode, legacy => Legacy}).

%%----------------------------------------------------------------------
%% Encrypted-field inner codec
%%
%% Operates on the output of dump(Inner, _) / input to load(Inner, _).
%% Inner is statically known at both seams, so no self-describing format
%% (no term_to_binary) is used - a bounded codec keyed on Inner, so a
%% decrypted-but-corrupt payload can never forge an arbitrary term.
%%----------------------------------------------------------------------

-doc """
Recognise an encrypted field type. Every check for "is this field
encrypted" goes through here: the query guard, audit redaction, schema
validation and the cast, dump and load seams.

Returns `{ok, Spec}` for `{encrypted, Inner}` and for a valid
`{encrypted, Inner, Opts}` (see `validate_encryption/1`), `unsupported` for
any other tuple tagged `encrypted`, and `none` for every other type.
`{encrypted, Inner}` is `#{mode => randomized, legacy => accept}`; the
options form defaults to `randomized` and `legacy => reject`. An array of
an encrypted type is encrypted element by element and stored as
`BYTEA[]`; it returns the element's spec with `array => Depth`, or
`unsupported` if the element type is unsupported or deterministic. An
`unsupported` type is still encrypted: it is redacted and refused in
filters, a schema that declares one raises when it is first read, and
cast, dump and load raise on a tagged one rather than handle it as
plaintext.

A deterministic field (`mode => deterministic`) encrypts equal values of
the column to equal ciphertexts, so `kura_query:where/2` can look it up by
equality and a unique index on it works. In exchange it reveals which rows
hold the same value (frequency analysis), and its ciphertext, which
`where/2` binds as a query parameter and so appears in telemetry and logged
parameters, is a stable pseudonym of the value. Both modes reveal a value's
length and whether it is `NULL`. The value is still personal data under the
GDPR: pseudonymised, not anonymised. Use it only where an equality lookup or
uniqueness is needed. A column holds at most 2^32 distinct values per key.
A unique index holds within one key generation: the same value written
under two deterministic keys is two different ciphertexts. The binding
context defaults to the table and column name, so renaming either without
pinning `context` makes stored values undecryptable.
""".
-spec encryption(kura_type()) -> encryption().
encryption({encrypted, Inner}) ->
    {ok, #{inner => Inner, mode => randomized, legacy => accept, normalize => none}};
encryption({encrypted, Inner, Opts}) ->
    case encryption_options(Inner, Opts) of
        {ok, Spec} -> {ok, Spec};
        {error, _Reason} -> unsupported
    end;
encryption({array, Inner}) ->
    array_encryption(encryption(Inner));
encryption(Type) ->
    case ?IS_ENCRYPTED_TAG(Type) of
        true -> unsupported;
        false -> none
    end.

array_encryption({ok, #{mode := deterministic}}) -> unsupported;
array_encryption({ok, Spec}) -> {ok, Spec#{array => maps:get(array, Spec, 0) + 1}};
array_encryption(Encryption) -> Encryption.

-doc """
Check an encrypted field type, as `kura_schema` does when it first reads a
schema. Returns `ok` for a type that is not encrypted or is a valid
encrypted type, and otherwise `{error, Reason}`:

- `unsupported_encrypted_type` - an `Inner` that cannot be encrypted (see
  `encryptable_inner/1`), or a tuple tagged `encrypted` of another shape;
- `invalid_encryption_options` - `Opts` is not a map;
- `{unknown_encryption_option, Key}` - a key other than `mode`,
  `normalize`, `context` and `legacy`;
- `{invalid_encryption_option, Key}` - a value that key does not take, or
  a `normalize` other than `none` on a field that is not a deterministic
  `string` or `text`;
- `unsupported_deterministic_type` - a deterministic `Inner` outside
  `deterministic_inner/1`, or an array of deterministic values.
""".
-spec validate_encryption(kura_type()) -> ok | {error, term()}.
validate_encryption({array, Inner}) ->
    case {validate_encryption(Inner), encryption(Inner)} of
        {ok, {ok, #{mode := deterministic}}} -> {error, unsupported_deterministic_type};
        {Result, _} -> Result
    end;
validate_encryption({encrypted, Inner}) ->
    case encryptable_inner(Inner) of
        true -> ok;
        false -> {error, unsupported_encrypted_type}
    end;
validate_encryption({encrypted, Inner, Opts}) ->
    case encryption_options(Inner, Opts) of
        {ok, _Spec} -> ok;
        {error, Reason} -> {error, Reason}
    end;
validate_encryption(Type) ->
    case ?IS_ENCRYPTED_TAG(Type) of
        true -> {error, unsupported_encrypted_type};
        false -> ok
    end.

encryption_options(Inner, Opts) when is_map(Opts) ->
    case maps:keys(Opts) -- ?OPTION_KEYS of
        [] -> options_spec(Inner, Opts);
        [Key | _] -> {error, {unknown_encryption_option, Key}}
    end;
encryption_options(_Inner, _Opts) ->
    {error, invalid_encryption_options}.

options_spec(Inner, Opts) ->
    Mode = maps:get(mode, Opts, randomized),
    Normalize = maps:get(normalize, Opts, none),
    Legacy = maps:get(legacy, Opts, reject),
    Checks = [
        {lists:member(Mode, [randomized, deterministic]), {invalid_encryption_option, mode}},
        {lists:member(Legacy, [accept, reject]), {invalid_encryption_option, legacy}},
        {
            lists:member(Normalize, [none, casefold, nfc, nfc_casefold]),
            {invalid_encryption_option, normalize}
        },
        {valid_context_option(Opts), {invalid_encryption_option, context}},
        {encryptable_inner(Inner), unsupported_encrypted_type},
        {Mode =:= randomized orelse deterministic_inner(Inner), unsupported_deterministic_type},
        {
            Normalize =:= none orelse (Mode =:= deterministic andalso is_text(Inner)),
            {invalid_encryption_option, normalize}
        }
    ],
    case [Reason || {false, Reason} <- Checks] of
        [] ->
            Spec = #{inner => Inner, mode => Mode, legacy => Legacy, normalize => Normalize},
            {ok, maps:merge(Spec, maps:with([context], Opts))};
        [Reason | _] ->
            {error, Reason}
    end.

valid_context_option(#{context := {Table, Column}}) ->
    valid_context_name(Table) andalso valid_context_name(Column);
valid_context_option(#{context := _}) ->
    false;
valid_context_option(#{}) ->
    true.

valid_context_name(Name) ->
    is_binary(Name) andalso byte_size(Name) >= 1 andalso byte_size(Name) =< 65535.

is_text(string) -> true;
is_text(text) -> true;
is_text(_) -> false.

array_pg_type(unsupported, _Inner) -> erlang:error({kura_crypto, unsupported_encrypted_type});
array_pg_type(_, Inner) -> <<(to_pg_type(Inner))/binary, "[]">>.

encrypted_pg_type({ok, #{}}) -> ~"BYTEA";
encrypted_pg_type(_) -> erlang:error({kura_crypto, unsupported_encrypted_type}).

cast_encrypted({ok, #{inner := Inner} = Spec}, V) ->
    case encryptable_inner(Inner) of
        true ->
            canonical_cast(Spec, cast(Inner, V));
        false ->
            {error, <<"cannot use ", (format_type(Inner))/binary, " as an encrypted inner type">>}
    end;
cast_encrypted(_, _V) ->
    erlang:error({kura_crypto, unsupported_encrypted_type}).

encryptable({ok, #{inner := Inner} = Spec}) ->
    encryptable_inner(Inner) orelse erlang:error({kura_crypto, {not_encryptable, Inner}}),
    Spec;
encryptable(_) ->
    erlang:error({kura_crypto, unsupported_encrypted_type}).

-doc "Return `true` if `Inner` may be used as an `{encrypted, Inner}` inner type.".
-spec encryptable_inner(kura_type()) -> boolean().
encryptable_inner(string) -> true;
encryptable_inner(text) -> true;
encryptable_inner(binary) -> true;
encryptable_inner(uuid) -> true;
encryptable_inner(jsonb) -> true;
encryptable_inner(integer) -> true;
encryptable_inner(bigint) -> true;
encryptable_inner(smallint) -> true;
encryptable_inner(boolean) -> true;
encryptable_inner(_) -> false.

-doc """
Return `true` if `Inner` may be used as the inner type of a deterministic
field: `string`, `text`, `binary`, `uuid`, `integer`, `bigint` or
`smallint`. `boolean` has two values, so its ciphertexts reveal it, and a
`jsonb` document has many encodings of one value.
""".
-spec deterministic_inner(kura_type()) -> boolean().
deterministic_inner(string) -> true;
deterministic_inner(text) -> true;
deterministic_inner(binary) -> true;
deterministic_inner(uuid) -> true;
deterministic_inner(integer) -> true;
deterministic_inner(bigint) -> true;
deterministic_inner(smallint) -> true;
deterministic_inner(_) -> false.

%% The plaintext a deterministic field encrypts Value to, as a write of it
%% would. Never raises, so Value stays out of crash reports.
-doc false.
-spec lookup_payload(encryption_spec(), term()) -> {ok, binary()} | error.
lookup_payload(#{inner := Inner} = Spec, Value) ->
    try cast_encrypted({ok, Spec}, Value) of
        {ok, Cast} when Cast =/= undefined -> deterministic_payload(Inner, Spec, Cast);
        _ -> error
    catch
        _:_ -> error
    end.

deterministic_payload(Inner, Spec, Cast) ->
    case dump(Inner, Cast) of
        {ok, Wire} -> {ok, encode_inner(Inner, canonical_wire(Spec, Wire))};
        {error, _} -> error
    end.

%% A deterministic value has one encoding, so equal values encrypt equally:
%% normalised in cast so the record shows what is stored, and again in dump
%% for a write that skips cast.
canonical_cast(#{mode := deterministic} = Spec, {ok, Value}) when Value =/= undefined ->
    canonical_value(Spec, Value);
canonical_cast(_Spec, Cast) ->
    Cast.

canonical_wire(#{mode := deterministic} = Spec, Wire) ->
    case canonical_value(Spec, Wire) of
        {ok, Canonical} -> Canonical;
        {error, _} -> erlang:error({kura_crypto, malformed_plaintext})
    end;
canonical_wire(_Spec, Wire) ->
    Wire.

canonical_value(#{inner := uuid}, Value) ->
    case canonical_uuid(Value) of
        {ok, Uuid} -> {ok, Uuid};
        error -> {error, ~"is not a valid UUID"}
    end;
canonical_value(#{inner := Inner, normalize := Normalize}, Value) when
    Normalize =/= none, (Inner =:= string orelse Inner =:= text)
->
    case normalize(Normalize, Value) of
        {ok, Normalized} -> {ok, Normalized};
        error -> {error, ~"is not valid UTF-8"}
    end;
canonical_value(_Spec, Value) ->
    {ok, Value}.

canonical_uuid(<<Raw:16/binary>>) ->
    {ok, format_uuid(binary:encode_hex(Raw, lowercase))};
canonical_uuid(<<Hex:32/binary>>) ->
    lowercase_uuid(Hex);
canonical_uuid(<<A:8/binary, $-, B:4/binary, $-, C:4/binary, $-, D:4/binary, $-, E:12/binary>>) ->
    lowercase_uuid(<<A/binary, B/binary, C/binary, D/binary, E/binary>>);
canonical_uuid(_Value) ->
    error.

lowercase_uuid(Hex) ->
    case is_hex(Hex) of
        true -> {ok, format_uuid(string:lowercase(Hex))};
        false -> error
    end.

is_hex(<<>>) -> true;
is_hex(<<C, Rest/binary>>) when C >= $0, C =< $9 -> is_hex(Rest);
is_hex(<<C, Rest/binary>>) when C >= $a, C =< $f -> is_hex(Rest);
is_hex(<<C, Rest/binary>>) when C >= $A, C =< $F -> is_hex(Rest);
is_hex(_) -> false.

%% Checked to be UTF-8 first: string:casefold/1 raises on anything else,
%% with the value in its reason.
normalize(Normalize, Value) when is_binary(Value) ->
    case unicode:characters_to_binary(Value) of
        Value -> {ok, normalized(Normalize, Value)};
        _ -> error
    end;
normalize(_Normalize, _Value) ->
    error.

normalized(nfc, Value) -> nfc(Value);
normalized(casefold, Value) -> casefold(Value);
normalized(nfc_casefold, Value) -> nfc(casefold(Value)).

nfc(Value) ->
    case unicode:characters_to_nfc_binary(Value) of
        Nfc when is_binary(Nfc) -> Nfc
    end.

casefold(Value) ->
    case string:casefold(Value) of
        Folded when is_binary(Folded) -> Folded
    end.

%% dump(Inner, _) already produced a binary for the binary-y types (jsonb
%% included), so those are identity here.
encode_inner(I, Wire) when I =:= string; I =:= text; I =:= binary; I =:= uuid; I =:= jsonb ->
    Wire;
encode_inner(I, Wire) when I =:= integer; I =:= bigint; I =:= smallint ->
    integer_to_binary(Wire);
encode_inner(boolean, true) ->
    <<1>>;
encode_inner(boolean, false) ->
    <<0>>;
encode_inner(_, _) ->
    erlang:error({kura_crypto, malformed_plaintext}).

decode_inner(I, Bin) when I =:= string; I =:= text; I =:= binary; I =:= uuid; I =:= jsonb ->
    Bin;
decode_inner(I, Bin) when I =:= integer; I =:= bigint; I =:= smallint ->
    try
        binary_to_integer(Bin)
    catch
        error:badarg -> erlang:error({kura_crypto, malformed_plaintext})
    end;
decode_inner(boolean, <<1>>) ->
    true;
decode_inner(boolean, <<0>>) ->
    false;
decode_inner(_, _) ->
    erlang:error({kura_crypto, malformed_plaintext}).

%%----------------------------------------------------------------------
%% pgvector codec (text form: "[1,2,3]")
%%----------------------------------------------------------------------

cast_vector(V, Dim) ->
    case lists:all(fun erlang:is_number/1, V) of
        false ->
            {error, ~"is not a vector (a list of numbers)"};
        true when Dim =:= undefined ->
            {ok, [float(X) || X <- V]};
        true ->
            case length(V) =:= Dim of
                true ->
                    {ok, [float(X) || X <- V]};
                false ->
                    {error,
                        <<"expected a ", (integer_to_binary(Dim))/binary, "-dimensional vector">>}
            end
    end.

vector_to_text(V) ->
    Inner = lists:join(~",", [vector_elem_to_binary(X) || X <- V]),
    iolist_to_binary([~"[", Inner, ~"]"]).

vector_elem_to_binary(X) when is_integer(X) -> integer_to_binary(X);
vector_elem_to_binary(X) when is_float(X) -> float_to_binary(X, [short]).

parse_vector(<<"[", Rest/binary>> = Bin) when byte_size(Rest) >= 1 ->
    case Rest of
        ~"]" ->
            [];
        _ ->
            Last = binary:part(Rest, byte_size(Rest) - 1, 1),
            Inner = binary:part(Rest, 0, byte_size(Rest) - 1),
            case Last of
                ~"]" -> [parse_vector_elem(T) || T <- binary:split(Inner, ~",", [global])];
                _ -> erlang:error({kura_vector, {malformed, Bin}})
            end
    end;
parse_vector(Bin) ->
    erlang:error({kura_vector, {malformed, Bin}}).

parse_vector_elem(Bin) ->
    try
        binary_to_float(Bin)
    catch
        error:badarg ->
            try
                float(binary_to_integer(Bin))
            catch
                error:badarg -> parse_vector_scientific(Bin)
            end
    end.

parse_vector_scientific(Bin) ->
    case binary:match(Bin, [~"e", ~"E"]) of
        {Pos, 1} ->
            <<Mant:Pos/binary, Exp/binary>> = Bin,
            Normalized =
                case binary:match(Mant, ~".") of
                    nomatch -> <<Mant/binary, ".0", Exp/binary>>;
                    _ -> Bin
                end,
            try
                binary_to_float(Normalized)
            catch
                error:badarg -> erlang:error({kura_vector, {malformed_element, Bin}})
            end;
        nomatch ->
            erlang:error({kura_vector, {malformed_element, Bin}})
    end.

%%----------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------

try_parse_integer(Bin) ->
    try
        {ok, binary_to_integer(Bin)}
    catch
        error:badarg -> {error, ~"is not a valid integer"}
    end.

try_parse_float(Bin) ->
    try
        {ok, binary_to_float(Bin)}
    catch
        error:badarg ->
            try
                {ok, erlang:float(binary_to_integer(Bin))}
            catch
                error:badarg -> {error, ~"is not a valid float"}
            end
    end.

parse_date(<<Y:4/binary, $-, M:2/binary, $-, D:2/binary>>) ->
    try
        {ok, {binary_to_integer(Y), binary_to_integer(M), binary_to_integer(D)}}
    catch
        error:badarg -> {error, ~"is not a valid date"}
    end;
parse_date(_) ->
    {error, ~"is not a valid date"}.

parse_datetime(Bin) ->
    case binary:split(Bin, [~"T", ~" "]) of
        [DatePart, TimePart] ->
            case parse_date(DatePart) of
                {ok, Date} ->
                    case parse_time(TimePart) of
                        {ok, Time} -> {ok, {Date, Time}};
                        Err -> Err
                    end;
                Err ->
                    Err
            end;
        _ ->
            {error, ~"is not a valid datetime"}
    end.

parse_time(Bin) ->
    Stripped = strip_tz(Bin),
    case binary:split(Stripped, ~":", [global]) of
        [H, M, S] ->
            try
                Sec =
                    case binary:split(S, ~".") of
                        [SecWhole, _Frac] -> binary_to_integer(SecWhole);
                        [SecWhole] -> binary_to_integer(SecWhole)
                    end,
                {ok, {binary_to_integer(H), binary_to_integer(M), Sec}}
            catch
                error:badarg -> {error, ~"is not a valid time"}
            end;
        [H, M] ->
            try
                {ok, {binary_to_integer(H), binary_to_integer(M), 0}}
            catch
                error:badarg -> {error, ~"is not a valid time"}
            end;
        _ ->
            {error, ~"is not a valid time"}
    end.

strip_tz(Bin) ->
    case binary:last(Bin) of
        $Z ->
            binary:part(Bin, 0, byte_size(Bin) - 1);
        _ ->
            case binary:match(Bin, [~"+", ~"-"], [{scope, {3, byte_size(Bin) - 3}}]) of
                {Pos, _} -> binary:part(Bin, 0, Pos);
                nomatch -> Bin
            end
    end.

format_uuid(<<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>>) ->
    <<A/binary, $-, B/binary, $-, C/binary, $-, D/binary, $-, E/binary>>;
format_uuid(V) ->
    V.

json_encode(V) ->
    try
        {ok, iolist_to_binary(json:encode(V))}
    catch
        _:_ -> {error, ~"cannot encode as JSON"}
    end.

json_decode(Bin) ->
    try
        {ok, json:decode(Bin)}
    catch
        _:_ -> {error, ~"is not valid JSON"}
    end.

cast_array(_Inner, [], Acc) ->
    {ok, lists:reverse(Acc)};
cast_array(Inner, [H | T], Acc) ->
    case cast(Inner, H) of
        {ok, V} -> cast_array(Inner, T, [V | Acc]);
        {error, _} = Err -> Err
    end.

dump_array(_Inner, [], _Context, Acc) ->
    {ok, lists:reverse(Acc)};
dump_array(Inner, [H | T], Context, Acc) ->
    case dump_value(Inner, H, Context) of
        {ok, V} -> dump_array(Inner, T, Context, [V | Acc]);
        {error, _} = Err -> Err
    end.

load_array(_Inner, [], _Context, Acc) ->
    {ok, lists:reverse(Acc)};
load_array(Inner, [H | T], Context, Acc) ->
    case load_value(Inner, H, Context) of
        {ok, V} -> load_array(Inner, T, Context, [V | Acc]);
        {error, _} = Err -> Err
    end.

format_type({enum, _}) ->
    ~"enum";
format_type({array, Inner}) ->
    <<"array_", (format_type(Inner))/binary>>;
format_type({embed, _, Mod}) ->
    atom_to_binary(Mod, utf8);
format_type({custom, Mod}) ->
    atom_to_binary(Mod, utf8);
format_type({vector, _}) ->
    ~"vector";
format_type(Type) when ?IS_ENCRYPTED_TAG(Type) ->
    format_encrypted(encryption(Type));
format_type(T) when is_atom(T) ->
    atom_to_binary(T, utf8).

format_encrypted({ok, #{inner := Inner}}) -> <<"encrypted_", (format_type(Inner))/binary>>;
format_encrypted(_) -> ~"encrypted".

dump_embed_to_term(Mod, Map) ->
    Types = kura_schema:field_types(Mod),
    NonVirtual = kura_schema:non_virtual_fields(Mod),
    dump_embed_fields(NonVirtual, Map, Types).

dump_embed_fields([], _Map, _Types) ->
    #{};
dump_embed_fields([Field | Rest], Map, Types) ->
    Acc = dump_embed_fields(Rest, Map, Types),
    BinKey = atom_to_binary(Field, utf8),
    case Map of
        #{Field := Val} ->
            case Types of
                #{Field := {embed, embeds_one, ChildMod}} when is_map(Val) ->
                    Acc#{BinKey => dump_embed_to_term(ChildMod, Val)};
                #{Field := {embed, embeds_many, ChildMod}} when is_list(Val) ->
                    Acc#{BinKey => [dump_embed_to_term(ChildMod, I) || I <- Val]};
                #{Field := Type} ->
                    case dump(Type, Val) of
                        {ok, Dumped} -> Acc#{BinKey => Dumped};
                        {error, _} -> Acc#{BinKey => Val}
                    end;
                #{} ->
                    Acc#{BinKey => Val}
            end;
        #{} ->
            Acc
    end.

load_embed_map(Mod, Map) ->
    Types = kura_schema:field_types(Mod),
    load_embed_entries(maps:to_list(Map), Types).

load_embed_entries([], _Types) ->
    #{};
load_embed_entries([{K, V} | Rest], Types) ->
    Acc = load_embed_entries(Rest, Types),
    AtomKey = normalize_key(K),
    case Types of
        #{AtomKey := {embed, embeds_one, ChildMod}} when is_map(V) ->
            Acc#{AtomKey => load_embed_map(ChildMod, V)};
        #{AtomKey := {embed, embeds_many, ChildMod}} when is_list(V) ->
            Acc#{AtomKey => [load_embed_map(ChildMod, I) || I <- V]};
        #{AtomKey := Type} ->
            case load(Type, V) of
                {ok, Loaded} -> Acc#{AtomKey => Loaded};
                {error, _} -> Acc#{AtomKey => V}
            end;
        #{} ->
            Acc#{AtomKey => V}
    end.

normalize_key(K) when is_binary(K) ->
    try
        binary_to_existing_atom(K, utf8)
    catch
        error:badarg -> K
    end;
normalize_key(K) when is_atom(K) ->
    K;
normalize_key(K) ->
    K.
