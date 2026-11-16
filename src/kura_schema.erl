-module(kura_schema).
-moduledoc """
Behaviour for defining database-backed schemas.

Implement the `table/0` and `fields/0` callbacks to define a schema module.
Mark one field with `primary_key = true`. Optionally implement `timestamps/0`
and `associations/0`.

## Timestamps

A schema with fields named `inserted_at` and `updated_at` has them set to
the current UTC time: `inserted_at` on insert, `updated_at` on insert and
on update. A value the write sets itself is kept.

Implement `timestamps/0` to stamp fields of other names, or none. It lists
the insert field, then the update field, each with its type:

```erlang
fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = created_at, type = utc_datetime},
     #kura_field{name = modified_at, type = utc_datetime}].

timestamps() -> [{created_at, utc_datetime}, {modified_at, utc_datetime}].
```

`[{created_at, utc_datetime}]` stamps on insert only. The fields are still
declared in `fields/0`.

kura 2.30 ignored `timestamps/0` and stamped the fields named `inserted_at`
and `updated_at`. Until kura 3.0 those two are therefore stamped whatever
`timestamps/0` says, and where that differs from the declaration kura logs
a warning, once for each schema each time the schema is loaded:

- A declaration that leaves out an `inserted_at` or `updated_at` field of
  the schema stamps the listed fields and that field. kura 3.0 stamps only
  the listed fields.
- A declaration kura cannot use (not such a list, or naming a field the
  schema does not have) stamps the fields named `inserted_at` and
  `updated_at`. kura 3.0 raises on it.
- `[]` is for a table whose timestamps the database maintains. It stamps
  nothing in kura 3.0, and until then a schema that has `inserted_at` or
  `updated_at` keeps them stamped.

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

table() -> <<"users">>.
fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = name, type = string},
     #kura_field{name = email, type = string}].
```

For a composite primary key, implement the optional `key/0` callback
returning the ordered key columns; `primary_key = true` markers are then
unnecessary. When `key/0` is absent the key is derived from the
`primary_key = true` fields, so existing single-key schemas are unchanged.

```erlang
-module(membership).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

table() -> <<"memberships">>.
key() -> [org_id, user_id].
fields() ->
    [#kura_field{name = org_id, type = uuid, nullable = false},
     #kura_field{name = user_id, type = uuid, nullable = false},
     #kura_field{name = role, type = string}].
```

## Column names

A field is stored in the column of its own name unless `column` says
otherwise:

```erlang
#kura_field{name = email, type = string, column = ~"email_address"}
```

Everywhere else the field is `email`: in changesets, in `where`,
`order_by`, `select` and the other query clauses, in `on_conflict`
targets, and in the maps the repo returns. Statements name the column,
and a row read back is keyed by the field. A `{fragment, SQL, Params}` is
SQL text, so it names the column itself. `column` on a virtual field is
ignored.
""".

-include("kura.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    field_names/1,
    field_types/1,
    is_schema/1,
    encrypted_fields/1,
    encryption_specs/1,
    field_specs/1,
    load_plan/1,
    column_map/1,
    renamed_columns/1,
    fields_by_column/1,
    timestamp_fields/1,
    redacted_fields/1,
    key/1,
    key_fields/1,
    primary_key/1,
    primary_key_field/1,
    non_virtual_fields/1,
    associations/1,
    association/2,
    assoc_fields/1,
    assoc_target/1,
    assoc_target_key/1,
    assoc_join_keys/1,
    assoc_on_delete/1,
    embeds/1,
    embed/2,
    constraints/1,
    indexes/1,
    generate_id/1,
    run_before_insert/2,
    run_after_insert/2,
    run_before_update/2,
    run_after_update/2,
    run_before_delete/2,
    run_after_delete/2,
    has_after_hook/2,
    forget/1
]).

-export_type([on_delete/0, field_encryption/0]).

-define(NOT_KEPT, '$kura_not_kept').
%% A kept load plan carries this number, and one kept with another, or with
%% none, was kept by another release of kura and is worked out again. Raise
%% it whenever what a plan holds changes.
-define(LOAD_PLAN_FORMAT, 1).

-doc """
Referential action for a foreign key when the referenced row is deleted.

These four are the actions every dialect kura ships renders identically,
so a schema that declares one is portable. `set_default` is deliberately
absent: PostgreSQL fails the delete when the column default is not itself
a valid parent key, SQLite only enforces it while `foreign_keys` is on,
and MySQL's InnoDB rejects it at DDL time. A dialect-specific action must
be introduced dialect-aware rather than added to this union.
""".
-type on_delete() :: cascade | restrict | set_null | no_action.
-doc "An encrypted field's `kura_types:encryption/1` spec with its `context` resolved.".
-type field_encryption() :: #{
    inner := kura_types:kura_type(),
    mode := kura_crypto:mode(),
    legacy := kura_crypto:legacy(),
    normalize := kura_types:normalize(),
    context := kura_crypto:context(),
    array => pos_integer()
}.

-callback table() -> binary().
-callback fields() -> [#kura_field{}].

-optional_callbacks([
    key/0,
    timestamps/0,
    associations/0,
    embeds/0,
    constraints/0,
    indexes/0,
    generate_id/0,
    tenant_scoped/0,
    redact/0,
    before_insert/1,
    after_insert/1,
    before_update/1,
    after_update/1,
    before_delete/1,
    after_delete/1
]).

-callback key() -> [atom()].
-callback timestamps() -> [{atom(), kura_types:kura_type()}].
-callback associations() -> [#kura_assoc{}].
-callback embeds() -> [#kura_embed{}].
-callback constraints() -> [kura_migration:table_constraint()].
-callback indexes() -> [kura_migration:index_def()].
-doc """
Return `false` for a schema every tenant shares, such as a table of plans
or countries. With `require_tenant` set, its rows may then be read and
written by a process that has no tenant, and under an attribute tenant a
query that reads only such schemas gets no tenant condition.

That is for a schema without the tenant's field. A schema that declares
the field of the attribute tenant holds each tenant's rows, so it is
matched on the field whatever this callback returns: read all tenants'
rows inside `kura_tenant:without_tenant/1`. See `m:kura_tenant`.
""".
-callback tenant_scoped() -> boolean().
-doc """
Return the fields whose values must stay out of logs and telemetry, such
as a password hash or a token. Encrypted fields need not be listed. In an
embedded schema it has the embed redacted whole in every schema that
embeds it. See `m:kura_redact`.
""".
-callback redact() -> [atom()].

-callback generate_id() -> term().

-callback before_insert(#kura_changeset{}) -> {ok, #kura_changeset{}} | {error, #kura_changeset{}}.
-callback after_insert(map()) -> {ok, map()} | {error, term()}.
-callback before_update(#kura_changeset{}) -> {ok, #kura_changeset{}} | {error, #kura_changeset{}}.
-callback after_update(map()) -> {ok, map()} | {error, term()}.
-callback before_delete(map()) -> ok | {error, term()}.
-callback after_delete(map()) -> ok | {error, term()}.

-doc "Return list of field names for a schema module.".
-spec field_names(module()) -> [atom()].
field_names(Mod) ->
    cache({kura_schema, field_names, Mod}, fun() ->
        [F#kura_field.name || F <- Mod:fields()]
    end).

-doc """
Return map of field name to kura type for a schema module.

The first call for a module also checks its encrypted fields, and raises
`{kura, Reason}` if one is used where it cannot work:

- `{unsupported_encrypted_type, Mod, Field}` - a type tagged `encrypted`
  that is not `{encrypted, Inner}` or `{encrypted, Inner, Opts}`, an
  `Inner` that cannot be encrypted (see `kura_types:encryptable_inner/1`),
  or an array of either;
- `{invalid_encryption_options, Mod, Field}` - `Opts` is not a map;
- `{unknown_encryption_option, Mod, Field, Key}` and
  `{invalid_encryption_option, Mod, Field, Key}` - an `Opts` key that does
  not exist, or a value it does not take (see
  `kura_types:validate_encryption/1`);
- `{unsupported_deterministic_type, Mod, Field}` - a deterministic field
  whose `Inner` is not in `kura_types:deterministic_inner/1`, or an array
  of deterministic values;
- `{encrypted_primary_key, Mod, Field}`;
- `{encrypted_association_key, Mod, Assoc, Field}` - a foreign key,
  referenced key or join-table key of an association;
- `{encrypted_embed_field, Mod, Embed, Field}` - an encrypted field in an
  embedded schema, at any depth;
- `{invalid_encrypted_column, Mod, Field}` - an encrypted field whose
  column name is not a binary of 1 to 65535 bytes;
- `{duplicate_encrypted_column, Mod, Column}` - two encrypted fields
  stored in the same column.

A key is matched by equality and an embed is stored as one JSON document,
so none of these can hold a ciphertext. A v2 frame is bound to its column
name, so two encrypted fields on one column would share a subkey.
""".
-spec field_types(module()) -> #{atom() => kura_types:kura_type()}.
field_types(Mod) ->
    cache({kura_schema, field_types, Mod}, fun() ->
        validate_encryption(Mod),
        Base = field_types_map(Mod:fields(), #{}),
        embed_types_map(embeds(Mod), Base)
    end).

-doc """
Return `true` if `Mod` is a loaded module exporting `table/0`, i.e. a schema
rather than a bare table name. Memoised, because the negative answer costs a
`code_server` round trip on every call.

A name that could not be loaded when it was first asked about is not
written off: once a module of that name is loaded, it is looked at again,
and what kura remembered of the name while it was not a schema is dropped
as `forget/1` drops it.
""".
-spec is_schema(atom()) -> boolean().
is_schema(Mod) when is_atom(Mod) ->
    Key = {kura_schema, is_schema, Mod},
    case persistent_term:get(Key, unknown) of
        Known when is_boolean(Known) ->
            Known;
        unloaded ->
            erlang:module_loaded(Mod) andalso loaded_since(Key, Mod);
        _Unknown ->
            case code:ensure_loaded(Mod) of
                {module, Mod} ->
                    exports_table(Key, Mod);
                _NotLoaded ->
                    persistent_term:put(Key, unloaded),
                    false
            end
    end.

%% Every answer cached for the name was worked out without the module.
-spec loaded_since(term(), module()) -> boolean().
loaded_since(Key, Mod) ->
    ok = forget(Mod),
    exports_table(Key, Mod).

-spec exports_table(term(), module()) -> boolean().
exports_table(Key, Mod) ->
    Schema = erlang:function_exported(Mod, table, 0),
    persistent_term:put(Key, Schema),
    Schema.

-doc """
Return the names of `Mod`'s encrypted fields (see `kura_types:encryption/1`),
or `[]` if it has none or is not a schema. Encryption uses a random nonce, so
a stored value never equals a freshly encrypted one - these fields cannot be
filtered on.
""".
-spec encrypted_fields(atom()) -> [atom()].
encrypted_fields(Mod) when is_atom(Mod) ->
    schema_cache({kura_schema, encrypted_fields, Mod}, [], fun() ->
        [N || N := T <- field_types(Mod), kura_types:encryption(T) =/= none]
    end).

-doc """
Return how each of `Mod`'s stored encrypted fields is encrypted: its
`kura_types:encryption/1` spec (`inner`, `mode`, `legacy`, `normalize` and,
for an array, `array`) with `context` set. `context` is the column a v2
ciphertext is bound to (see `kura_crypto`): the field's `context` option
if it has one, and otherwise `{Table, Column}`, where `Table` is the
schema's unprefixed `table()` and `Column` the field's column name (see
`column_map/1`). Renaming the table or the column of a field that does not
pin `context` makes its stored v2 values undecryptable. Virtual fields are
left out. `#{}` if `Mod` has no encrypted fields or is not a schema.
""".
-spec encryption_specs(atom()) -> #{atom() => field_encryption()}.
encryption_specs(Mod) when is_atom(Mod) ->
    schema_cache({kura_schema, encryption_specs, Mod}, #{}, fun() ->
        case encrypted_fields(Mod) of
            [] -> #{};
            Fields -> encryption_specs(Mod:table(), Fields, column_map(Mod), field_types(Mod))
        end
    end).

-doc false.
-spec field_specs(module()) ->
    {#{atom() => kura_types:kura_type()}, #{atom() => field_encryption()}}.
field_specs(Mod) ->
    cache({kura_schema, field_specs, Mod}, fun() -> {field_types(Mod), encryption_specs(Mod)} end).

-doc false.
-spec load_plan(module()) ->
    {#{atom() => kura_types:kura_type()}, #{atom() => field_encryption()}, #{binary() => atom()}}
    | undefined.
load_plan(Mod) ->
    Key = {kura_schema, load_plan, Mod},
    case persistent_term:get(Key, ?NOT_KEPT) of
        {?LOAD_PLAN_FORMAT, {Types, Specs, ByColumn} = Plan} when
            is_map(Types), is_map(Specs), is_map(ByColumn)
        ->
            Plan;
        _NotKeptOrUnreadable ->
            planned(Key, Mod)
    end.

%% A module that is not a schema now may be loaded as one later, so
%% `undefined` is worked out again each time.
planned(Key, Mod) ->
    %% is_schema/1 loads the module the first time the name is asked about,
    %% so a table named by a bare atom costs no code server call for each
    %% result set.
    _ = is_schema(Mod),
    case erlang:module_loaded(Mod) andalso erlang:function_exported(Mod, fields, 0) of
        true ->
            {Types, Specs} = field_specs(Mod),
            Plan = {Types, Specs, fields_by_column(Mod)},
            persistent_term:put(Key, {?LOAD_PLAN_FORMAT, Plan}),
            Plan;
        false ->
            undefined
    end.

encryption_specs(Table, Fields, Columns, Types) ->
    #{
        F => field_encryption(Spec, {Table, Column})
     || F <- Fields,
        #{F := Column} <- [Columns],
        {ok, Spec} <- [kura_types:encryption(maps:get(F, Types))]
    }.

field_encryption(#{context := _} = Spec, _Default) -> Spec;
field_encryption(Spec, Default) -> Spec#{context => Default}.

-doc "Return map of field name to column name (binary), excluding virtual fields.".
-spec column_map(module()) -> #{atom() => binary()}.
column_map(Mod) ->
    cache({kura_schema, column_map, Mod}, fun() ->
        FieldMap = field_column_map(Mod:fields(), #{}),
        embed_column_map(embeds(Mod), FieldMap)
    end).

-doc """
Return the fields of `Mod` stored under a column whose name differs from
the field's, as field name to column name. Empty for a schema that sets no
`column`, and for a module that is not a schema.
""".
-spec renamed_columns(module()) -> #{atom() => binary()}.
renamed_columns(Mod) ->
    schema_cache({kura_schema, renamed_columns, Mod}, #{}, fun() ->
        case erlang:function_exported(Mod, fields, 0) of
            true -> renamed(Mod:fields(), #{});
            false -> #{}
        end
    end).

renamed([], Acc) ->
    Acc;
renamed([#kura_field{name = Name, column = Column, virtual = false} | Rest], Acc) when
    is_binary(Column)
->
    case atom_to_binary(Name, utf8) of
        Column -> renamed(Rest, Acc);
        _ -> renamed(Rest, Acc#{Name => Column})
    end;
renamed([_ | Rest], Acc) ->
    renamed(Rest, Acc).

%% The columns of `Mod` that load into a field of another name, as column
%% name to field name: the inverse of renamed_columns/1.
-doc false.
-spec fields_by_column(module()) -> #{binary() => atom()}.
fields_by_column(Mod) ->
    schema_cache({kura_schema, fields_by_column, Mod}, #{}, fun() ->
        #{Column => Field || Field := Column <- renamed_columns(Mod)}
    end).

%% The fields the repo stamps with the current UTC time, as
%% {OnInsert, OnUpdate}. The moduledoc says which they are and what an
%% unusable timestamps/0 does.
-doc false.
-spec timestamp_fields(module()) -> {[atom()], [atom()]}.
timestamp_fields(Mod) ->
    cache({kura_schema, timestamp_fields, Mod}, fun() ->
        Types = field_types(Mod),
        Named = named_timestamps(Types),
        case erlang:function_exported(Mod, timestamps, 0) of
            true -> declared_timestamps(Mod, Mod:timestamps(), Types, Named);
            false -> Named
        end
    end).

named_timestamps(Types) ->
    OnUpdate = [updated_at || is_map_key(updated_at, Types)],
    {[inserted_at || is_map_key(inserted_at, Types)] ++ OnUpdate, OnUpdate}.

declared_timestamps(Mod, Declared, Types, {NamedOnInsert, NamedOnUpdate} = Named) ->
    case listed_timestamps(Declared, Types) of
        {ok, {OnInsert, OnUpdate}} ->
            ok = unlisted_timestamps(
                Mod,
                Declared,
                lists:usort((NamedOnInsert -- OnInsert) ++ (NamedOnUpdate -- OnUpdate))
            ),
            {lists:usort(OnInsert ++ NamedOnInsert), lists:usort(OnUpdate ++ NamedOnUpdate)};
        nothing when Named =:= {[], []} ->
            Named;
        nothing ->
            ok = timestamps_warning(#{
                msg =>
                    ~"kura: timestamps/0 lists no field, and the schema's inserted_at and updated_at are stamped as kura 2.30 stamped them",
                schema => Mod,
                timestamps => Declared,
                fix => ~"nothing to do to keep the stamps; kura 3.0 reads [] as stamp nothing"
            }),
            Named;
        unusable ->
            ok = timestamps_warning(#{
                msg =>
                    ~"kura: timestamps/0 is not a list of one or two of the schema's fields, so the fields named inserted_at and updated_at are stamped, as kura 2.30 stamped them",
                schema => Mod,
                timestamps => Declared,
                fix =>
                    ~"return [{InsertedField, Type}, {UpdatedField, Type}] or [{InsertedField, Type}], each a field of fields/0; kura 3.0 raises on anything else"
            }),
            Named
    end.

listed_timestamps([], _Types) ->
    nothing;
listed_timestamps([{Inserted, _Type}], Types) when is_map_key(Inserted, Types) ->
    {ok, {[Inserted], []}};
listed_timestamps([{Inserted, _InsertedType}, {Updated, _UpdatedType}], Types) when
    is_map_key(Inserted, Types), is_map_key(Updated, Types)
->
    {ok, {lists:usort([Inserted, Updated]), [Updated]}};
listed_timestamps(_Declared, _Types) ->
    unusable.

unlisted_timestamps(_Mod, _Declared, []) ->
    ok;
unlisted_timestamps(Mod, Declared, StillStamped) ->
    timestamps_warning(#{
        msg =>
            ~"kura: timestamps/0 leaves out a field named inserted_at or updated_at, which is still stamped as kura 2.30 stamped it",
        schema => Mod,
        timestamps => Declared,
        still_stamped => StillStamped,
        fix =>
            ~"nothing to do to keep the stamp; kura 3.0 stamps only the fields timestamps/0 lists"
    }).

%% The result is cached per schema, so this is logged once for each load of
%% the schema.
timestamps_warning(Report) ->
    ?LOG_WARNING(Report),
    ok.

-doc """
Return the fields of `Mod` whose values `m:kura_redact` replaces: its
encrypted fields, those its `redact/0` callback lists, and each embed
whose schema, or a schema embedded in that one at any depth, lists a
field in its own `redact/0`. An embed is one value, so it is replaced
whole. `[]` for a module that is not a schema.
""".
-spec redacted_fields(module()) -> [atom()].
redacted_fields(Mod) ->
    schema_cache({kura_schema, redacted_fields, Mod}, [], fun() ->
        lists:usort(encrypted_fields(Mod) ++ redacting_embeds(Mod) ++ listed_redacted(Mod))
    end).

-spec listed_redacted(module()) -> [atom()].
listed_redacted(Mod) ->
    case erlang:function_exported(Mod, redact, 0) of
        true -> Mod:redact();
        false -> []
    end.

-spec redacting_embeds(module()) -> [atom()].
redacting_embeds(Mod) ->
    [
        Name
     || #kura_embed{name = Name, schema = Embedded} <- embeds(Mod),
        lists:any(fun lists_a_redacted_field/1, embedded_in([Embedded], []))
    ].

-spec lists_a_redacted_field(module()) -> boolean().
lists_a_redacted_field(Mod) ->
    listed_redacted(Mod) =/= [].

%% The schemas and every schema embedded in them, each once: a schema may
%% embed itself.
-spec embedded_in([module()], [module()]) -> [module()].
embedded_in([], Seen) ->
    Seen;
embedded_in([Mod | Rest], Seen) ->
    case lists:member(Mod, Seen) of
        true ->
            embedded_in(Rest, Seen);
        false ->
            embedded_in([E#kura_embed.schema || E <- embeds(Mod)] ++ Rest, [Mod | Seen])
    end.

-doc "Return list of non-virtual field names.".
-spec non_virtual_fields(module()) -> [atom()].
non_virtual_fields(Mod) ->
    cache({kura_schema, non_virtual_fields, Mod}, fun() ->
        Fields = [F#kura_field.name || F <- Mod:fields(), F#kura_field.virtual =:= false],
        EmbedNames = [E#kura_embed.name || E <- embeds(Mod)],
        Fields ++ EmbedNames
    end).

-doc """
Return the ordered primary-key column names for a schema module.

A schema declares its key with the optional `key/0` callback (an ordered
list, arity 1 or more for composite keys). When `key/0` is not
implemented, the key is derived from the fields marked
`primary_key = true`, so existing single-key schemas need no change.
""".
-spec key(module()) -> [atom()].
key(Mod) ->
    cache({kura_schema, key, Mod}, fun() ->
        case key_names(Mod) of
            [_ | _] = Key -> Key;
            [] -> error({no_primary_key, Mod})
        end
    end).

%% Ordered key column names, possibly empty (no primary key). key/0
%% wins; otherwise the primary_key = true fields (the compat shim).
key_names(Mod) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, key, 0) of
        true -> Mod:key();
        false -> [F#kura_field.name || F <- Mod:fields(), F#kura_field.primary_key =:= true]
    end.

-doc "Return the ordered primary-key field records for a schema module.".
-spec key_fields(module()) -> [#kura_field{}].
key_fields(Mod) ->
    cache({kura_schema, key_fields, Mod}, fun() ->
        FieldMap = #{F#kura_field.name => F || F <- Mod:fields()},
        [
            case FieldMap of
                #{N := F} -> F;
                #{} -> error({key_field_not_found, Mod, N})
            end
         || N <- key(Mod)
        ]
    end).

-doc """
Return the single primary-key field name for a schema module.

Convenience over `key/1` for the common single-key case; raises
`{composite_primary_key, Mod, Keys}` on a composite key.
""".
-spec primary_key(module()) -> atom().
primary_key(Mod) ->
    case key(Mod) of
        [PK] -> PK;
        Keys -> error({composite_primary_key, Mod, Keys})
    end.

-doc """
Return the single primary-key field record, or `undefined` if the
schema has no primary key. Raises on a composite key.
""".
-spec primary_key_field(module()) -> #kura_field{} | undefined.
primary_key_field(Mod) ->
    case key_names(Mod) of
        [] ->
            undefined;
        [PK] ->
            [Field] = [F || F <- Mod:fields(), F#kura_field.name =:= PK],
            Field;
        Keys ->
            error({composite_primary_key, Mod, Keys})
    end.

-doc "Return all associations defined on a schema module.".
-spec associations(module()) -> [#kura_assoc{}].
associations(Mod) ->
    cache({kura_schema, associations, Mod}, fun() ->
        _ = code:ensure_loaded(Mod),
        case erlang:function_exported(Mod, associations, 0) of
            true -> Mod:associations();
            false -> []
        end
    end).

-doc "Look up a single association by name.".
-spec association(module(), atom()) -> {ok, #kura_assoc{}} | {error, not_found}.
association(Mod, Name) ->
    case [A || A <- associations(Mod), A#kura_assoc.name =:= Name] of
        [Assoc] -> {ok, Assoc};
        [] -> {error, not_found}
    end.

-doc """
Return the foreign-key column(s) of an association as an ordered list.

A `#kura_ref{}`'s `fields` win; a legacy single `foreign_key` lowers to
a one-element list, so single-column associations are the arity-1 case
of the same list. Returns `[]` if neither is set.
""".
-spec assoc_fields(#kura_assoc{}) -> [atom()].
assoc_fields(#kura_assoc{ref = #kura_ref{fields = Fields}}) when Fields =/= undefined ->
    Fields;
assoc_fields(#kura_assoc{foreign_key = FK}) when FK =/= undefined ->
    [FK];
assoc_fields(#kura_assoc{}) ->
    [].

-doc """
Return the target schema module of an association.

A `#kura_ref{}`'s `target` wins; otherwise the legacy `schema` field.
""".
-spec assoc_target(#kura_assoc{}) -> module() | undefined.
assoc_target(#kura_assoc{ref = #kura_ref{target = T}}) when T =/= undefined ->
    T;
assoc_target(#kura_assoc{schema = S}) ->
    S.

-doc """
Return the explicit referenced key column(s) on the target side, or
`undefined` to mean "default to the target schema's key". Only a
`#kura_ref{}` can carry this; legacy associations always return
`undefined`.
""".
-spec assoc_target_key(#kura_assoc{}) -> [atom()] | undefined.
assoc_target_key(#kura_assoc{ref = #kura_ref{target_key = Cols}}) ->
    Cols;
assoc_target_key(#kura_assoc{}) ->
    undefined.

-doc """
Return a many_to_many association's join-table columns as
`{OwnerCols, RelatedCols}`, each an ordered list. A single-column
`join_keys = {owner, related}` is the one-element-list case.
""".
-spec assoc_join_keys(#kura_assoc{}) -> {[atom()], [atom()]}.
assoc_join_keys(#kura_assoc{join_keys = {Owner, Related}}) ->
    {as_col_list(Owner), as_col_list(Related)}.

as_col_list(C) when is_atom(C) -> [C];
as_col_list(C) when is_list(C) -> C.

-doc """
Return the referential action a `belongs_to` asks for when its parent row
is deleted, ready to be placed on the `#kura_column{}` that owns the key.

`belongs_to` is the only association that owns a foreign-key column, so it
is the only one that can carry the constraint. Declaring `on_delete` on any
other association type raises `{on_delete_not_owned, Name, Type}` rather
than being ignored - the clause would never reach the DDL, and a cascade
rule that silently does not exist is worse than a build failure.

An undeclared `on_delete` on a `belongs_to` lowers to `no_action`, which is
what generated foreign keys have always emitted. Any other type returns
`undefined`: it owns no column, so it contributes no clause.
""".
-spec assoc_on_delete(#kura_assoc{}) -> on_delete() | undefined.
assoc_on_delete(#kura_assoc{type = belongs_to, on_delete = undefined}) ->
    no_action;
assoc_on_delete(#kura_assoc{type = belongs_to, on_delete = Action, name = Name}) ->
    case Action of
        cascade -> Action;
        restrict -> Action;
        set_null -> Action;
        no_action -> Action;
        _ -> error({invalid_on_delete, Name, Action})
    end;
assoc_on_delete(#kura_assoc{on_delete = undefined}) ->
    undefined;
assoc_on_delete(#kura_assoc{name = Name, type = Type}) ->
    error({on_delete_not_owned, Name, Type}).

-doc "Return all embeds defined on a schema module.".
-spec embeds(module()) -> [#kura_embed{}].
embeds(Mod) ->
    cache({kura_schema, embeds, Mod}, fun() ->
        _ = code:ensure_loaded(Mod),
        case erlang:function_exported(Mod, embeds, 0) of
            true -> Mod:embeds();
            false -> []
        end
    end).

-doc "Look up a single embed by name.".
-spec embed(module(), atom()) -> {ok, #kura_embed{}} | {error, not_found}.
embed(Mod, Name) ->
    case [E || E <- embeds(Mod), E#kura_embed.name =:= Name] of
        [Embed] -> {ok, Embed};
        [] -> {error, not_found}
    end.

-doc "Return all table-level constraints defined on a schema module.".
-spec constraints(module()) -> [kura_migration:table_constraint()].
constraints(Mod) ->
    cache({kura_schema, constraints, Mod}, fun() ->
        _ = code:ensure_loaded(Mod),
        case erlang:function_exported(Mod, constraints, 0) of
            true -> Mod:constraints();
            false -> []
        end
    end).

-doc "Return all indexes defined on a schema module.".
-spec indexes(module()) -> [kura_migration:index_def()].
indexes(Mod) ->
    cache({kura_schema, indexes, Mod}, fun() ->
        _ = code:ensure_loaded(Mod),
        case erlang:function_exported(Mod, indexes, 0) of
            true -> Mod:indexes();
            false -> []
        end
    end).

%%----------------------------------------------------------------------
%% Internal: encrypted field placement
%%
%% Reads fields/0 of the schemas involved directly rather than through
%% field_types/1, so two schemas that refer to each other do not recurse.
%%----------------------------------------------------------------------

validate_encryption(Mod) ->
    Fields = Mod:fields(),
    refuse_types(
        Mod,
        [
            {F, Reason}
         || #kura_field{name = F, type = T} <- Fields,
            {error, Reason} <- [kura_types:validate_encryption(T)]
        ]
    ),
    Encrypted = declared_encrypted(Mod),
    refuse(encrypted_primary_key, Mod, [F || F <- key_names(Mod), lists:member(F, Encrypted)]),
    validate_encrypted_columns(Mod, Fields, Encrypted),
    case
        [
            {Name, F}
         || #kura_assoc{name = Name} = A <- associations(Mod),
            F <- encrypted_assoc_keys(Encrypted, A)
        ]
    of
        [] -> ok;
        [{Name, F} | _] -> error({kura, {encrypted_association_key, Mod, Name, F}})
    end,
    validate_embeds(Mod, embeds(Mod), [Mod]).

validate_encrypted_columns(Mod, Fields, Encrypted) ->
    Columns = [
        {F, column_name(Field)}
     || #kura_field{name = F, virtual = false} = Field <- Fields, lists:member(F, Encrypted)
    ],
    refuse(invalid_encrypted_column, Mod, [F || {F, C} <- Columns, not valid_column_name(C)]),
    Names = [C || {_, C} <- Columns],
    refuse(duplicate_encrypted_column, Mod, Names -- lists:usort(Names)).

valid_column_name(C) when is_binary(C), byte_size(C) >= 1, byte_size(C) =< 65535 -> true;
valid_column_name(_) -> false.

refuse_types(_Mod, []) -> ok;
refuse_types(Mod, [{Field, {Reason, Key}} | _]) -> error({kura, {Reason, Mod, Field, Key}});
refuse_types(Mod, [{Field, Reason} | _]) -> error({kura, {Reason, Mod, Field}}).

refuse(_Reason, _Mod, []) -> ok;
refuse(Reason, Mod, [F | _]) -> error({kura, {Reason, Mod, F}}).

encrypted_assoc_keys(Encrypted, #kura_assoc{type = belongs_to} = A) ->
    Referenced =
        case assoc_target_key(A) of
            undefined -> [];
            Cols -> [C || C <- Cols, lists:member(C, declared_encrypted(assoc_target(A)))]
        end,
    [F || F <- assoc_fields(A), lists:member(F, Encrypted)] ++ Referenced;
encrypted_assoc_keys(Encrypted, #kura_assoc{type = Type} = A) when
    Type =:= has_many; Type =:= has_one
->
    TargetEncrypted = declared_encrypted(assoc_target(A)),
    OwnerKey =
        case assoc_target_key(A) of
            undefined -> [];
            Cols -> [C || C <- Cols, lists:member(C, Encrypted)]
        end,
    [F || F <- assoc_fields(A), lists:member(F, TargetEncrypted)] ++ OwnerKey;
encrypted_assoc_keys(
    _Encrypted,
    #kura_assoc{
        type = many_to_many, join_through = Through, join_keys = {_, _}
    } = A
) when is_atom(Through) ->
    {Owner, Related} = assoc_join_keys(A),
    ThroughEncrypted = declared_encrypted(Through),
    [F || F <- Owner ++ Related, lists:member(F, ThroughEncrypted)];
encrypted_assoc_keys(_Encrypted, #kura_assoc{}) ->
    [].

validate_embeds(_Mod, [], _Seen) ->
    ok;
validate_embeds(Mod, [#kura_embed{name = Name, schema = Embedded} | Rest], Seen) ->
    case declared_encrypted(Embedded) of
        [] -> ok;
        [F | _] -> error({kura, {encrypted_embed_field, Mod, Name, F}})
    end,
    case lists:member(Embedded, Seen) of
        true -> ok;
        false -> validate_embeds(Mod, embeds(Embedded), [Embedded | Seen])
    end,
    validate_embeds(Mod, Rest, Seen).

declared_encrypted(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            case erlang:function_exported(Mod, fields, 0) of
                true ->
                    [
                        F
                     || #kura_field{name = F, type = T} <- Mod:fields(),
                        kura_types:encryption(T) =/= none
                    ];
                false ->
                    []
            end;
        _ ->
            []
    end.

%%----------------------------------------------------------------------
%% Internal: recursive map builders
%%----------------------------------------------------------------------

field_types_map([], Acc) ->
    Acc;
field_types_map([F | Rest], Acc) ->
    field_types_map(Rest, Acc#{F#kura_field.name => F#kura_field.type}).

embed_types_map([], Acc) ->
    Acc;
embed_types_map([E | Rest], Acc) ->
    embed_types_map(Rest, Acc#{E#kura_embed.name => {embed, E#kura_embed.type, E#kura_embed.schema}}).

field_column_map([], Acc) ->
    Acc;
field_column_map([#kura_field{virtual = true} | Rest], Acc) ->
    field_column_map(Rest, Acc);
field_column_map([F | Rest], Acc) ->
    field_column_map(Rest, Acc#{F#kura_field.name => column_name(F)}).

column_name(#kura_field{name = Name, column = undefined}) -> atom_to_binary(Name, utf8);
column_name(#kura_field{column = Column}) -> Column.

embed_column_map([], Acc) ->
    Acc;
embed_column_map([E | Rest], Acc) ->
    embed_column_map(Rest, Acc#{E#kura_embed.name => atom_to_binary(E#kura_embed.name, utf8)}).

%%----------------------------------------------------------------------
%% ID generation
%%----------------------------------------------------------------------

-doc "Generate a primary key value if the schema defines `generate_id/0`.".
-spec generate_id(module()) -> {ok, term()} | undefined.
generate_id(Mod) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, generate_id, 0) of
        true -> {ok, Mod:generate_id()};
        false -> undefined
    end.

%%----------------------------------------------------------------------
%% Lifecycle hooks
%%----------------------------------------------------------------------

-doc "Run before_insert hook if defined. Returns `{ok, CS}` or `{error, CS}`.".
-spec run_before_insert(module(), #kura_changeset{}) ->
    {ok, #kura_changeset{}} | {error, #kura_changeset{}}.
run_before_insert(Mod, CS) ->
    run_hook(Mod, before_insert, CS).

-doc "Run after_insert hook if defined. Returns `{ok, Record}` or `{error, Reason}`.".
-spec run_after_insert(module(), map()) -> {ok, map()} | {error, term()}.
run_after_insert(Mod, Record) ->
    run_after_hook(Mod, after_insert, Record).

-doc "Run before_update hook if defined. Returns `{ok, CS}` or `{error, CS}`.".
-spec run_before_update(module(), #kura_changeset{}) ->
    {ok, #kura_changeset{}} | {error, #kura_changeset{}}.
run_before_update(Mod, CS) ->
    run_hook(Mod, before_update, CS).

-doc "Run after_update hook if defined. Returns `{ok, Record}` or `{error, Reason}`.".
-spec run_after_update(module(), map()) -> {ok, map()} | {error, term()}.
run_after_update(Mod, Record) ->
    run_after_hook(Mod, after_update, Record).

-doc "Run before_delete hook if defined. Returns `ok` or `{error, Reason}`.".
-spec run_before_delete(module(), map()) -> ok | {error, term()}.
run_before_delete(Mod, Record) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, before_delete, 1) of
        true -> Mod:before_delete(Record);
        false -> ok
    end.

-doc "Run after_delete hook if defined. Returns `ok` or `{error, Reason}`.".
-spec run_after_delete(module(), map()) -> ok | {error, term()}.
run_after_delete(Mod, Record) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, after_delete, 1) of
        true -> Mod:after_delete(Record);
        false -> ok
    end.

-doc "Check if a schema module has an after_* hook for the given action.".
-spec has_after_hook(module(), insert | update | delete) -> boolean().
has_after_hook(Mod, Action) ->
    Hook =
        case Action of
            insert -> after_insert;
            update -> after_update;
            delete -> after_delete
        end,
    _ = code:ensure_loaded(Mod),
    erlang:function_exported(Mod, Hook, 1).

run_hook(Mod, Hook, CS) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, Hook, 1) of
        true -> Mod:Hook(CS);
        false -> {ok, CS}
    end.

run_after_hook(Mod, Hook, Record) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, Hook, 1) of
        true -> Mod:Hook(Record);
        false -> {ok, Record}
    end.

-doc """
Drop every cached schema lookup for `Mod`, and every compiled query.

Lookups such as `key/1` and `associations/1` are memoised per module for
the life of the VM, which is correct while a module's code never changes.
A tool that loads, purges and reloads schema modules in one VM (a build
plugin, a test harness) calls this after replacing `Mod`, so the next
lookup reads the new code instead of the old module's answer. The
compiled-query cache is flushed too, since its SQL carries the old
module's table and column names.

Call it once the new code is loaded and before anything queries `Mod`
again: a lookup already computing from the old code when `forget/1`
runs can store its answer afterwards.

Each erased entry forces a scan of every process (a global GC), so this
is for tooling, not request paths.
""".
-spec forget(module()) -> ok.
forget(Mod) ->
    _ = [
        persistent_term:erase(Key)
     || {{kura_schema, _, M} = Key, _} <- persistent_term:get(), M =:= Mod
    ],
    kura_query_cache:flush().

%%----------------------------------------------------------------------
%% Internal: persistent_term cache
%%----------------------------------------------------------------------

%% The answer for a name that is not a schema is not kept: a module of
%% that name may be loaded later, and is_schema/1 is what notices. A kept
%% answer was therefore worked out from a schema, and is read without asking.
schema_cache({kura_schema, _Tag, Mod} = Key, NotASchema, Fun) ->
    case persistent_term:get(Key, ?NOT_KEPT) of
        ?NOT_KEPT ->
            case is_schema(Mod) of
                true -> cache(Key, Fun);
                false -> NotASchema
            end;
        Kept ->
            Kept
    end.

%% Keys are {kura_schema, Tag, Mod}; forget/1 relies on that shape.
cache(Key, Fun) ->
    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            Value = Fun(),
            persistent_term:put(Key, Value),
            Value
    end.
