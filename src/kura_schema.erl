-module(kura_schema).
-moduledoc """
Behaviour for defining database-backed schemas.

Implement the `table/0` and `fields/0` callbacks to define a schema module.
Mark one field with `primary_key = true`. Optionally implement `timestamps/0`
and `associations/0`.

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
""".

-include("kura.hrl").

-export([
    field_names/1,
    field_types/1,
    column_map/1,
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
    has_after_hook/2
]).

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

-doc "Return map of field name to kura type for a schema module.".
-spec field_types(module()) -> #{atom() => kura_types:kura_type()}.
field_types(Mod) ->
    cache({kura_schema, field_types, Mod}, fun() ->
        Base = field_types_map(Mod:fields(), #{}),
        embed_types_map(embeds(Mod), Base)
    end).

-doc "Return map of field name to column name (binary), excluding virtual fields.".
-spec column_map(module()) -> #{atom() => binary()}.
column_map(Mod) ->
    cache({kura_schema, column_map, Mod}, fun() ->
        FieldMap = field_column_map(Mod:fields(), #{}),
        embed_column_map(embeds(Mod), FieldMap)
    end).

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
        FieldMap = maps:from_list([{F#kura_field.name, F} || F <- Mod:fields()]),
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
    Col =
        case F#kura_field.column of
            undefined -> atom_to_binary(F#kura_field.name, utf8);
            C -> C
        end,
    field_column_map(Rest, Acc#{F#kura_field.name => Col}).

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

%%----------------------------------------------------------------------
%% Internal: persistent_term cache
%%----------------------------------------------------------------------

cache(Key, Fun) ->
    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            Value = Fun(),
            persistent_term:put(Key, Value),
            Value
    end.
