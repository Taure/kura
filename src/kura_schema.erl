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
""".

-include("kura.hrl").

-export([
    field_names/1,
    field_types/1,
    column_map/1,
    primary_key/1,
    primary_key_field/1,
    non_virtual_fields/1,
    associations/1,
    association/2,
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

-doc "Return the primary key field name for a schema module.".
-spec primary_key(module()) -> atom().
primary_key(Mod) ->
    cache({kura_schema, primary_key, Mod}, fun() ->
        case [F#kura_field.name || F <- Mod:fields(), F#kura_field.primary_key =:= true] of
            [PK] -> PK;
            [] -> error({no_primary_key, Mod})
        end
    end).

-doc "Return the primary key field record, or `undefined` if not found.".
-spec primary_key_field(module()) -> #kura_field{} | undefined.
primary_key_field(Mod) ->
    case [F || F <- Mod:fields(), F#kura_field.primary_key =:= true] of
        [Field] -> Field;
        [] -> undefined
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
