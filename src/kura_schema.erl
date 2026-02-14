-module(kura_schema).
-moduledoc """
Behaviour for defining database-backed schemas.

Implement the `table/0`, `fields/0`, and `primary_key/0` callbacks to define
a schema module. Optionally implement `timestamps/0` and `associations/0`.

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

table() -> <<"users">>.
primary_key() -> id.
fields() ->
    [#kura_field{name = id, type = id},
     #kura_field{name = name, type = string},
     #kura_field{name = email, type = string}].
```
""".

-include("kura.hrl").

-export([
    field_names/1,
    field_types/1,
    column_map/1,
    primary_key_field/1,
    non_virtual_fields/1,
    associations/1,
    association/2,
    embeds/1,
    embed/2
]).

-callback table() -> binary().
-callback fields() -> [#kura_field{}].
-callback primary_key() -> atom().

-optional_callbacks([timestamps/0, associations/0, embeds/0]).

-callback timestamps() -> [{atom(), kura_types:kura_type()}].
-callback associations() -> [#kura_assoc{}].
-callback embeds() -> [#kura_embed{}].

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
        Base = maps:from_list([{F#kura_field.name, F#kura_field.type} || F <- Mod:fields()]),
        EmbedTypes = [
            {E#kura_embed.name, {embed, E#kura_embed.type, E#kura_embed.schema}}
         || E <- embeds(Mod)
        ],
        maps:merge(Base, maps:from_list(EmbedTypes))
    end).

-doc "Return map of field name to column name (binary), excluding virtual fields.".
-spec column_map(module()) -> #{atom() => binary()}.
column_map(Mod) ->
    cache({kura_schema, column_map, Mod}, fun() ->
        FieldMap = maps:from_list([
            {
                F#kura_field.name,
                case F#kura_field.column of
                    undefined -> atom_to_binary(F#kura_field.name, utf8);
                    Col -> Col
                end
            }
         || F <- Mod:fields(), F#kura_field.virtual =:= false
        ]),
        EmbedMap = maps:from_list([
            {E#kura_embed.name, atom_to_binary(E#kura_embed.name, utf8)}
         || E <- embeds(Mod)
        ]),
        maps:merge(FieldMap, EmbedMap)
    end).

-doc "Return list of non-virtual field names.".
-spec non_virtual_fields(module()) -> [atom()].
non_virtual_fields(Mod) ->
    cache({kura_schema, non_virtual_fields, Mod}, fun() ->
        Fields = [F#kura_field.name || F <- Mod:fields(), F#kura_field.virtual =:= false],
        EmbedNames = [E#kura_embed.name || E <- embeds(Mod)],
        Fields ++ EmbedNames
    end).

-doc "Return the primary key field record, or `undefined` if not found.".
-spec primary_key_field(module()) -> #kura_field{} | undefined.
primary_key_field(Mod) ->
    PK = Mod:primary_key(),
    case [F || F <- Mod:fields(), F#kura_field.name =:= PK] of
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
