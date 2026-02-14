-module(kura_schema).

-include("kura.hrl").

-export([
    field_names/1,
    field_types/1,
    column_map/1,
    primary_key_field/1,
    non_virtual_fields/1,
    associations/1,
    association/2
]).

-callback table() -> binary().
-callback fields() -> [#kura_field{}].
-callback primary_key() -> atom().

-optional_callbacks([timestamps/0, associations/0]).

-callback timestamps() -> [{atom(), kura_types:kura_type()}].
-callback associations() -> [#kura_assoc{}].

%% Return list of field names for a schema module.
-spec field_names(module()) -> [atom()].
field_names(Mod) ->
    cache({kura_schema, field_names, Mod}, fun() ->
        [F#kura_field.name || F <- Mod:fields()]
    end).

%% Return map of field name => kura type.
-spec field_types(module()) -> #{atom() => kura_types:kura_type()}.
field_types(Mod) ->
    cache({kura_schema, field_types, Mod}, fun() ->
        maps:from_list([{F#kura_field.name, F#kura_field.type} || F <- Mod:fields()])
    end).

%% Return map of field name => column name (binary), excluding virtual fields.
-spec column_map(module()) -> #{atom() => binary()}.
column_map(Mod) ->
    cache({kura_schema, column_map, Mod}, fun() ->
        maps:from_list([
            {
                F#kura_field.name,
                case F#kura_field.column of
                    undefined -> atom_to_binary(F#kura_field.name, utf8);
                    Col -> Col
                end
            }
         || F <- Mod:fields(), F#kura_field.virtual =:= false
        ])
    end).

%% Return list of non-virtual field names.
-spec non_virtual_fields(module()) -> [atom()].
non_virtual_fields(Mod) ->
    cache({kura_schema, non_virtual_fields, Mod}, fun() ->
        [F#kura_field.name || F <- Mod:fields(), F#kura_field.virtual =:= false]
    end).

%% Return the primary key field record.
-spec primary_key_field(module()) -> #kura_field{} | undefined.
primary_key_field(Mod) ->
    PK = Mod:primary_key(),
    case [F || F <- Mod:fields(), F#kura_field.name =:= PK] of
        [Field] -> Field;
        [] -> undefined
    end.

-spec associations(module()) -> [#kura_assoc{}].
associations(Mod) ->
    cache({kura_schema, associations, Mod}, fun() ->
        code:ensure_loaded(Mod),
        case erlang:function_exported(Mod, associations, 0) of
            true -> Mod:associations();
            false -> []
        end
    end).

-spec association(module(), atom()) -> {ok, #kura_assoc{}} | {error, not_found}.
association(Mod, Name) ->
    case [A || A <- associations(Mod), A#kura_assoc.name =:= Name] of
        [Assoc] -> {ok, Assoc};
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
