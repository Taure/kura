-module(kura_changeset_assoc).

-include("kura.hrl").

-export([
    cast_assoc/2,
    cast_assoc/3,
    put_assoc/3,
    put_assoc/4,
    cast_embed/2,
    cast_embed/3
]).

-type caster() :: fun((map(), map()) -> #kura_changeset{}).

%%----------------------------------------------------------------------
%% Association casting
%%----------------------------------------------------------------------

-spec cast_assoc(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_assoc(CS, AssocName) ->
    cast_assoc(CS, AssocName, #{}).

-spec cast_assoc(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
cast_assoc(CS = #kura_changeset{schema = SchemaMod, params = Params, data = Data}, AssocName, Opts) ->
    case kura_schema:association(SchemaMod, AssocName) of
        {ok, Assoc} ->
            WithFun = assoc_caster(Opts, SchemaMod, AssocName, Assoc),
            case Params of
                #{AssocName := NestedParams} ->
                    Existing = maps:get(AssocName, Data, undefined),
                    cast_assoc_params(CS, AssocName, Assoc, NestedParams, Existing, WithFun);
                #{} ->
                    CS
            end;
        {error, not_found} ->
            kura_changeset:add_error(CS, AssocName, ~"unknown association")
    end.

-spec put_assoc(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_assoc(CS, AssocName, Value) ->
    put_assoc(CS, AssocName, Value, #{}).

-spec put_assoc(#kura_changeset{}, atom(), term(), map()) -> #kura_changeset{}.
put_assoc(CS = #kura_changeset{schema = SchemaMod, assoc_changes = AC}, AssocName, Value, Opts) ->
    case kura_schema:association(SchemaMod, AssocName) of
        {ok, Assoc} ->
            Changesets = assoc_value(Assoc, Value, Opts, SchemaMod, AssocName),
            CS#kura_changeset{assoc_changes = AC#{AssocName => Changesets}};
        {error, not_found} ->
            kura_changeset:add_error(CS, AssocName, ~"unknown association")
    end.

%%----------------------------------------------------------------------
%% Embed casting
%%----------------------------------------------------------------------

-spec cast_embed(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_embed(CS, EmbedName) ->
    cast_embed(CS, EmbedName, #{}).

-spec cast_embed(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
cast_embed(CS = #kura_changeset{schema = SchemaMod, params = Params}, EmbedName, Opts) ->
    case kura_schema:embed(SchemaMod, EmbedName) of
        {ok, Embed} ->
            WithFun = caster(Opts, SchemaMod, EmbedName, Embed#kura_embed.schema, []),
            case Params of
                #{EmbedName := NestedParams} ->
                    cast_embed_params(CS, EmbedName, Embed, NestedParams, WithFun);
                #{} ->
                    CS
            end;
        {error, not_found} ->
            kura_changeset:add_error(CS, EmbedName, ~"unknown embed")
    end.

%%----------------------------------------------------------------------
%% Internal: embed helpers
%%----------------------------------------------------------------------

cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_one}, Params, WithFun) when
    is_map(Params)
->
    NormParams = kura_changeset:normalize_params(Params),
    ChildCS = WithFun(#{}, NormParams),
    case ChildCS#kura_changeset.valid of
        true ->
            Changes = CS#kura_changeset.changes,
            CS#kura_changeset{
                changes = Changes#{EmbedName => kura_changeset:apply_changes(ChildCS)}
            };
        false ->
            CS#kura_changeset{
                valid = false, errors = CS#kura_changeset.errors ++ ChildCS#kura_changeset.errors
            }
    end;
cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_one}, _Params, _WithFun) ->
    kura_changeset:add_error(CS, EmbedName, ~"expected a map");
cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_many}, ParamsList, WithFun) when
    is_list(ParamsList)
->
    Results = cast_embed_list(ParamsList, WithFun, []),
    case all_valid(Results) of
        true ->
            Applied = [kura_changeset:apply_changes(R) || R <- Results],
            Changes = CS#kura_changeset.changes,
            CS#kura_changeset{changes = Changes#{EmbedName => Applied}};
        false ->
            AllErrors = collect_errors(Results, CS#kura_changeset.errors),
            CS#kura_changeset{valid = false, errors = AllErrors}
    end;
cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_many}, _Params, _WithFun) ->
    kura_changeset:add_error(CS, EmbedName, ~"expected a list").

%%----------------------------------------------------------------------
%% Internal: association helpers
%%----------------------------------------------------------------------

%% The foreign key is set from the parent, so it is never cast from the
%% child's params. A many_to_many child holds no foreign key.
assoc_caster(Opts, ParentSchema, AssocName, Assoc = #kura_assoc{type = many_to_many}) ->
    ChildSchema = kura_schema:assoc_target(Assoc),
    caster(Opts, ParentSchema, AssocName, ChildSchema, key_exclude(ChildSchema));
assoc_caster(Opts, ParentSchema, AssocName, Assoc = #kura_assoc{}) ->
    ChildSchema = kura_schema:assoc_target(Assoc),
    Exclude = key_exclude(ChildSchema) ++ kura_schema:assoc_fields(Assoc),
    caster(Opts, ParentSchema, AssocName, ChildSchema, Exclude).

%% How a child's params become its changeset: the caller's `with`, the
%% caller's `cast` list, or every castable field. `with` wins over `cast`.
-spec caster(map(), module() | undefined, atom(), module(), [atom()]) -> caster().
caster(#{with := {Mod, Fun}}, _Parent, _Name, _Child, _Exclude) when is_atom(Mod), is_atom(Fun) ->
    fun(Data, Params) -> Mod:Fun(Data, Params) end;
caster(#{with := Fun}, _Parent, _Name, _Child, _Exclude) when is_function(Fun, 2) ->
    Fun;
caster(#{with := With}, Parent, Name, _Child, _Exclude) ->
    erlang:error({kura, {invalid_nested_cast, Parent, Name, {with, With}}});
caster(#{cast := Fields}, Parent, Name, Child, Exclude) when is_list(Fields) ->
    Castable = castable_fields(Child, Exclude),
    case [F || F <- Fields, not lists:member(F, Castable)] of
        [] -> field_caster(Child, Fields);
        Refused -> erlang:error({kura, {invalid_nested_cast, Parent, Name, {cast, Refused}}})
    end;
caster(#{cast := Fields}, Parent, Name, _Child, _Exclude) ->
    erlang:error({kura, {invalid_nested_cast, Parent, Name, {cast, Fields}}});
caster(#{}, Parent, Name, Child, Exclude) ->
    ok = every_field_allowed(Parent, Name),
    field_caster(Child, castable_fields(Child, Exclude)).

field_caster(Child, Allowed) ->
    fun(Data, Params) -> kura_changeset:cast(Child, Data, Params, Allowed) end.

%% With no allow-list every castable field of the child is taken from the
%% params. `strict_assoc_cast` refuses that; left unset it is allowed, with
%% one warning per boot.
every_field_allowed(Parent, Name) ->
    case kura_settings:flag(strict_assoc_cast) of
        true ->
            erlang:error({kura, {nested_cast_needs_allow_list, Parent, Name}});
        false ->
            ok;
        unset ->
            kura_boot_warning:once(strict_assoc_cast, #{
                msg =>
                    ~"kura: a nested cast names no allow-list, so params can set every field of the child",
                schema => Parent,
                name => Name,
                fix =>
                    ~"pass cast or with to cast_assoc, cast_embed and put_assoc, then set {kura, [{strict_assoc_cast, true}]}; that is the default in 3.0"
            })
    end.

%% Key columns the framework owns and the caller may not cast: only the
%% single auto-generated surrogate PK. A composite (natural) key is
%% caller-supplied, so nothing is excluded on that account.
key_exclude(Schema) ->
    case kura_schema:key(Schema) of
        [PK] -> [PK];
        _ -> []
    end.

cast_assoc_params(CS, AssocName, Assoc, NestedParams, Existing, WithFun) ->
    case Assoc#kura_assoc.type of
        has_many ->
            cast_has_many(CS, AssocName, Assoc, NestedParams, Existing, WithFun);
        many_to_many ->
            cast_has_many(CS, AssocName, Assoc, NestedParams, Existing, WithFun);
        has_one ->
            cast_has_one(CS, AssocName, NestedParams, WithFun)
    end.

cast_has_many(CS, AssocName, Assoc, ParamsList, Existing, WithFun) when is_list(ParamsList) ->
    ChildSchema = kura_schema:assoc_target(Assoc),
    Key = kura_schema:key(ChildSchema),
    ExistingLookup =
        case is_list(Existing) of
            true ->
                build_existing_lookup(Existing, Key, #{});
            false ->
                #{}
        end,
    ChildCSs = cast_has_many_children(ParamsList, Key, ExistingLookup, WithFun, []),
    AC = CS#kura_changeset.assoc_changes,
    CS1 = CS#kura_changeset{assoc_changes = AC#{AssocName => ChildCSs}},
    case all_valid(ChildCSs) of
        true -> CS1;
        false -> CS1#kura_changeset{valid = false}
    end;
cast_has_many(CS, AssocName, _Assoc, _BadParams, _Existing, _WithFun) ->
    kura_changeset:add_error(CS, AssocName, ~"expected a list").

cast_has_one(CS, AssocName, ChildParams, WithFun) when is_map(ChildParams) ->
    NormParams = kura_changeset:normalize_params(ChildParams),
    ChildCS = WithFun(#{}, NormParams),
    ChildCS1 = ChildCS#kura_changeset{action = insert},
    AC = CS#kura_changeset.assoc_changes,
    CS1 = CS#kura_changeset{assoc_changes = AC#{AssocName => ChildCS1}},
    case ChildCS1#kura_changeset.valid of
        true -> CS1;
        false -> CS1#kura_changeset{valid = false}
    end;
cast_has_one(CS, AssocName, _BadParams, _WithFun) ->
    kura_changeset:add_error(CS, AssocName, ~"expected a map").

holds_a_map(Value) when is_list(Value) -> lists:any(fun erlang:is_map/1, Value);
holds_a_map(Value) -> is_map(Value).

assoc_value(Assoc, Value, Opts, SchemaMod, AssocName) ->
    case holds_a_map(Value) of
        true ->
            Caster = assoc_caster(Opts, SchemaMod, AssocName, Assoc),
            coerce_assoc_value(Assoc, Value, Caster);
        false ->
            Value
    end.

coerce_assoc_value(Assoc, Value, Caster) when is_list(Value) ->
    [coerce_single(Assoc, V, Caster) || V <- Value];
coerce_assoc_value(Assoc, Value, Caster) when is_map(Value) ->
    coerce_single(Assoc, Value, Caster).

coerce_single(Assoc = #kura_assoc{type = many_to_many}, Map, Caster) when is_map(Map) ->
    Key = kura_schema:key(kura_schema:assoc_target(Assoc)),
    NormMap = kura_changeset:normalize_params(Map),
    case key_data(Key, NormMap) of
        {ok, KeyData} -> (Caster(KeyData, NormMap))#kura_changeset{action = undefined};
        error -> (Caster(#{}, NormMap))#kura_changeset{action = insert}
    end;
coerce_single(#kura_assoc{}, Map, Caster) when is_map(Map) ->
    (Caster(#{}, Map))#kura_changeset{action = insert};
coerce_single(_Assoc, Other, _Caster) ->
    Other.

%% {ok, key sub-map} distinguishes an existing record from a new insert.
-spec key_data([atom()], map()) -> {ok, map()} | error.
key_data(Key, Map) ->
    case has_all_keys(Key, Map) of
        true -> {ok, maps:with(Key, Map)};
        false -> error
    end.

has_all_keys(Key, Map) ->
    lists:all(fun(K) -> maps:get(K, Map, undefined) =/= undefined end, Key).

castable_fields(Schema, Exclude) ->
    AllFields = kura_schema:field_names(Schema),
    NonVirtual = kura_schema:non_virtual_fields(Schema),
    [F || F <- AllFields, lists:member(F, NonVirtual), not lists:member(F, Exclude)].

%%----------------------------------------------------------------------
%% Internal: type narrowing helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec cast_embed_list([map()], caster(), [#kura_changeset{}]) -> [#kura_changeset{}].
cast_embed_list([], _WithFun, Acc) ->
    reverse_changesets(Acc, []);
cast_embed_list([ChildParams | Rest], WithFun, Acc) ->
    NormParams = kura_changeset:normalize_params(ChildParams),
    CS = WithFun(#{}, NormParams),
    cast_embed_list(Rest, WithFun, [CS | Acc]).

-spec all_valid([#kura_changeset{}]) -> boolean().
all_valid([]) -> true;
all_valid([#kura_changeset{valid = false} | _]) -> false;
all_valid([_ | Rest]) -> all_valid(Rest).

-spec collect_errors([#kura_changeset{}], [{atom(), binary()}]) -> [{atom(), binary()}].
collect_errors([], Acc) ->
    Acc;
collect_errors([#kura_changeset{valid = false, errors = E} | Rest], Acc) ->
    collect_errors(Rest, Acc ++ E);
collect_errors([_ | Rest], Acc) ->
    collect_errors(Rest, Acc).

-spec build_existing_lookup([map()], [atom()], map()) -> map().
build_existing_lookup([], _Key, Acc) ->
    Acc;
build_existing_lookup([E | Rest], Key, Acc) ->
    case key_tuple(Key, E) of
        undefined -> build_existing_lookup(Rest, Key, Acc);
        Tuple -> build_existing_lookup(Rest, Key, Acc#{Tuple => E})
    end.

-spec cast_has_many_children(
    [map()],
    [atom()],
    map(),
    fun((map(), map()) -> #kura_changeset{}),
    [#kura_changeset{}]
) -> [#kura_changeset{}].
cast_has_many_children([], _Key, _Lookup, _WithFun, Acc) ->
    reverse_changesets(Acc, []);
cast_has_many_children([ChildParams | Rest], Key, Lookup, WithFun, Acc) ->
    NormParams = kura_changeset:normalize_params(ChildParams),
    ChildCS = classify_child(Key, key_tuple(Key, NormParams), Lookup, WithFun, NormParams),
    cast_has_many_children(Rest, Key, Lookup, WithFun, [ChildCS | Acc]).

%% A missing key is always a new insert. A single surrogate key that is
%% present means an update (an auto-generated id is only supplied to
%% reference an existing record). A composite/natural key is always
%% supplied, so it is an update only when it matches a loaded existing row.
classify_child(_Key, undefined, _Lookup, WithFun, NormParams) ->
    (WithFun(#{}, NormParams))#kura_changeset{action = insert};
classify_child([_Single], Tuple, Lookup, WithFun, NormParams) ->
    (WithFun(maps:get(Tuple, Lookup, #{}), NormParams))#kura_changeset{action = update};
classify_child(_Composite, Tuple, Lookup, WithFun, NormParams) ->
    case Lookup of
        #{Tuple := ExistingData} ->
            (WithFun(ExistingData, NormParams))#kura_changeset{action = update};
        #{} ->
            (WithFun(#{}, NormParams))#kura_changeset{action = insert}
    end.

%% The key value-tuple for an existing record, else undefined (a new insert).
-spec key_tuple([atom()], map()) -> tuple() | undefined.
key_tuple(Key, Map) ->
    case has_all_keys(Key, Map) of
        true -> list_to_tuple([maps:get(K, Map) || K <- Key]);
        false -> undefined
    end.

-spec reverse_changesets([#kura_changeset{}], [#kura_changeset{}]) -> [#kura_changeset{}].
reverse_changesets([], Acc) -> Acc;
reverse_changesets([H | T], Acc) -> reverse_changesets(T, [H | Acc]).
