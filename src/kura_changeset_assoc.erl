-module(kura_changeset_assoc).

-include("kura.hrl").

-export([
    cast_assoc/2,
    cast_assoc/3,
    put_assoc/3,
    cast_embed/2,
    cast_embed/3
]).

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
            NestedParams = maps:get(AssocName, Params, undefined),
            case NestedParams of
                undefined ->
                    CS;
                _ ->
                    Existing = maps:get(AssocName, Data, undefined),
                    WithFun = maps:get(with, Opts, default_cast_fun(Assoc)),
                    cast_assoc_params(CS, AssocName, Assoc, NestedParams, Existing, WithFun)
            end;
        {error, not_found} ->
            kura_changeset:add_error(CS, AssocName, <<"unknown association">>)
    end.

-spec put_assoc(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_assoc(CS = #kura_changeset{schema = SchemaMod, assoc_changes = AC}, AssocName, Value) ->
    case kura_schema:association(SchemaMod, AssocName) of
        {ok, Assoc} ->
            Changesets = coerce_assoc_value(Assoc, Value),
            CS#kura_changeset{assoc_changes = AC#{AssocName => Changesets}};
        {error, not_found} ->
            kura_changeset:add_error(CS, AssocName, <<"unknown association">>)
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
            NestedParams = maps:get(EmbedName, Params, undefined),
            case NestedParams of
                undefined ->
                    CS;
                _ ->
                    WithFun = maps:get(with, Opts, default_embed_cast_fun(Embed)),
                    cast_embed_params(CS, EmbedName, Embed, NestedParams, WithFun)
            end;
        {error, not_found} ->
            kura_changeset:add_error(CS, EmbedName, <<"unknown embed">>)
    end.

%%----------------------------------------------------------------------
%% Internal: embed helpers
%%----------------------------------------------------------------------

default_embed_cast_fun(#kura_embed{schema = EmbedSchema}) ->
    AllFields = kura_schema:field_names(EmbedSchema),
    NonVirtual = kura_schema:non_virtual_fields(EmbedSchema),
    Allowed = [F || F <- AllFields, lists:member(F, NonVirtual)],
    fun(Data, EmbedParams) ->
        kura_changeset:cast(EmbedSchema, Data, EmbedParams, Allowed)
    end.

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
    kura_changeset:add_error(CS, EmbedName, <<"expected a map">>);
cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_many}, ParamsList, WithFun) when
    is_list(ParamsList)
->
    Results = lists:map(
        fun(ChildParams) ->
            NormParams = kura_changeset:normalize_params(ChildParams),
            WithFun(#{}, NormParams)
        end,
        ParamsList
    ),
    AllValid = lists:all(fun(#kura_changeset{valid = V}) -> V end, Results),
    case AllValid of
        true ->
            Applied = [kura_changeset:apply_changes(R) || R <- Results],
            Changes = CS#kura_changeset.changes,
            CS#kura_changeset{changes = Changes#{EmbedName => Applied}};
        false ->
            AllErrors = lists:foldl(
                fun(#kura_changeset{errors = E}, Acc) -> Acc ++ E end,
                CS#kura_changeset.errors,
                [R || R <- Results, R#kura_changeset.valid =:= false]
            ),
            CS#kura_changeset{valid = false, errors = AllErrors}
    end;
cast_embed_params(CS, EmbedName, #kura_embed{type = embeds_many}, _Params, _WithFun) ->
    kura_changeset:add_error(CS, EmbedName, <<"expected a list">>).

%%----------------------------------------------------------------------
%% Internal: association helpers
%%----------------------------------------------------------------------

default_cast_fun(#kura_assoc{type = many_to_many, schema = ChildSchema}) ->
    PK = kura_schema:primary_key(ChildSchema),
    AllFields = kura_schema:field_names(ChildSchema),
    NonVirtual = kura_schema:non_virtual_fields(ChildSchema),
    Allowed = [F || F <- AllFields, lists:member(F, NonVirtual), F =/= PK],
    fun(Data, ChildParams) ->
        kura_changeset:cast(ChildSchema, Data, ChildParams, Allowed)
    end;
default_cast_fun(#kura_assoc{schema = ChildSchema, foreign_key = FK}) ->
    PK = kura_schema:primary_key(ChildSchema),
    AllFields = kura_schema:field_names(ChildSchema),
    NonVirtual = kura_schema:non_virtual_fields(ChildSchema),
    Allowed = [F || F <- AllFields, lists:member(F, NonVirtual), F =/= PK, F =/= FK],
    fun(Data, ChildParams) ->
        kura_changeset:cast(ChildSchema, Data, ChildParams, Allowed)
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
    ChildSchema = Assoc#kura_assoc.schema,
    PK = kura_schema:primary_key(ChildSchema),
    ExistingLookup =
        case is_list(Existing) of
            true ->
                maps:from_list([{maps:get(PK, E), E} || E <- Existing, maps:is_key(PK, E)]);
            false ->
                #{}
        end,
    ChildCSs = lists:map(
        fun(ChildParams) ->
            NormParams = kura_changeset:normalize_params(ChildParams),
            case maps:find(PK, NormParams) of
                {ok, PKVal} when PKVal =/= undefined ->
                    ExistingData = maps:get(PKVal, ExistingLookup, #{}),
                    ChildCS = WithFun(ExistingData, NormParams),
                    ChildCS#kura_changeset{action = update};
                _ ->
                    ChildCS = WithFun(#{}, NormParams),
                    ChildCS#kura_changeset{action = insert}
            end
        end,
        ParamsList
    ),
    AllValid = lists:all(fun(#kura_changeset{valid = V}) -> V end, ChildCSs),
    AC = CS#kura_changeset.assoc_changes,
    CS1 = CS#kura_changeset{assoc_changes = AC#{AssocName => ChildCSs}},
    case AllValid of
        true -> CS1;
        false -> CS1#kura_changeset{valid = false}
    end;
cast_has_many(CS, AssocName, _Assoc, _BadParams, _Existing, _WithFun) ->
    kura_changeset:add_error(CS, AssocName, <<"expected a list">>).

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
    kura_changeset:add_error(CS, AssocName, <<"expected a map">>).

coerce_assoc_value(Assoc, Value) when is_list(Value) ->
    [coerce_single(Assoc, V) || V <- Value];
coerce_assoc_value(Assoc, Value) when is_map(Value), not is_map_key(valid, Value) ->
    coerce_single(Assoc, Value);
coerce_assoc_value(_Assoc, #kura_changeset{} = CS) ->
    CS;
coerce_assoc_value(_Assoc, Value) ->
    Value.

coerce_single(#kura_assoc{type = many_to_many, schema = ChildSchema}, Map) when is_map(Map) ->
    PK = kura_schema:primary_key(ChildSchema),
    AllFields = kura_schema:field_names(ChildSchema),
    NonVirtual = kura_schema:non_virtual_fields(ChildSchema),
    Allowed = [F || F <- AllFields, lists:member(F, NonVirtual), F =/= PK],
    NormMap = kura_changeset:normalize_params(Map),
    case maps:find(PK, NormMap) of
        {ok, PKVal} when PKVal =/= undefined ->
            CS = kura_changeset:cast(ChildSchema, #{PK => PKVal}, NormMap, Allowed),
            CS#kura_changeset{action = undefined};
        _ ->
            CS = kura_changeset:cast(ChildSchema, #{}, NormMap, Allowed),
            CS#kura_changeset{action = insert}
    end;
coerce_single(#kura_assoc{schema = ChildSchema, foreign_key = FK}, Map) when is_map(Map) ->
    PK = kura_schema:primary_key(ChildSchema),
    AllFields = kura_schema:field_names(ChildSchema),
    NonVirtual = kura_schema:non_virtual_fields(ChildSchema),
    Allowed = [F || F <- AllFields, lists:member(F, NonVirtual), F =/= PK, F =/= FK],
    CS = kura_changeset:cast(ChildSchema, #{}, Map, Allowed),
    CS#kura_changeset{action = insert};
coerce_single(_Assoc, #kura_changeset{} = CS) ->
    CS.
