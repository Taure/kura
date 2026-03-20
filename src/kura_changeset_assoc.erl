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
            case Params of
                #{AssocName := NestedParams} ->
                    Existing = maps:get(AssocName, Data, undefined),
                    WithFun = maps:get(with, Opts, default_cast_fun(Assoc)),
                    cast_assoc_params(CS, AssocName, Assoc, NestedParams, Existing, WithFun);
                #{} ->
                    CS
            end;
        {error, not_found} ->
            kura_changeset:add_error(CS, AssocName, ~"unknown association")
    end.

-spec put_assoc(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_assoc(CS = #kura_changeset{schema = SchemaMod, assoc_changes = AC}, AssocName, Value) ->
    case kura_schema:association(SchemaMod, AssocName) of
        {ok, Assoc} ->
            Changesets = coerce_assoc_value(Assoc, Value),
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
            case Params of
                #{EmbedName := NestedParams} ->
                    WithFun = maps:get(with, Opts, default_embed_cast_fun(Embed)),
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

default_embed_cast_fun(#kura_embed{schema = EmbedSchema}) ->
    Allowed = castable_fields(EmbedSchema, []),
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

default_cast_fun(#kura_assoc{type = many_to_many, schema = ChildSchema}) ->
    PK = kura_schema:primary_key(ChildSchema),
    Allowed = castable_fields(ChildSchema, [PK]),
    fun(Data, ChildParams) ->
        kura_changeset:cast(ChildSchema, Data, ChildParams, Allowed)
    end;
default_cast_fun(#kura_assoc{schema = ChildSchema, foreign_key = FK}) ->
    PK = kura_schema:primary_key(ChildSchema),
    Allowed = castable_fields(ChildSchema, [PK, FK]),
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
                build_existing_lookup(Existing, PK, #{});
            false ->
                #{}
        end,
    ChildCSs = cast_has_many_children(ParamsList, PK, ExistingLookup, WithFun, []),
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
    Allowed = castable_fields(ChildSchema, [PK]),
    NormMap = kura_changeset:normalize_params(Map),
    case NormMap of
        #{PK := PKVal} when PKVal =/= undefined ->
            CS = kura_changeset:cast(ChildSchema, #{PK => PKVal}, NormMap, Allowed),
            CS#kura_changeset{action = undefined};
        #{} ->
            CS = kura_changeset:cast(ChildSchema, #{}, NormMap, Allowed),
            CS#kura_changeset{action = insert}
    end;
coerce_single(#kura_assoc{schema = ChildSchema, foreign_key = FK}, Map) when is_map(Map) ->
    PK = kura_schema:primary_key(ChildSchema),
    Allowed = castable_fields(ChildSchema, [PK, FK]),
    CS = kura_changeset:cast(ChildSchema, #{}, Map, Allowed),
    CS#kura_changeset{action = insert};
coerce_single(_Assoc, #kura_changeset{} = CS) ->
    CS.

castable_fields(Schema, Exclude) ->
    AllFields = kura_schema:field_names(Schema),
    NonVirtual = kura_schema:non_virtual_fields(Schema),
    [F || F <- AllFields, lists:member(F, NonVirtual), not lists:member(F, Exclude)].

%%----------------------------------------------------------------------
%% Internal: type narrowing helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec cast_embed_list([map()], fun((map(), map()) -> #kura_changeset{}), [#kura_changeset{}]) ->
    [#kura_changeset{}].
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

-spec build_existing_lookup([map()], atom(), map()) -> map().
build_existing_lookup([], _PK, Acc) ->
    Acc;
build_existing_lookup([E | Rest], PK, Acc) ->
    case maps:get(PK, E, undefined) of
        undefined -> build_existing_lookup(Rest, PK, Acc);
        Key -> build_existing_lookup(Rest, PK, Acc#{Key => E})
    end.

-spec cast_has_many_children(
    [map()],
    atom(),
    map(),
    fun((map(), map()) -> #kura_changeset{}),
    [#kura_changeset{}]
) -> [#kura_changeset{}].
cast_has_many_children([], _PK, _Lookup, _WithFun, Acc) ->
    reverse_changesets(Acc, []);
cast_has_many_children([ChildParams | Rest], PK, Lookup, WithFun, Acc) ->
    NormParams = kura_changeset:normalize_params(ChildParams),
    ChildCS =
        case NormParams of
            #{PK := PKVal} when PKVal =/= undefined ->
                ExistingData = maps:get(PKVal, Lookup, #{}),
                CS = WithFun(ExistingData, NormParams),
                CS#kura_changeset{action = update};
            #{} ->
                CS = WithFun(#{}, NormParams),
                CS#kura_changeset{action = insert}
        end,
    cast_has_many_children(Rest, PK, Lookup, WithFun, [ChildCS | Acc]).

-spec reverse_changesets([#kura_changeset{}], [#kura_changeset{}]) -> [#kura_changeset{}].
reverse_changesets([], Acc) -> Acc;
reverse_changesets([H | T], Acc) -> reverse_changesets(T, [H | Acc]).
