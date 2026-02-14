-module(kura_changeset).

-include("kura.hrl").

-export([
    cast/4,
    validate_required/2,
    validate_format/3,
    validate_length/3,
    validate_number/3,
    validate_inclusion/3,
    validate_change/3,
    unique_constraint/2,
    unique_constraint/3,
    foreign_key_constraint/2,
    foreign_key_constraint/3,
    check_constraint/3,
    check_constraint/4,
    add_error/3,
    get_change/2,
    get_change/3,
    get_field/2,
    get_field/3,
    put_change/3,
    apply_changes/1,
    apply_action/2
]).

%%----------------------------------------------------------------------
%% cast/4 — create a changeset from schema, data, params, allowed fields
%%----------------------------------------------------------------------

-spec cast(module(), map(), map(), [atom()]) -> #kura_changeset{}.
cast(SchemaMod, Data, Params, Allowed) ->
    Types = kura_schema:field_types(SchemaMod),
    NormParams = normalize_params(Params),
    {Changes, Errors} = cast_params(NormParams, Allowed, Types, Data),
    #kura_changeset{
        valid = Errors =:= [],
        schema = SchemaMod,
        data = Data,
        params = NormParams,
        changes = Changes,
        errors = Errors,
        types = Types
    }.

%%----------------------------------------------------------------------
%% Validations
%%----------------------------------------------------------------------

-spec validate_required(#kura_changeset{}, [atom()]) -> #kura_changeset{}.
validate_required(CS = #kura_changeset{changes = Changes, data = Data, errors = Errors}, Fields) ->
    NewErrors = lists:foldl(
        fun(Field, Acc) ->
            Val =
                case maps:find(Field, Changes) of
                    {ok, V} -> V;
                    error -> maps:get(Field, Data, undefined)
                end,
            case is_blank(Val) of
                true -> [{Field, <<"can't be blank">>} | Acc];
                false -> Acc
            end
        end,
        [],
        Fields
    ),
    CS#kura_changeset{
        required = Fields,
        errors = Errors ++ lists:reverse(NewErrors),
        valid = (Errors ++ NewErrors) =:= []
    }.

-spec validate_format(#kura_changeset{}, atom(), binary()) -> #kura_changeset{}.
validate_format(CS, Field, Pattern) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val when is_binary(Val) ->
            case re:run(Val, Pattern) of
                {match, _} -> CS;
                nomatch -> add_error(CS, Field, <<"has invalid format">>)
            end;
        _ ->
            CS
    end.

-spec validate_length(#kura_changeset{}, atom(), [{atom(), non_neg_integer()}]) ->
    #kura_changeset{}.
validate_length(CS, Field, Opts) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val when is_binary(Val) ->
            Len = byte_size(Val),
            check_length(CS, Field, Len, Opts);
        Val when is_list(Val) ->
            Len = length(Val),
            check_length(CS, Field, Len, Opts);
        _ ->
            CS
    end.

-spec validate_number(#kura_changeset{}, atom(), [{atom(), number()}]) -> #kura_changeset{}.
validate_number(CS, Field, Opts) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val when is_number(Val) ->
            check_number(CS, Field, Val, Opts);
        _ ->
            CS
    end.

-spec validate_inclusion(#kura_changeset{}, atom(), [term()]) -> #kura_changeset{}.
validate_inclusion(CS, Field, Values) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val ->
            case lists:member(Val, Values) of
                true -> CS;
                false -> add_error(CS, Field, <<"is invalid">>)
            end
    end.

-spec validate_change(#kura_changeset{}, atom(), fun((term()) -> ok | {error, binary()})) ->
    #kura_changeset{}.
validate_change(CS, Field, Fun) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val ->
            case Fun(Val) of
                ok -> CS;
                {error, Msg} -> add_error(CS, Field, Msg)
            end
    end.

%%----------------------------------------------------------------------
%% Constraint declarations
%%----------------------------------------------------------------------

-spec unique_constraint(#kura_changeset{}, atom()) -> #kura_changeset{}.
unique_constraint(CS, Field) ->
    unique_constraint(CS, Field, #{}).

-spec unique_constraint(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
unique_constraint(CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts) ->
    Table = SchemaMod:table(),
    Name = maps:get(
        name, Opts, <<Table/binary, "_", (atom_to_binary(Field, utf8))/binary, "_key">>
    ),
    Msg = maps:get(message, Opts, <<"has already been taken">>),
    C = #kura_constraint{type = unique, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

-spec foreign_key_constraint(#kura_changeset{}, atom()) -> #kura_changeset{}.
foreign_key_constraint(CS, Field) ->
    foreign_key_constraint(CS, Field, #{}).

-spec foreign_key_constraint(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
foreign_key_constraint(
    CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts
) ->
    Table = SchemaMod:table(),
    Name = maps:get(
        name, Opts, <<Table/binary, "_", (atom_to_binary(Field, utf8))/binary, "_fkey">>
    ),
    Msg = maps:get(message, Opts, <<"does not exist">>),
    C = #kura_constraint{type = foreign_key, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

-spec check_constraint(#kura_changeset{}, binary(), atom()) -> #kura_changeset{}.
check_constraint(CS, Constraint, Field) ->
    check_constraint(CS, Constraint, Field, #{}).

-spec check_constraint(#kura_changeset{}, binary(), atom(), map()) -> #kura_changeset{}.
check_constraint(CS = #kura_changeset{constraints = Constraints}, Constraint, Field, Opts) ->
    Msg = maps:get(message, Opts, <<"is invalid">>),
    C = #kura_constraint{type = check, constraint = Constraint, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

%%----------------------------------------------------------------------
%% Error / change helpers
%%----------------------------------------------------------------------

-spec add_error(#kura_changeset{}, atom(), binary()) -> #kura_changeset{}.
add_error(CS = #kura_changeset{errors = Errors}, Field, Message) ->
    CS#kura_changeset{errors = Errors ++ [{Field, Message}], valid = false}.

-spec get_change(#kura_changeset{}, atom()) -> term().
get_change(#kura_changeset{changes = Changes}, Field) ->
    maps:get(Field, Changes, undefined).

-spec get_change(#kura_changeset{}, atom(), term()) -> term().
get_change(#kura_changeset{changes = Changes}, Field, Default) ->
    maps:get(Field, Changes, Default).

-spec get_field(#kura_changeset{}, atom()) -> term().
get_field(#kura_changeset{changes = Changes, data = Data}, Field) ->
    case maps:find(Field, Changes) of
        {ok, V} -> V;
        error -> maps:get(Field, Data, undefined)
    end.

-spec get_field(#kura_changeset{}, atom(), term()) -> term().
get_field(#kura_changeset{changes = Changes, data = Data}, Field, Default) ->
    case maps:find(Field, Changes) of
        {ok, V} -> V;
        error -> maps:get(Field, Data, Default)
    end.

-spec put_change(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_change(CS = #kura_changeset{changes = Changes}, Field, Value) ->
    CS#kura_changeset{changes = Changes#{Field => Value}}.

-spec apply_changes(#kura_changeset{}) -> map().
apply_changes(#kura_changeset{data = Data, changes = Changes}) ->
    maps:merge(Data, Changes).

-spec apply_action(#kura_changeset{}, atom()) -> {ok, map()} | {error, #kura_changeset{}}.
apply_action(CS = #kura_changeset{valid = true}, Action) ->
    {ok, apply_changes(CS#kura_changeset{action = Action})};
apply_action(CS, Action) ->
    {error, CS#kura_changeset{action = Action}}.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

normalize_params(Params) when is_map(Params) ->
    maps:fold(
        fun(K, V, Acc) ->
            Key =
                if
                    is_binary(K) ->
                        try
                            binary_to_existing_atom(K, utf8)
                        catch
                            error:badarg -> K
                        end;
                    is_atom(K) ->
                        K;
                    is_list(K) ->
                        try
                            list_to_existing_atom(K)
                        catch
                            error:badarg -> list_to_binary(K)
                        end;
                    true ->
                        K
                end,
            Acc#{Key => V}
        end,
        #{},
        Params
    ).

cast_params(Params, Allowed, Types, Data) ->
    lists:foldl(
        fun(Field, {ChangeAcc, ErrAcc}) ->
            case maps:find(Field, Params) of
                {ok, Value} ->
                    case maps:find(Field, Types) of
                        {ok, Type} ->
                            case kura_types:cast(Type, Value) of
                                {ok, Casted} ->
                                    OldVal = maps:get(Field, Data, undefined),
                                    if
                                        Casted =:= OldVal ->
                                            {ChangeAcc, ErrAcc};
                                        true ->
                                            {ChangeAcc#{Field => Casted}, ErrAcc}
                                    end;
                                {error, Msg} ->
                                    {ChangeAcc, ErrAcc ++ [{Field, Msg}]}
                            end;
                        error ->
                            {ChangeAcc, ErrAcc}
                    end;
                error ->
                    {ChangeAcc, ErrAcc}
            end
        end,
        {#{}, []},
        Allowed
    ).

is_blank(undefined) -> true;
is_blank(<<>>) -> true;
is_blank(null) -> true;
is_blank(_) -> false.

check_length(CS, Field, Len, Opts) ->
    lists:foldl(
        fun
            ({min, Min}, Acc) when Len < Min ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be at least ~B character(s)", [Min])
                    )
                );
            ({max, Max}, Acc) when Len > Max ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be at most ~B character(s)", [Max])
                    )
                );
            ({is, Exact}, Acc) when Len =/= Exact ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be ~B character(s)", [Exact])
                    )
                );
            (_, Acc) ->
                Acc
        end,
        CS,
        Opts
    ).

check_number(CS, Field, Val, Opts) ->
    lists:foldl(
        fun
            ({greater_than, N}, Acc) when Val =< N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be greater than ~p", [N])
                    )
                );
            ({less_than, N}, Acc) when Val >= N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be less than ~p", [N])
                    )
                );
            ({greater_than_or_equal_to, N}, Acc) when Val < N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be greater than or equal to ~p", [N])
                    )
                );
            ({less_than_or_equal_to, N}, Acc) when Val > N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be less than or equal to ~p", [N])
                    )
                );
            ({equal_to, N}, Acc) when Val =/= N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be equal to ~p", [N])
                    )
                );
            (_, Acc) ->
                Acc
        end,
        CS,
        Opts
    ).
