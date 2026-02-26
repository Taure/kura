-module(kura_changeset).
-moduledoc """
Changesets for casting external data, validating fields, and declaring constraints.

A changeset tracks changes against existing data, accumulates errors, and
declares database constraints for friendly error handling on insert/update.

```erlang
CS = kura_changeset:cast(my_user, #{}, Params, [name, email]),
CS1 = kura_changeset:validate_required(CS, [name, email]),
CS2 = kura_changeset:validate_format(CS1, email, <<"@">>),
CS3 = kura_changeset:unique_constraint(CS2, email).
```
""".

-include("kura.hrl").

-export([
    cast/4,
    cast_assoc/2,
    cast_assoc/3,
    put_assoc/3,
    validate_required/2,
    validate_format/3,
    validate_length/3,
    validate_number/3,
    validate_inclusion/3,
    validate_confirmation/2,
    validate_confirmation/3,
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
    apply_action/2,
    cast_embed/2,
    cast_embed/3,
    normalize_params/1
]).

-doc """
Create a changeset by casting `Params` against type definitions, filtering to `Allowed` fields.

The first argument can be either a schema module (atom) or a types map for schemaless changesets:

```erlang
%% Schema-based
CS = kura_changeset:cast(my_user, #{}, Params, [name, email]).

%% Schemaless
Types = #{email => string, password => string},
CS = kura_changeset:cast(Types, #{}, Params, [email, password]).
```

Schemaless changesets are validation-only — they cannot be persisted via `kura_repo`.
""".
-spec cast(module() | #{atom() => kura_types:kura_type()}, map(), map(), [atom()]) ->
    #kura_changeset{}.
cast(Types, Data, Params, Allowed) when is_map(Types) ->
    NormParams = normalize_params(Params),
    {Changes, Errors} = cast_params(NormParams, Allowed, Types, Data),
    #kura_changeset{
        valid = Errors =:= [],
        schema = undefined,
        data = Data,
        params = NormParams,
        changes = Changes,
        errors = Errors,
        types = Types
    };
cast(SchemaMod, Data, Params, Allowed) when is_atom(SchemaMod) ->
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

-doc "Validate that all `Fields` are present and non-blank.".
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

-doc "Validate that `Field` matches the regex `Pattern`.".
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

-doc "Validate length of `Field`. Opts: `{min, N}`, `{max, N}`, `{is, N}`.".
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

-doc "Validate numeric `Field`. Opts: `{greater_than, N}`, `{less_than, N}`, etc.".
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

-doc "Validate that `Field` value is in the given list of `Values`.".
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

-doc "Validate that `Field` has a matching confirmation field in params.".
-spec validate_confirmation(#kura_changeset{}, atom()) -> #kura_changeset{}.
validate_confirmation(CS, Field) ->
    validate_confirmation(CS, Field, #{}).

-doc "Validate confirmation with options. Opts: `#{message => binary()}`.".
-spec validate_confirmation(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
validate_confirmation(CS, Field, Opts) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val ->
            ConfField = confirmation_field(Field),
            Msg = maps:get(message, Opts, <<"does not match">>),
            case maps:find(ConfField, CS#kura_changeset.params) of
                {ok, Val} -> CS;
                _ -> add_error(CS, ConfField, Msg)
            end
    end.

confirmation_field(Field) ->
    binary_to_atom(<<(atom_to_binary(Field))/binary, "_confirmation">>).

-doc "Validate `Field` with a custom function returning `ok` or `{error, Message}`.".
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

-doc "Declare a unique constraint on `Field` for friendly DB error mapping.".
-spec unique_constraint(#kura_changeset{}, atom()) -> #kura_changeset{}.
unique_constraint(CS, Field) ->
    unique_constraint(CS, Field, #{}).

-spec unique_constraint(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
unique_constraint(#kura_changeset{schema = undefined}, _Field, Opts) when
    not is_map_key(name, Opts)
->
    error(
        {schemaless_constraint,
            "unique_constraint on a schemaless changeset requires :name in opts"}
    );
unique_constraint(CS = #kura_changeset{schema = undefined, constraints = Constraints}, Field, Opts) ->
    Name = maps:get(name, Opts),
    Msg = maps:get(message, Opts, <<"has already been taken">>),
    C = #kura_constraint{type = unique, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]};
unique_constraint(CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts) ->
    Table = SchemaMod:table(),
    Name = maps:get(
        name, Opts, <<Table/binary, "_", (atom_to_binary(Field, utf8))/binary, "_key">>
    ),
    Msg = maps:get(message, Opts, <<"has already been taken">>),
    C = #kura_constraint{type = unique, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

-doc "Declare a foreign key constraint on `Field`.".
-spec foreign_key_constraint(#kura_changeset{}, atom()) -> #kura_changeset{}.
foreign_key_constraint(CS, Field) ->
    foreign_key_constraint(CS, Field, #{}).

-spec foreign_key_constraint(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
foreign_key_constraint(#kura_changeset{schema = undefined}, _Field, Opts) when
    not is_map_key(name, Opts)
->
    error(
        {schemaless_constraint,
            "foreign_key_constraint on a schemaless changeset requires :name in opts"}
    );
foreign_key_constraint(
    CS = #kura_changeset{schema = undefined, constraints = Constraints}, Field, Opts
) ->
    Name = maps:get(name, Opts),
    Msg = maps:get(message, Opts, <<"does not exist">>),
    C = #kura_constraint{type = foreign_key, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]};
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

-doc "Declare a check constraint with the given `Constraint` name mapped to `Field`.".
-spec check_constraint(#kura_changeset{}, binary(), atom()) -> #kura_changeset{}.
check_constraint(CS, Constraint, Field) ->
    check_constraint(CS, Constraint, Field, #{}).

-spec check_constraint(#kura_changeset{}, binary(), atom(), map()) -> #kura_changeset{}.
check_constraint(CS = #kura_changeset{constraints = Constraints}, Constraint, Field, Opts) ->
    Msg = maps:get(message, Opts, <<"is invalid">>),
    C = #kura_constraint{type = check, constraint = Constraint, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

-doc "Add an error to the changeset for `Field`.".
-spec add_error(#kura_changeset{}, atom(), binary()) -> #kura_changeset{}.
add_error(CS = #kura_changeset{errors = Errors}, Field, Message) ->
    CS#kura_changeset{errors = Errors ++ [{Field, Message}], valid = false}.

-doc "Get a change value, or `undefined` if the field was not changed.".
-spec get_change(#kura_changeset{}, atom()) -> term().
get_change(#kura_changeset{changes = Changes}, Field) ->
    maps:get(Field, Changes, undefined).

-doc "Get a change value, or `Default` if the field was not changed.".
-spec get_change(#kura_changeset{}, atom(), term()) -> term().
get_change(#kura_changeset{changes = Changes}, Field, Default) ->
    maps:get(Field, Changes, Default).

-doc "Get the field value from changes (preferred) or data.".
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

-doc "Put a change value directly, bypassing casting.".
-spec put_change(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_change(CS = #kura_changeset{changes = Changes}, Field, Value) ->
    CS#kura_changeset{changes = Changes#{Field => Value}}.

-doc "Merge changes into data, returning the resulting map (does not validate).".
-spec apply_changes(#kura_changeset{}) -> map().
apply_changes(#kura_changeset{data = Data, changes = Changes}) ->
    maps:merge(Data, Changes).

-doc "Apply changes if valid, returning `{ok, Map}` or `{error, Changeset}` with the action set.".
-spec apply_action(#kura_changeset{}, atom()) -> {ok, map()} | {error, #kura_changeset{}}.
apply_action(CS = #kura_changeset{valid = true}, Action) ->
    {ok, apply_changes(CS#kura_changeset{action = Action})};
apply_action(CS, Action) ->
    {error, CS#kura_changeset{action = Action}}.

%%----------------------------------------------------------------------
%% Association casting (delegated to kura_changeset_assoc)
%%----------------------------------------------------------------------

-doc "Cast nested association parameters into child changesets.".
-spec cast_assoc(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_assoc(CS, AssocName) ->
    kura_changeset_assoc:cast_assoc(CS, AssocName).

-doc "Cast nested association parameters with options. Opts: `with` — custom changeset function.".
-spec cast_assoc(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
cast_assoc(CS, AssocName, Opts) ->
    kura_changeset_assoc:cast_assoc(CS, AssocName, Opts).

-doc "Put pre-built changesets or raw maps directly as association changes.".
-spec put_assoc(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_assoc(CS, AssocName, Value) ->
    kura_changeset_assoc:put_assoc(CS, AssocName, Value).

%%----------------------------------------------------------------------
%% Embed casting (delegated to kura_changeset_assoc)
%%----------------------------------------------------------------------

-doc "Cast nested embedded schema parameters into parent changes.".
-spec cast_embed(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_embed(CS, EmbedName) ->
    kura_changeset_assoc:cast_embed(CS, EmbedName).

-doc "Cast nested embedded schema parameters with options. Opts: `with` — custom changeset function.".
-spec cast_embed(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
cast_embed(CS, EmbedName, Opts) ->
    kura_changeset_assoc:cast_embed(CS, EmbedName, Opts).

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

normalize_params(Params) when is_map(Params) ->
    maps:fold(
        fun(K, V, Acc) -> Acc#{normalize_key(K) => V} end,
        #{},
        Params
    ).

normalize_key(K) when is_binary(K) ->
    try
        binary_to_existing_atom(K, utf8)
    catch
        error:badarg -> K
    end;
normalize_key(K) when is_atom(K) ->
    K;
normalize_key(K) when is_list(K) ->
    try
        list_to_existing_atom(K)
    catch
        error:badarg -> list_to_binary(K)
    end;
normalize_key(K) ->
    K.

cast_params(_Params, [], _Types, _Data) ->
    {#{}, []};
cast_params(Params, [Field | Rest], Types, Data) ->
    {Changes, Errors} = cast_params(Params, Rest, Types, Data),
    case {maps:find(Field, Params), maps:find(Field, Types)} of
        {{ok, Value}, {ok, Type}} ->
            case kura_types:cast(Type, Value) of
                {ok, Casted} ->
                    case maps:get(Field, Data, undefined) of
                        Casted -> {Changes, Errors};
                        _ -> {Changes#{Field => Casted}, Errors}
                    end;
                {error, Msg} ->
                    {Changes, Errors ++ [{Field, Msg}]}
            end;
        _ ->
            {Changes, Errors}
    end.

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
