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
    put_assoc/4,
    validate_required/2,
    validate_format/3,
    validate_length/3,
    validate_number/3,
    validate_inclusion/3,
    validate_exclusion/3,
    validate_subset/3,
    validate_confirmation/2,
    validate_confirmation/3,
    validate_change/3,
    unique_constraint/2,
    unique_constraint/3,
    foreign_key_constraint/2,
    foreign_key_constraint/3,
    check_constraint/3,
    check_constraint/4,
    exclusion_constraint/2,
    exclusion_constraint/3,
    add_error/3,
    get_change/2,
    get_change/3,
    get_field/2,
    get_field/3,
    put_change/3,
    delete_change/2,
    apply_changes/1,
    apply_action/2,
    traverse_errors/2,
    prepare_changes/2,
    optimistic_lock/2,
    cast_embed/2,
    cast_embed/3,
    normalize_params/1
]).

-ifdef(TEST).
-export([
    join_col_names/2,
    join_bins/2,
    build_table_constraints/2,
    build_index_constraints/2,
    schema_table/1
]).
-endif.

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

Schemaless changesets are validation-only - they cannot be persisted via `kura_repo`.
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
    Constraints = build_schema_constraints(SchemaMod),
    #kura_changeset{
        valid = Errors =:= [],
        schema = SchemaMod,
        data = Data,
        params = NormParams,
        changes = Changes,
        errors = Errors,
        types = Types,
        constraints = Constraints
    }.

-doc "Validate that all `Fields` are present and non-blank.".
-spec validate_required(#kura_changeset{}, [atom()]) -> #kura_changeset{}.
validate_required(CS = #kura_changeset{changes = Changes, data = Data, errors = Errors}, Fields) ->
    NewErrors = check_required_fields(Fields, Changes, Data),
    CS#kura_changeset{
        required = Fields,
        errors = Errors ++ NewErrors,
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
                nomatch -> add_error(CS, Field, ~"has invalid format")
            end;
        _ ->
            CS
    end.

-doc """
Validate length of `Field`. Opts: `{min, N}`, `{max, N}`, `{is, N}`.

A binary is measured in characters, counted as Unicode code points: the
unit PostgreSQL's `VARCHAR(N)` limit counts, so a value that passes always
fits the column. `~"日本語"` has length 3 and four emoji have length 4. A
binary that is not valid UTF-8 gets the error `is not valid UTF-8` instead
of a length check.

On a field typed `binary` or `{encrypted, binary}` the value is bytes, so it
is measured with `byte_size/1` and the messages say byte(s). A list is
measured by its length.
""".
-spec validate_length(#kura_changeset{}, atom(), [{atom(), non_neg_integer()}]) ->
    #kura_changeset{}.
validate_length(CS, Field, Opts) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val when is_binary(Val) ->
            binary_length(CS, Field, Val, Opts);
        Val when is_list(Val) ->
            Len = length(Val),
            check_length(CS, Field, Len, ~"character(s)", Opts);
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
                false -> add_error(CS, Field, ~"is invalid")
            end
    end.

-doc "Validate that `Field` value is NOT in the given list of `DisallowedValues`.".
-spec validate_exclusion(#kura_changeset{}, atom(), [term()]) -> #kura_changeset{}.
validate_exclusion(CS, Field, DisallowedValues) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Val ->
            case lists:member(Val, DisallowedValues) of
                true -> add_error(CS, Field, ~"is reserved");
                false -> CS
            end
    end.

-doc "Validate that all values in an array `Field` are within `AllowedValues`.".
-spec validate_subset(#kura_changeset{}, atom(), [term()]) -> #kura_changeset{}.
validate_subset(CS, Field, AllowedValues) ->
    case get_change(CS, Field) of
        undefined ->
            CS;
        Vals when is_list(Vals) ->
            case lists:all(fun(V) -> lists:member(V, AllowedValues) end, Vals) of
                true -> CS;
                false -> add_error(CS, Field, ~"has an invalid entry")
            end;
        _ ->
            add_error(CS, Field, ~"is invalid")
    end.

-doc """
Validate that the params repeat the change to `Field` under
`<Field>_confirmation`, as an atom or a binary key.

A mismatch is reported on `<Field>_confirmation` wherever an atom of that
name exists, and it exists in any application whose loaded code names the
confirmation: in a schema, in the view that shows the error, in the params.
kura does not create the atom, so where there is none the mismatch is
reported on `Field`. A changeset that has the confirmation as a field
always reports it there; declare it as a virtual field to be sure of it:

```erlang
#kura_field{name = password_confirmation, type = string, virtual = true}
```
""".
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
            Name = <<(atom_to_binary(Field, utf8))/binary, "_confirmation">>,
            case confirmation(Name, CS#kura_changeset.params) of
                {ok, Val} ->
                    CS;
                _Other ->
                    add_error(
                        CS,
                        confirmation_field(Name, Field),
                        maps:get(message, Opts, ~"does not match")
                    )
            end
    end.

-spec confirmation(binary(), map()) -> {ok, term()} | error.
confirmation(Name, Params) when is_map_key(Name, Params) ->
    maps:find(Name, Params);
confirmation(Name, Params) ->
    try binary_to_existing_atom(Name, utf8) of
        Key -> maps:find(Key, Params)
    catch
        error:badarg -> error
    end.

%% The name is never made into an atom: one that the application's code
%% names exists already, and one that only the params name must not.
-spec confirmation_field(binary(), atom()) -> atom().
confirmation_field(Name, Field) ->
    try
        binary_to_existing_atom(Name, utf8)
    catch
        error:badarg -> Field
    end.

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
    Msg = maps:get(message, Opts, ~"has already been taken"),
    C = #kura_constraint{type = unique, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]};
unique_constraint(CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts) ->
    Msg = maps:get(message, Opts, ~"has already been taken"),
    Declared = [
        #kura_constraint{type = unique, constraint = Name, field = Field, message = Msg}
     || Name <- constraint_names(SchemaMod, Field, ~"_key", Opts)
    ],
    CS#kura_changeset{constraints = Constraints ++ Declared}.

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
    Msg = maps:get(message, Opts, ~"does not exist"),
    C = #kura_constraint{type = foreign_key, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]};
foreign_key_constraint(
    CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts
) ->
    Msg = maps:get(message, Opts, ~"does not exist"),
    Declared = [
        #kura_constraint{type = foreign_key, constraint = Name, field = Field, message = Msg}
     || Name <- constraint_names(SchemaMod, Field, ~"_fkey", Opts)
    ],
    CS#kura_changeset{constraints = Constraints ++ Declared}.

-doc "Declare a check constraint with the given `Constraint` name mapped to `Field`.".
-spec check_constraint(#kura_changeset{}, binary(), atom()) -> #kura_changeset{}.
check_constraint(CS, Constraint, Field) ->
    check_constraint(CS, Constraint, Field, #{}).

-spec check_constraint(#kura_changeset{}, binary(), atom(), map()) -> #kura_changeset{}.
check_constraint(CS = #kura_changeset{constraints = Constraints}, Constraint, Field, Opts) ->
    Msg = maps:get(message, Opts, ~"is invalid"),
    C = #kura_constraint{type = check, constraint = Constraint, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]}.

-doc "Declare a PostgreSQL exclusion (`EXCLUDE`) constraint on `Field`.".
-spec exclusion_constraint(#kura_changeset{}, atom()) -> #kura_changeset{}.
exclusion_constraint(CS, Field) ->
    exclusion_constraint(CS, Field, #{}).

-spec exclusion_constraint(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
exclusion_constraint(#kura_changeset{schema = undefined}, _Field, Opts) when
    not is_map_key(name, Opts)
->
    error(
        {schemaless_constraint,
            "exclusion_constraint on a schemaless changeset requires :name in opts"}
    );
exclusion_constraint(
    CS = #kura_changeset{schema = undefined, constraints = Constraints}, Field, Opts
) ->
    Name = maps:get(name, Opts),
    Msg = maps:get(message, Opts, ~"violates an exclusion constraint"),
    C = #kura_constraint{type = exclusion, constraint = Name, field = Field, message = Msg},
    CS#kura_changeset{constraints = Constraints ++ [C]};
exclusion_constraint(
    CS = #kura_changeset{schema = SchemaMod, constraints = Constraints}, Field, Opts
) ->
    Msg = maps:get(message, Opts, ~"violates an exclusion constraint"),
    Declared = [
        #kura_constraint{type = exclusion, constraint = Name, field = Field, message = Msg}
     || Name <- constraint_names(SchemaMod, Field, ~"_excl", Opts)
    ],
    CS#kura_changeset{constraints = Constraints ++ Declared}.

-spec constraint_names(module(), atom(), binary(), map()) -> [binary()].
constraint_names(_SchemaMod, _Field, _Suffix, #{name := Name}) ->
    [Name];
constraint_names(SchemaMod, Field, Suffix, #{}) ->
    Table = SchemaMod:table(),
    ByField = <<Table/binary, "_", (atom_to_binary(Field, utf8))/binary, Suffix/binary>>,
    case kura_schema:renamed_columns(SchemaMod) of
        #{Field := Column} -> [<<Table/binary, "_", Column/binary, Suffix/binary>>, ByField];
        #{} -> [ByField]
    end.

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
get_field(ChangeSet, Field) ->
    get_field(ChangeSet, Field, undefined).

-spec get_field(#kura_changeset{}, atom(), term()) -> term().
get_field(#kura_changeset{changes = Changes, data = Data}, Field, Default) ->
    case Changes of
        #{Field := V} -> V;
        #{} -> maps:get(Field, Data, Default)
    end.

-doc "Put a change value directly, bypassing casting.".
-spec put_change(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_change(CS = #kura_changeset{changes = Changes}, Field, Value) ->
    CS#kura_changeset{changes = Changes#{Field => Value}}.

-doc """
Remove `Field` from the changeset's changes. Data, params and errors are
left as they are, as in Ecto's `delete_change/2`, so `get_field/2` falls
back to the field's value in the data. Removing a field that has no change
is a no-op.

Scrub a virtual secret once it has been used:

```erlang
hash_password(CS) ->
    case kura_changeset:get_change(CS, password) of
        undefined ->
            CS;
        Password ->
            CS1 = kura_changeset:put_change(CS, password_hash, hash(Password)),
            kura_changeset:delete_change(CS1, password)
    end.
```

The plain password is then not in `changes`, so neither `apply_changes/1`
nor a hook sees it. `params` still holds the raw input: do not log or
return the changeset itself.
""".
-spec delete_change(#kura_changeset{}, atom()) -> #kura_changeset{}.
delete_change(CS = #kura_changeset{changes = Changes}, Field) ->
    CS#kura_changeset{changes = maps:remove(Field, Changes)}.

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

-doc "Register a callback to run just before repo operations (insert/update).".
-spec prepare_changes(#kura_changeset{}, fun((#kura_changeset{}) -> #kura_changeset{})) ->
    #kura_changeset{}.
prepare_changes(CS = #kura_changeset{prepare = Prepare}, Fun) when is_function(Fun, 1) ->
    CS#kura_changeset{prepare = Prepare ++ [Fun]}.

-doc "Enable optimistic locking on `Field`, incrementing it on each update.".
-spec optimistic_lock(#kura_changeset{}, atom()) -> #kura_changeset{}.
optimistic_lock(CS = #kura_changeset{data = Data}, Field) ->
    Current = maps:get(Field, Data, 0),
    CS1 = put_change(CS, Field, Current + 1),
    CS1#kura_changeset{optimistic_lock = Field}.

-doc "Transform errors via a callback, returning `#{field => [transformed_messages]}`.".
-spec traverse_errors(#kura_changeset{}, fun((atom(), binary()) -> term())) -> map().
traverse_errors(#kura_changeset{errors = Errors}, Fun) ->
    ErrorMap = group_errors(Errors, #{}),
    maps:map(
        fun(Field, Messages) ->
            [Fun(Field, Msg) || Msg <- Messages]
        end,
        ErrorMap
    ).

%%----------------------------------------------------------------------
%% Association casting (delegated to kura_changeset_assoc)
%%----------------------------------------------------------------------

-doc """
Cast nested association parameters into child changesets, taking every
castable field of the child from the params. Prefer `cast_assoc/3` with
an allow-list; see it for why.
""".
-spec cast_assoc(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_assoc(CS, AssocName) ->
    kura_changeset_assoc:cast_assoc(CS, AssocName).

-doc """
Cast nested association parameters into child changesets.

`Opts` says which of the child's fields the params may set:

- `cast => [Field]` - only these fields, as `cast/4` would.
- `with => {Module, Function}` or `with => fun((Data, Params) -> Changeset)` -
  the child's own changeset function, called as
  `Module:Function(Data, Params)`. Use it to validate the child as well.
  It wins over `cast`.

```erlang
kura_changeset:cast_assoc(CS, comments, #{cast => [body]}).
kura_changeset:cast_assoc(CS, comments, #{with => {comment, changeset}}).
```

With neither, the params may set every non-virtual field of the child
except a single-column primary key and the foreign key. Nested params
usually arrive in a request, so that lets a request set fields such as
`approved` or `deleted_at`. Set `{kura, [{strict_assoc_cast, true}]}` to
make the allow-list mandatory: a nested cast without one then raises
`{kura, {nested_cast_needs_allow_list, Schema, Name}}`. Left unset, the
permissive default stays and kura logs one warning per boot; `false`
keeps it without the warning. The default becomes `true` in kura 3.0.

A `cast` list naming a field that cannot be cast (unknown, virtual, the
primary key or the foreign key) raises
`{kura, {invalid_nested_cast, Schema, Name, {cast, Fields}}}`.
""".
-spec cast_assoc(#kura_changeset{}, atom(), map()) -> #kura_changeset{}.
cast_assoc(CS, AssocName, Opts) ->
    kura_changeset_assoc:cast_assoc(CS, AssocName, Opts).

-doc """
Put pre-built changesets or raw maps directly as association changes. A
raw map is cast like nested params with no allow-list; see `put_assoc/4`.
""".
-spec put_assoc(#kura_changeset{}, atom(), term()) -> #kura_changeset{}.
put_assoc(CS, AssocName, Value) ->
    kura_changeset_assoc:put_assoc(CS, AssocName, Value).

-doc """
Put pre-built changesets or raw maps as association changes. `Opts` takes
the `cast` and `with` options of `cast_assoc/3` and applies them to the
raw maps; changesets are used as they are. With `strict_assoc_cast` set,
a raw map needs one of the two.
""".
-spec put_assoc(#kura_changeset{}, atom(), term(), map()) -> #kura_changeset{}.
put_assoc(CS, AssocName, Value, Opts) ->
    kura_changeset_assoc:put_assoc(CS, AssocName, Value, Opts).

%%----------------------------------------------------------------------
%% Embed casting (delegated to kura_changeset_assoc)
%%----------------------------------------------------------------------

-doc """
Cast nested embedded schema parameters into parent changes, taking every
non-virtual field of the embedded schema from the params. Prefer
`cast_embed/3` with an allow-list.
""".
-spec cast_embed(#kura_changeset{}, atom()) -> #kura_changeset{}.
cast_embed(CS, EmbedName) ->
    kura_changeset_assoc:cast_embed(CS, EmbedName).

-doc """
Cast nested embedded schema parameters into parent changes. `Opts` takes
the `cast` and `with` options of `cast_assoc/3`, and `strict_assoc_cast`
applies in the same way.
""".
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
    case {Params, Types} of
        {#{Field := Value}, #{Field := Type}} ->
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

-spec check_required_fields([atom()], map(), map()) -> [{atom(), binary()}].
check_required_fields([], _Changes, _Data) ->
    [];
check_required_fields([Field | Rest], Changes, Data) ->
    Val =
        case Changes of
            #{Field := V} -> V;
            #{} -> maps:get(Field, Data, undefined)
        end,
    Tail = check_required_fields(Rest, Changes, Data),
    case is_blank(Val) of
        true -> [{Field, ~"can't be blank"} | Tail];
        false -> Tail
    end.

-spec group_errors([{atom(), binary()}], #{atom() => [binary()]}) -> #{atom() => [binary()]}.
group_errors([], Acc) ->
    Acc;
group_errors([{F, M} | Rest], Acc) ->
    NewAcc =
        case Acc of
            #{F := Existing} -> Acc#{F := Existing ++ [M]};
            #{} -> Acc#{F => [M]}
        end,
    group_errors(Rest, NewAcc).

binary_length(#kura_changeset{types = Types} = CS, Field, Val, Opts) ->
    case Types of
        #{Field := binary} ->
            check_length(CS, Field, byte_size(Val), ~"byte(s)", Opts);
        #{Field := {encrypted, binary}} ->
            check_length(CS, Field, byte_size(Val), ~"byte(s)", Opts);
        #{} ->
            text_length(CS, Field, Val, Opts)
    end.

text_length(CS, Field, Val, Opts) ->
    case codepoints(Val, 0) of
        invalid -> add_error(CS, Field, ~"is not valid UTF-8");
        Len -> check_length(CS, Field, Len, ~"character(s)", Opts)
    end.

codepoints(<<_/utf8, Rest/binary>>, N) ->
    codepoints(Rest, N + 1);
codepoints(<<>>, N) ->
    N;
codepoints(_Invalid, _N) ->
    invalid.

check_length(CS, Field, Len, Unit, Opts) ->
    lists:foldl(
        fun
            ({min, Min}, Acc = #kura_changeset{}) when Len < Min ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be at least ~B ~ts", [Min, Unit])
                    )
                );
            ({max, Max}, Acc = #kura_changeset{}) when Len > Max ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be at most ~B ~ts", [Max, Unit])
                    )
                );
            ({is, Exact}, Acc = #kura_changeset{}) when Len =/= Exact ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("should be ~B ~ts", [Exact, Unit])
                    )
                );
            (_, Acc = #kura_changeset{}) ->
                Acc
        end,
        CS,
        Opts
    ).

check_number(CS, Field, Val, Opts) ->
    lists:foldl(
        fun
            ({greater_than, N}, Acc = #kura_changeset{}) when Val =< N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be greater than ~p", [N])
                    )
                );
            ({less_than, N}, Acc = #kura_changeset{}) when Val >= N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be less than ~p", [N])
                    )
                );
            ({greater_than_or_equal_to, N}, Acc = #kura_changeset{}) when Val < N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be greater than or equal to ~p", [N])
                    )
                );
            ({less_than_or_equal_to, N}, Acc = #kura_changeset{}) when Val > N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be less than or equal to ~p", [N])
                    )
                );
            ({equal_to, N}, Acc = #kura_changeset{}) when Val =/= N ->
                add_error(
                    Acc,
                    Field,
                    iolist_to_binary(
                        io_lib:format("must be equal to ~p", [N])
                    )
                );
            (_, Acc = #kura_changeset{}) ->
                Acc
        end,
        CS,
        Opts
    ).

%% An embedded schema declares no constraints or indexes and need not
%% export `table/0`, so the table is only looked up when there are some.
build_schema_constraints(SchemaMod) ->
    case {kura_schema:constraints(SchemaMod), kura_schema:indexes(SchemaMod)} of
        {[], []} ->
            [];
        {Constraints, Indexes} ->
            Table = schema_table(SchemaMod),
            build_table_constraints(Constraints, Table) ++
                build_index_constraints(Indexes, Table)
    end.

-spec schema_table(module()) -> binary().
schema_table(SchemaMod) -> SchemaMod:table().

-spec build_table_constraints([kura_migration:table_constraint()], binary()) ->
    [#kura_constraint{}].
build_table_constraints([], _Table) ->
    [];
build_table_constraints([{unique, [First | _] = Cols} | Rest], Table) ->
    ColsBin = join_col_names(Cols, ~"_"),
    Name = iolist_to_binary([Table, ~"_", ColsBin, ~"_key"]),
    [
        #kura_constraint{
            type = unique,
            constraint = Name,
            field = First,
            message = ~"has already been taken"
        }
        | build_table_constraints(Rest, Table)
    ];
build_table_constraints([{check, Expr} | Rest], Table) when is_binary(Expr) ->
    Name = iolist_to_binary([Table, ~"_check"]),
    [
        #kura_constraint{
            type = check,
            constraint = Name,
            field = base,
            message = ~"is invalid"
        }
        | build_table_constraints(Rest, Table)
    ];
build_table_constraints([_ | Rest], Table) ->
    build_table_constraints(Rest, Table).

-spec build_index_constraints([kura_migration:index_def()], binary()) -> [#kura_constraint{}].
build_index_constraints([], _Table) ->
    [];
build_index_constraints([{[First | _] = Cols, #{unique := true} = Opts} | Rest], Table) ->
    Declared = kura_migration:index_name(Table, Cols, Opts),
    Generated = kura_migration:index_name(Table, Cols),
    [
        #kura_constraint{
            type = unique,
            constraint = Name,
            field = First,
            message = ~"has already been taken"
        }
     || Name <- [Declared | [Generated || Generated =/= Declared]]
    ] ++ build_index_constraints(Rest, Table);
build_index_constraints([_ | Rest], Table) ->
    build_index_constraints(Rest, Table).

-spec join_col_names([atom()], binary()) -> binary().
join_col_names(Cols, Sep) ->
    Bins = [atom_to_binary(C, utf8) || C <- Cols],
    join_bins(Bins, Sep).

-spec join_bins([binary()], binary()) -> binary().
join_bins([], _Sep) -> <<>>;
join_bins([H], _Sep) -> H;
join_bins([H | T], Sep) -> <<H/binary, Sep/binary, (join_bins(T, Sep))/binary>>.
