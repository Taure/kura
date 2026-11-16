-module(kura_redact).
-moduledoc """
Keeps the values of sensitive fields out of logs and telemetry.

A schema names its sensitive fields with the optional `redact/0` callback.
Encrypted fields are always among them:

```erlang
-module(account).
-behaviour(kura_schema).
-export([table/0, fields/0, redact/0]).

redact() -> [password_hash, reset_token].
```

An embedded schema names its own the same way. An embed is stored and
logged as one value, so an embed whose schema lists a field, or embeds a
schema that does, is replaced whole wherever its parent is redacted.

`kura_schema:redacted_fields/1` returns the full list. Their values are
replaced by `~"[redacted]"`:

- in the `params` of the `[kura, repo, query]` telemetry event and of the
  query log, for a statement kura builds from a schema. Where the
  position of each value is known (an insert, an update or delete by key,
  an `insert_all`) only the redacted fields' values are replaced. For a
  query that filters on a redacted field, an `update_all` that sets one
  and an upsert, every value of the statement is replaced. A statement
  that touches no redacted field is shown as it is. A query is judged by
  every schema it reads: the one it is from, the ones it joins, and those
  of its subqueries, CTEs and combined queries. A condition anywhere in
  it counts as filtering on a redacted field when it names one of any of
  those schemas, so a plain column with the name of a redacted field of
  another source in the same query has its statement's values replaced
  too. A distance from a redacted vector field counts as a filter on it;
- in what `record/2` and `changeset/1` return, for your own logging.

The same event's `error_reason` keeps only the error's code and the names
it carries (constraint, table, column) for every statement, since a
database error message can quote a value, or the whole failing row. With
`{telemetry_error_detail, true}` in the `kura` application environment the
whole error is reported for a statement that reads or writes no schema
with redacted fields.

An insert or update of a changeset that fails with an error kura maps to
no field reports it on `base`: as `~"database error 22P02"`, the error's
code, for a schema with redacted fields, and with the database's message
for any other. What a query, `insert_all`, `update_all` or `delete_all`
returns as `{error, Reason}` is the driver's error as it is, message and
detail included: log its `code`, not the term.

Raw SQL through `kura_repo_worker:query/3` and `{fragment, SQL, Params}`
values inside a query that names no redacted field are not redacted: kura
cannot tell what they hold. Run raw SQL whose parameters are sensitive
with `kura_repo_worker:query/4` and `#{sensitive => true}`.
""".

-include("kura.hrl").

-export([
    record/2,
    changeset/1,
    query_params/2,
    mode/1,
    mode/2,
    shown/2,
    sensitive_params/1,
    update_all_params/3,
    write_params/3,
    error_reason/1
]).

%% A statement's parameters as telemetry may show them: the parameters
%% themselves for a statement on a schema with no redacted field, else
%% {redacted, Shown}. Neither type is exported: they are between kura's own
%% modules.
-type shown() :: [term()] | {redacted, [term()]}.

%% How the parameters of a query are shown: as they are (plain), as they
%% are but marked as a redacted schema's (redacted), or each replaced
%% (hidden).
-type mode() :: plain | redacted | hidden.

-define(REDACTED, ~"[redacted]").
-define(IS_CHANGESET(Term),
    (is_tuple(Term) andalso tuple_size(Term) =:= tuple_size(#kura_changeset{}) andalso
        element(1, Term) =:= kura_changeset)
).
-define(ERROR_KEYS, [code, severity, constraint, table, schema, column, data_type]).

-doc """
Return `Record`, a map of `Schema`'s fields, with the value of each
redacted field replaced by `~"[redacted]"`. Safe to log.

The records an association of `Schema` holds, such as preloaded ones, are
redacted by the association's schema. A key is a field's name as an atom,
a binary or a string; the value under any other key is replaced, since
kura cannot tell which field it is.

Never raises. Where `Record` is not a map, `Schema` is not a schema, or
its `redact/0` does not return a list of fields, the result is
`#{base => ~"[redacted]"}`. A schema with no redacted field and no
association has nothing to hide, so its record is returned as it is,
whatever its keys.
""".
-spec record(module(), map()) -> map().
record(Schema, Record) ->
    shown_map(hidden_in(Schema), Record).

-doc """
Return `Changeset` safe to log; not for writing.

The redacted fields' values are replaced in its `data`, `changes` and
`params`, as `record/2` replaces them, and so is every error message that
holds one of those values: a message that quotes a redacted field's text
or integer, as a database's `invalid input syntax` does. The changesets
in `assoc_changes` are redacted the same way.

```erlang
[{base, ~"invalid input syntax for type uuid: \"tok-SECRET\""}] = CS#kura_changeset.errors,
[{base, ~"[redacted]"}] = (kura_redact:changeset(CS))#kura_changeset.errors.
```

`params` are what the caller was sent, under the keys it was sent with.
For a schema with redacted fields, a value in `params` is shown only under
a key that names a field, an embed or an association of the schema, as an
atom, a binary or a string. The value under any other key is replaced and
the key is kept, since a confirmation such as `~"password_confirmation"`
or a mistyped name can hold what a redacted field holds. A confirmation
declared as a virtual field is a field: list it in `redact/0` beside the
field it confirms. The params of an association are read the same way, by
its schema.

```erlang
#{~"email" := ~"a@b.c", ~"password" := ~"[redacted]", ~"password_confirmation" := ~"[redacted]"} =
    (kura_redact:changeset(CS))#kura_changeset.params.
```

Never raises, whatever the changeset holds. What kura cannot read is
replaced, never passed through:

- an error message that is not text (a binary, a string or an atom), and
  every message when `data`, `changes` or `params` is not a map, becomes
  `~"[redacted]"`. A message that is text is returned as a binary;
- an error that is not a `{Field, Message}` tuple with an atom for a field
  becomes `{Field, ~"[redacted]"}` when it starts with one, and
  `{base, ~"[redacted]"}` when it does not;
- a `data`, `changes` or `params` that is not a map becomes
  `#{base => ~"[redacted]"}`;
- an entry of `assoc_changes` that is not a changeset, a term that is not
  a changeset at all, and a changeset whose schema is not a schema or has
  a `redact/0` that does not return a list of fields, become an invalid
  changeset whose only error is `{base, ~"[redacted]"}`.

That is for a schema with redacted fields or with associations. A
changeset without a schema, or of a schema that has neither, has nothing
redacted or replaced but its `assoc_changes`.
""".
-spec changeset(#kura_changeset{}) -> #kura_changeset{}.
changeset(Changeset) ->
    shown_changeset(Changeset).

%% The fields are read from the tuple, not through the record: a record
%% pattern promises the field types the record declares, and what a
%% changeset from outside holds in a field is whatever was put there.
-spec shown_changeset(term()) -> #kura_changeset{}.
shown_changeset(Changeset) when ?IS_CHANGESET(Changeset) ->
    case element(#kura_changeset.schema, Changeset) of
        undefined -> with_shown_assocs(Changeset);
        Schema -> shown_changeset(Changeset, hidden_in(Schema))
    end;
shown_changeset(_NotAChangeset) ->
    hidden_changeset().

-spec shown_changeset(tuple(), hidden() | unknown) -> #kura_changeset{}.
shown_changeset(_Changeset, unknown) ->
    hidden_changeset();
shown_changeset(Changeset, {[], Nested}) when map_size(Nested) =:= 0 ->
    with_shown_assocs(Changeset);
shown_changeset(Changeset, Hidden) ->
    Data = element(#kura_changeset.data, Changeset),
    Changes = element(#kura_changeset.changes, Changeset),
    Params = element(#kura_changeset.params, Changeset),
    Errors = element(#kura_changeset.errors, Changeset),
    case with_shown_assocs(Changeset) of
        #kura_changeset{} = Shown ->
            Shown#kura_changeset{
                data = shown_map(Hidden, Data),
                changes = shown_map(Hidden, Changes),
                params = shown_params(element(#kura_changeset.schema, Changeset), Hidden, Params),
                errors = shown_errors(Errors, quotable([Data, Changes, Params], Hidden))
            }
    end.

-spec with_shown_assocs(tuple()) -> #kura_changeset{}.
with_shown_assocs(Changeset) ->
    Assocs = shown_assocs(element(#kura_changeset.assoc_changes, Changeset)),
    case Changeset of
        #kura_changeset{} = CS -> CS#kura_changeset{assoc_changes = Assocs};
        _NotAChangeset -> hidden_changeset()
    end.

-spec hidden_changeset() -> #kura_changeset{}.
hidden_changeset() ->
    #kura_changeset{valid = false, errors = [{base, ?REDACTED}]}.

-spec shown_assocs(term()) -> #{atom() => #kura_changeset{} | [#kura_changeset{}]}.
shown_assocs(Assocs) when is_map(Assocs) ->
    #{assoc_name(Name) => shown_assoc(Value) || Name := Value <- Assocs};
shown_assocs(_NotAMap) ->
    #{base => hidden_changeset()}.

-spec assoc_name(term()) -> atom().
assoc_name(Name) when is_atom(Name) -> Name;
assoc_name(_Name) -> base.

-spec shown_assoc(term()) -> #kura_changeset{} | [#kura_changeset{}].
shown_assoc([Changeset | Rest]) ->
    case shown_assoc(Rest) of
        Shown when is_list(Shown) -> [shown_changeset(Changeset) | Shown];
        Shown -> [shown_changeset(Changeset), Shown]
    end;
shown_assoc([]) ->
    [];
shown_assoc(Changeset) ->
    shown_changeset(Changeset).

%% What is hidden in a map of a schema's fields: its redacted fields, and
%% the schema of each association, whose records are redacted by it.
%% `unknown` when the schema cannot say, or the name is no schema, so that
%% everything is hidden.
-type hidden() :: {[atom()], #{atom() => module()}}.

-spec hidden_in(term()) -> hidden() | unknown.
hidden_in(Schema) when is_atom(Schema) ->
    case kura_schema:is_schema(Schema) andalso redacted(Schema) of
        false -> unknown;
        unknown -> unknown;
        Redacted -> hidden_in(Schema, Redacted)
    end;
hidden_in(_Schema) ->
    unknown.

-spec hidden_in(module(), [atom()]) -> hidden() | unknown.
hidden_in(Schema, Redacted) ->
    try
        {Redacted,
            #{
                Assoc#kura_assoc.name => Target
             || #kura_assoc{} = Assoc <- kura_schema:associations(Schema),
                Target <- [kura_schema:assoc_target(Assoc)],
                is_atom(Target),
                Target =/= undefined
            }}
    catch
        _:_ -> unknown
    end.

%% The redacted fields of a schema, `[]` for a name that is no schema, and
%% `unknown` when what its redact/0 returns is not a list of fields.
-spec redacted(atom()) -> [atom()] | unknown.
redacted(Schema) ->
    try kura_schema:redacted_fields(Schema) of
        Fields ->
            case all_atoms(Fields) of
                true -> Fields;
                false -> unknown
            end
    catch
        _:_ -> unknown
    end.

-spec all_atoms(term()) -> boolean().
all_atoms([Field | Fields]) when is_atom(Field) -> all_atoms(Fields);
all_atoms(Fields) -> Fields =:= [].

-spec shown_map(hidden() | unknown, term()) -> map().
shown_map({[], Nested}, Map) when is_map(Map), map_size(Nested) =:= 0 ->
    Map;
shown_map({_Redacted, _Nested} = Hidden, Map) when is_map(Map) ->
    #{Key => shown_value(fields, entry(Key, Hidden), Value) || Key := Value <- Map};
shown_map(_Hidden, _Map) ->
    hidden_map().

%% Params are what a caller was sent, under the keys it was sent with. In
%% a schema with redacted fields a key that names none of its fields can
%% hold what a redacted field holds, as a confirmation or a mistyped name
%% does, so its value is hidden.
-spec shown_params(term(), hidden() | unknown, term()) -> map().
shown_params(Schema, {[_ | _], _Nested} = Hidden, Params) when is_atom(Schema), is_map(Params) ->
    case param_names(Schema, Hidden) of
        unknown ->
            hidden_map();
        Names ->
            #{
                Key => shown_value(params, param(Key, Names, Hidden), Value)
             || Key := Value <- Params
            }
    end;
shown_params(_Schema, {[], Nested}, Params) when is_map(Params), map_size(Nested) =:= 0 ->
    Params;
shown_params(_Schema, {[], _Nested} = Hidden, Params) when is_map(Params) ->
    #{Key => shown_value(params, entry(Key, Hidden), Value) || Key := Value <- Params};
shown_params(_Schema, _Hidden, _Params) ->
    hidden_map().

%% Every name a key of a schema's params can be: its fields, virtual ones
%% included, its embeds and its associations.
-spec param_names(module(), hidden()) -> [atom()] | unknown.
param_names(Schema, {Redacted, Nested}) ->
    try
        kura_schema:field_names(Schema) ++
            [Name || #kura_embed{name = Name} <- kura_schema:embeds(Schema)]
    of
        Names ->
            case all_atoms(Names) of
                true -> Names ++ Redacted ++ maps:keys(Nested);
                false -> unknown
            end
    catch
        _:_ -> unknown
    end.

-spec param(term(), [atom()], hidden()) -> hide | show | {nested, module()}.
param(Key, Names, Hidden) ->
    case named(Key, Names) of
        {ok, Name} -> entry(Name, Hidden);
        error -> hide
    end.

-spec named(term(), [atom()]) -> {ok, atom()} | error.
named(Key, Names) when is_atom(Key) ->
    case lists:member(Key, Names) of
        true -> {ok, Key};
        false -> error
    end;
named(Key, Names) when is_binary(Key) ->
    case [Name || Name <- Names, atom_to_binary(Name, utf8) =:= Key] of
        [Name | _] -> {ok, Name};
        [] -> error
    end;
named(Key, Names) when is_list(Key) ->
    case text(Key) of
        {ok, Text} -> named(Text, Names);
        error -> error
    end;
named(_Key, _Names) ->
    error.

-spec hidden_map() -> map().
hidden_map() ->
    #{base => ?REDACTED}.

%% Whether a map holds a record's fields or a changeset's params, which
%% decides how the maps nested in it are read.
-type holds() :: fields | params.

-spec shown_value(holds(), hide | show | {nested, module()}, term()) -> term().
shown_value(_Holds, hide, _Value) -> ?REDACTED;
shown_value(_Holds, show, Value) -> Value;
shown_value(Holds, {nested, Target}, Value) -> shown_nested(Holds, Target, Value).

-spec shown_nested(holds(), module(), term()) -> term().
shown_nested(fields, Target, Value) when is_map(Value) ->
    shown_map(hidden_in(Target), Value);
shown_nested(params, Target, Value) when is_map(Value) ->
    shown_params(Target, hidden_in(Target), Value);
shown_nested(Holds, Target, [Value | Rest]) ->
    [shown_nested(Holds, Target, Value) | nested_rest(Holds, Target, Rest)];
shown_nested(_Holds, _Target, #kura_changeset{} = Changeset) ->
    changeset(Changeset);
shown_nested(_Holds, _Target, Value) when is_atom(Value); Value =:= [] ->
    Value;
shown_nested(_Holds, _Target, _Value) ->
    ?REDACTED.

-spec nested_rest(holds(), module(), term()) -> [term()].
nested_rest(Holds, Target, [Value | Rest]) ->
    [shown_nested(Holds, Target, Value) | nested_rest(Holds, Target, Rest)];
nested_rest(_Holds, _Target, []) ->
    [];
nested_rest(_Holds, _Target, _Tail) ->
    [?REDACTED].

%% What a key of a map of fields says of its value: a redacted field's is
%% hidden, an association's is its records, any other field's is shown. A
%% key that is no name cannot say, so its value is hidden.
-spec entry(term(), hidden()) -> hide | show | {nested, module()}.
entry(Key, {Redacted, Nested}) when is_atom(Key) ->
    case lists:member(Key, Redacted) of
        true ->
            hide;
        false ->
            case Nested of
                #{Key := Target} -> {nested, Target};
                #{} -> show
            end
    end;
entry(Key, {Redacted, Nested} = Hidden) when is_binary(Key) ->
    case [Name || Name <- Redacted ++ maps:keys(Nested), atom_to_binary(Name, utf8) =:= Key] of
        [Name | _] -> entry(Name, Hidden);
        [] -> show
    end;
entry(Key, Hidden) when is_list(Key) ->
    case text(Key) of
        {ok, Name} -> entry(Name, Hidden);
        error -> hide
    end;
entry(_Key, _Hidden) ->
    hide.

%% The text of every value hidden in the maps, as an error message would
%% quote it. `unknown` when a map is not one: its values cannot be told.
-spec quotable([term()], hidden()) -> [binary()] | unknown.
quotable(Maps, Hidden) ->
    case [Map || Map <- Maps, not is_map(Map)] of
        [] ->
            lists:usort([
                Text
             || Map <- Maps, Value <- hidden_values(Hidden, Map), Text <- texts(Value)
            ]);
        _NotAMap ->
            unknown
    end.

-spec hidden_values(hidden() | unknown, term()) -> [term()].
hidden_values({_Redacted, _Nested} = Hidden, Map) when is_map(Map) ->
    lists:append([hidden_value(entry(Key, Hidden), Value) || Key := Value <- Map]);
hidden_values(_Hidden, Map) ->
    [Map].

-spec hidden_value(hide | show | {nested, module()}, term()) -> [term()].
hidden_value(hide, Value) -> [Value];
hidden_value(show, _Value) -> [];
hidden_value({nested, Target}, Value) -> hidden_nested(Target, Value).

-spec hidden_nested(module(), term()) -> [term()].
hidden_nested(Target, Value) when is_map(Value) ->
    hidden_values(hidden_in(Target), Value);
hidden_nested(Target, [Value | Rest]) ->
    hidden_nested(Target, Value) ++ hidden_nested(Target, Rest);
hidden_nested(_Target, Value) when is_atom(Value); Value =:= [] ->
    [];
hidden_nested(_Target, Value) ->
    [Value].

%% Every text and integer in a value, at any depth: an error message can
%% quote an element of a list as it can quote the whole. A list of integers
%% that reads as text is taken both ways.
-spec texts(term()) -> [binary()].
texts(<<>>) ->
    [];
texts(Value) when is_binary(Value) ->
    [Value];
texts(Value) when is_integer(Value) ->
    [integer_to_binary(Value)];
texts(Value) when is_list(Value) ->
    case text(Value) of
        {ok, <<>>} -> [];
        {ok, Text} -> [Text | element_texts(Value)];
        error -> element_texts(Value)
    end;
texts(Value) when is_tuple(Value) ->
    element_texts(tuple_to_list(Value));
texts(Value) when is_map(Value) ->
    element_texts(maps:keys(Value)) ++ element_texts(maps:values(Value));
texts(_Value) ->
    [].

-spec element_texts(term()) -> [binary()].
element_texts([Value | Rest]) -> texts(Value) ++ element_texts(Rest);
element_texts(Tail) -> texts(Tail).

-spec shown_errors(term(), [binary()] | unknown) -> [{atom(), binary()}].
shown_errors([Error | Rest], Quotable) ->
    [shown_error(Error, Quotable) | shown_errors(Rest, Quotable)];
shown_errors([], _Quotable) ->
    [];
shown_errors(_NotAList, _Quotable) ->
    [{base, ?REDACTED}].

-spec shown_error(term(), [binary()] | unknown) -> {atom(), binary()}.
shown_error({Field, Message}, Quotable) when is_atom(Field) ->
    {Field, shown_message(Message, Quotable)};
shown_error(Error, _Quotable) when is_tuple(Error), tuple_size(Error) >= 1 ->
    case element(1, Error) of
        Field when is_atom(Field) -> {Field, ?REDACTED};
        _NotAField -> {base, ?REDACTED}
    end;
shown_error(_Error, _Quotable) ->
    {base, ?REDACTED}.

-spec shown_message(term(), [binary()] | unknown) -> binary().
shown_message(_Message, unknown) ->
    ?REDACTED;
shown_message(Message, Quotable) ->
    case message_text(Message) of
        {ok, Text} when Quotable =:= [] ->
            Text;
        {ok, Text} ->
            case binary:match(Text, Quotable) of
                nomatch -> Text;
                _Found -> ?REDACTED
            end;
        error ->
            ?REDACTED
    end.

-spec message_text(term()) -> {ok, binary()} | error.
message_text(Message) when is_atom(Message) -> {ok, atom_to_binary(Message, utf8)};
message_text(Message) -> text(Message).

%% Never raises: a raise here would carry the redacted values in its
%% stacktrace.
-spec text(term()) -> {ok, binary()} | error.
text(Text) when is_binary(Text) ->
    {ok, Text};
text(Chars) when is_list(Chars) ->
    try chardata(lists:flatten(Chars)) of
        {ok, Flat} -> encoded(unicode:characters_to_binary(Flat));
        error -> error
    catch
        _:_ -> error
    end;
text(_NotText) ->
    error.

-spec chardata([term()]) -> {ok, [integer() | binary()]} | error.
chardata([Char | Rest]) when is_integer(Char); is_binary(Char) ->
    case chardata(Rest) of
        {ok, Chars} -> {ok, [Char | Chars]};
        error -> error
    end;
chardata([]) ->
    {ok, []};
chardata(_NotText) ->
    error.

-spec encoded(term()) -> {ok, binary()} | error.
encoded(Text) when is_binary(Text) -> {ok, Text};
encoded(_Incomplete) -> error.

%% What telemetry may show of the parameters of a compiled `Query`: `Params`
%% itself when no schema it reads has a redacted field, else
%% {redacted, Shown}. `Shown` holds one ~"[redacted]" per parameter when the
%% query, or a query nested in it, filters on a redacted field, and is
%% `Params` otherwise.
%%
%% This and the functions below never raise: a raise would carry the
%% parameters in its stacktrace. What cannot be read counts as redacted.
-doc false.
-spec query_params(#kura_query{}, [term()]) -> shown().
query_params(Query, Params) ->
    shown(mode(Query), Params).

%% How the parameters of any query of this shape are shown. It depends on
%% the query's sources and on which fields its conditions name, never on a
%% value, so it is worked out once for a cached shape.
-doc false.
-spec mode(#kura_query{}) -> mode().
mode(Query) ->
    try kura_query:sources(Query) of
        Sources -> mode(Query, Sources)
    catch
        _:_ -> hidden
    end.

%% mode/1 for a caller that has the query's sources already.
-doc false.
-spec mode(#kura_query{}, [kura_query:source()]) -> mode().
mode(Query, Sources) ->
    try redacted_read(Sources) of
        unknown ->
            hidden;
        [] ->
            plain;
        Redacted ->
            case names_anywhere(Query, Redacted) of
                true -> hidden;
                false -> redacted
            end
    catch
        _:_ -> hidden
    end.

-doc false.
-spec shown(mode(), [term()]) -> shown().
shown(plain, Params) when is_list(Params) -> Params;
shown(redacted, Params) when is_list(Params) -> {redacted, Params};
shown(_Mode, Params) -> {redacted, all_redacted(Params)}.

%% What telemetry may show of parameters the caller says are sensitive: one
%% ~"[redacted]" for each.
-doc false.
-spec sensitive_params([term()]) -> shown().
sensitive_params(Params) ->
    {redacted, all_redacted(Params)}.

%% What telemetry may show of the parameters of an update_all of `Query`
%% that sets `SetFields`. As query_params/2, and every parameter is replaced
%% when a set field is redacted too.
-doc false.
-spec update_all_params(#kura_query{}, [atom()], [term()]) -> shown().
update_all_params(#kura_query{from = From} = Query, SetFields, Params) ->
    case redacted_of(From) of
        [] ->
            query_params(Query, Params);
        unknown ->
            sensitive_params(Params);
        Redacted ->
            case touches(SetFields, Redacted) orelse mode(Query) =:= hidden of
                true -> sensitive_params(Params);
                false -> shown(redacted, Params)
            end
    end;
update_all_params(_NotAQuery, _SetFields, Params) ->
    sensitive_params(Params).

%% What telemetry may show of the parameters of a write to `Schema`:
%% `Params` itself when the schema has no redacted field, else
%% {redacted, Shown}. `Fields` says what `Params` hold: a list of fields
%% when the parameters are those fields' values in that order, once or once
%% per row, or {touches, Fields} when the statement binds values for those
%% fields somewhere among its parameters.
-doc false.
-spec write_params(kura_dialect:table(), [atom()] | {touches, [atom()]}, [term()]) -> shown().
write_params(Schema, Fields, Params) when is_atom(Schema) ->
    case redacted(Schema) of
        [] when is_list(Params) -> Params;
        unknown -> sensitive_params(Params);
        Redacted -> {redacted, shown(Fields, Redacted, Params)}
    end;
write_params(Table, _Fields, Params) when is_binary(Table), is_list(Params) ->
    Params;
write_params(_Table, _Fields, Params) ->
    sensitive_params(Params).

shown({touches, Fields}, Redacted, Params) when is_list(Params) ->
    case touches(Fields, Redacted) of
        true -> all_redacted(Params);
        false -> Params
    end;
shown([_ | _] = Fields, Redacted, Params) when length(Params) rem length(Fields) =:= 0 ->
    by_position(Params, Fields, Fields, Redacted);
shown(_Fields, _Redacted, Params) ->
    all_redacted(Params).

%% Fields that are no list of fields count as touching a redacted one.
touches([Field | Fields], Redacted) ->
    lists:member(Field, Redacted) orelse touches(Fields, Redacted);
touches([], _Redacted) ->
    false;
touches(_NotAList, _Redacted) ->
    true.

by_position([], _Fields, _AllFields, _Redacted) ->
    [];
by_position(Params, [], AllFields, Redacted) ->
    by_position(Params, AllFields, AllFields, Redacted);
by_position([Value | Params], [Field | Fields], AllFields, Redacted) ->
    [positioned(Field, Value, Redacted) | by_position(Params, Fields, AllFields, Redacted)].

positioned(Field, Value, Redacted) when is_atom(Field) ->
    case lists:member(Field, Redacted) of
        true -> ?REDACTED;
        false -> Value
    end;
positioned(_Field, _Value, _Redacted) ->
    ?REDACTED.

%% One ~"[redacted]" for each parameter, and one for what is not a list or
%% follows a list that does not end.
all_redacted([_ | Params]) -> [?REDACTED | all_redacted(Params)];
all_redacted([]) -> [];
all_redacted(_NotAList) -> [?REDACTED].

%% What telemetry may show of the error of a failed statement: the error's
%% code and the names in it, never its message or detail, which can quote a
%% value.
-doc false.
-spec error_reason(term()) -> term().
error_reason(Reason) when is_map(Reason) -> maps:with(?ERROR_KEYS, Reason);
error_reason(Reason) when is_atom(Reason) -> Reason;
error_reason({Tag, Reason}) when is_atom(Tag), is_map(Reason) -> {Tag, error_reason(Reason)};
error_reason(_Reason) -> redacted.

%% The redacted fields of every source the query reads, at any depth. A
%% condition is judged against all of them and not only those of its own
%% level: the database resolves a name a subquery's sources lack to the
%% outer query's column, and a CTE hands its columns to whatever reads it.
%% `unknown` when one of them cannot say what it redacts.
-spec redacted_read([kura_query:source()]) -> [atom()] | unknown.
redacted_read(Sources) ->
    Read = [redacted_of(Source) || {Role, Source} <- Sources, Role =/= preload],
    case lists:member(unknown, Read) of
        true -> unknown;
        false -> lists:usort([Field || Redacted <- Read, is_list(Redacted), Field <- Redacted])
    end.

names_anywhere(
    #kura_query{wheres = Wheres, havings = Havings, order_bys = Orders, select = Select} = Query,
    Redacted
) ->
    names_field(Wheres, Redacted) orelse
        names_field(Havings, Redacted) orelse
        measures(Orders, Redacted) orelse
        measures(selected(Select), Redacted) orelse
        nested_name(kura_query:nested(Query), Redacted).

nested_name([], _Redacted) ->
    false;
nested_name([Query | Rest], Redacted) ->
    names_anywhere(Query, Redacted) orelse nested_name(Rest, Redacted).

selected({exprs, Exprs}) when is_list(Exprs) -> Exprs;
selected(_Select) -> [].

%% A distance binds the vector it measures from, which says as much about a
%% redacted vector as a filter's value does.
measures([{_AliasOrExpr, {distance, Field, _Metric, _Vector}} | Rest], Redacted) ->
    lists:member(Field, Redacted) orelse measures(Rest, Redacted);
measures([{{distance, Field, _Metric, _Vector}, _Direction} | Rest], Redacted) ->
    lists:member(Field, Redacted) orelse measures(Rest, Redacted);
measures([_ | Rest], Redacted) ->
    measures(Rest, Redacted);
measures(_, _Redacted) ->
    false.

-spec redacted_of(term()) -> [atom()] | unknown.
redacted_of(From) when is_atom(From), From =/= undefined -> redacted(From);
redacted_of(_From) -> [].

names_field([Condition | Rest], Redacted) ->
    condition_names(Condition, Redacted) orelse names_field(Rest, Redacted);
names_field(_, _Redacted) ->
    false.

condition_names({Op, Conditions}, Redacted) when Op =:= 'and'; Op =:= 'or' ->
    names_field(Conditions, Redacted);
condition_names({'not', Inner}, Redacted) ->
    condition_names(Inner, Redacted);
condition_names(Condition, Redacted) when is_tuple(Condition), tuple_size(Condition) >= 2 ->
    case element(1, Condition) of
        Field when is_atom(Field) -> lists:member(Field, Redacted);
        Fields when is_list(Fields) -> touches(Fields, Redacted);
        {distance, Field, _Metric, _Vector} -> lists:member(Field, Redacted);
        _ -> false
    end;
condition_names(_Condition, _Redacted) ->
    false.
