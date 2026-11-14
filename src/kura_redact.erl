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

`kura_schema:redacted_fields/1` returns the full list. Their values are
replaced by `~"[redacted]"`:

- in the `params` of the `[kura, repo, query]` telemetry event and of the
  query log, for a statement kura builds from a schema. Where the
  position of each value is known (an insert, an update or delete by key,
  an `insert_all`) only the redacted fields' values are replaced. For a
  query that filters on a redacted field, an `update_all` that sets one
  and an upsert, every value of the statement is replaced. A statement
  that touches no redacted field is shown as it is. A query is judged by
  the schema it reads from: a subquery over another schema is looked at
  only when the outer schema has redacted fields of its own, or the query
  has a CTE or a combination;
- in the same event's `error_reason`, which for a statement on a schema
  with redacted fields keeps only the error's code and the names it
  carries (constraint, table, column), since a database error message can
  quote a value, or the whole failing row;
- in what `record/2` and `changeset/1` return, for your own logging.

Raw SQL through a repo's `query/2` and `{fragment, SQL, Params}` values
inside a query that names no redacted field are not redacted: kura cannot
tell what they hold.
""".

-include("kura.hrl").

-export([
    record/2,
    changeset/1,
    query_params/2,
    update_all_params/3,
    write_params/3,
    error_reason/1
]).

-export_type([shown/0]).

-doc """
A statement's parameters as telemetry may show them: the parameters
themselves for a statement on a schema with no redacted field, else
`{redacted, Shown}`.
""".
-type shown() :: [term()] | {redacted, [term()]}.

-define(REDACTED, ~"[redacted]").
-define(ERROR_KEYS, [code, severity, constraint, table, schema, column, data_type]).

-doc """
Return `Record`, a map of `Schema`'s fields, with the value of each
redacted field replaced by `~"[redacted]"`. Safe to log.
""".
-spec record(module(), map()) -> map().
record(Schema, Record) when is_map(Record) ->
    case kura_schema:redacted_fields(Schema) of
        [] -> Record;
        Redacted -> redacted_map(Record, Redacted)
    end.

-doc """
Return `Changeset` with the redacted fields' values replaced in its
`data`, `changes` and `params`. Safe to log; not for writing.
""".
-spec changeset(#kura_changeset{}) -> #kura_changeset{}.
changeset(#kura_changeset{schema = undefined} = CS) ->
    CS;
changeset(#kura_changeset{schema = Schema} = CS) ->
    case kura_schema:redacted_fields(Schema) of
        [] ->
            CS;
        Redacted ->
            CS#kura_changeset{
                data = redacted_map(CS#kura_changeset.data, Redacted),
                changes = redacted_map(CS#kura_changeset.changes, Redacted),
                params = redacted_map(CS#kura_changeset.params, Redacted)
            }
    end.

redacted_map(Map, Redacted) ->
    #{K => redacted_value(K, V, Redacted) || K := V <- Map}.

redacted_value(Key, Value, Redacted) ->
    case names_redacted(Key, Redacted) of
        true -> ?REDACTED;
        false -> Value
    end.

names_redacted(Key, Redacted) when is_atom(Key) ->
    lists:member(Key, Redacted);
names_redacted(Key, Redacted) when is_binary(Key) ->
    lists:any(fun(Field) -> atom_to_binary(Field, utf8) =:= Key end, Redacted);
names_redacted(_Key, _Redacted) ->
    false.

-doc """
What telemetry may show of the parameters of a compiled `Query`: `Params`
itself when the schema it reads from has no redacted field, else
`{redacted, Shown}`. `Shown` holds one `~"[redacted]"` per parameter when
the query, or a query nested in it, filters on a redacted field, and is
`Params` otherwise.
""".
-spec query_params(#kura_query{}, [term()]) -> shown().
query_params(#kura_query{from = From, ctes = [], combinations = []} = Query, Params) ->
    case redacted_of(From) of
        [] -> Params;
        _ -> {redacted, filtered_params(Query, Params)}
    end;
query_params(Query, Params) ->
    {redacted, filtered_params(Query, Params)}.

filtered_params(Query, Params) ->
    case filters_redacted(Query) of
        true -> all_redacted(Params);
        false -> Params
    end.

-doc """
What telemetry may show of the parameters of an `update_all` of `Query`
that sets `SetFields`. As `query_params/2`, and every parameter is
replaced when a set field is redacted too.
""".
-spec update_all_params(#kura_query{}, [atom()], [term()]) -> shown().
update_all_params(#kura_query{from = From} = Query, SetFields, Params) ->
    case redacted_of(From) of
        [] ->
            query_params(Query, Params);
        Redacted ->
            case any_member(SetFields, Redacted) of
                true -> {redacted, all_redacted(Params)};
                false -> {redacted, filtered_params(Query, Params)}
            end
    end.

-doc """
What telemetry may show of the parameters of a write to `Schema`:
`Params` itself when the schema has no redacted field, else
`{redacted, Shown}`. `Fields` says what `Params` hold: a list of fields
when the parameters are those fields' values in that order, once or once
per row, or `{touches, Fields}` when the statement binds values for those
fields somewhere among its parameters.
""".
-spec write_params(kura_dialect:table(), [atom()] | {touches, [atom()]}, [term()]) -> shown().
write_params(Schema, Fields, Params) when is_atom(Schema) ->
    case kura_schema:redacted_fields(Schema) of
        [] -> Params;
        Redacted -> {redacted, shown(Fields, Redacted, Params)}
    end;
write_params(_Table, _Fields, Params) ->
    Params.

shown({touches, Fields}, Redacted, Params) ->
    case any_member(Fields, Redacted) of
        true -> all_redacted(Params);
        false -> Params
    end;
shown([], _Redacted, Params) ->
    all_redacted(Params);
shown(Fields, Redacted, Params) ->
    case length(Params) rem length(Fields) of
        0 -> by_position(Params, Fields, Fields, Redacted);
        _ -> all_redacted(Params)
    end.

any_member(Fields, Redacted) ->
    lists:any(fun(Field) -> lists:member(Field, Redacted) end, Fields).

by_position([], _Fields, _AllFields, _Redacted) ->
    [];
by_position(Params, [], AllFields, Redacted) ->
    by_position(Params, AllFields, AllFields, Redacted);
by_position([Value | Params], [Field | Fields], AllFields, Redacted) ->
    [redacted_value(Field, Value, Redacted) | by_position(Params, Fields, AllFields, Redacted)].

all_redacted(Params) ->
    [?REDACTED || _ <- Params].

-doc """
What telemetry may show of the error of a failed statement on a schema
with redacted fields: the error's code and the names in it, never its
message or detail, which can quote a value.
""".
-spec error_reason(term()) -> term().
error_reason(Reason) when is_map(Reason) -> maps:with(?ERROR_KEYS, Reason);
error_reason(Reason) when is_atom(Reason) -> Reason;
error_reason({Tag, Reason}) when is_atom(Tag), is_map(Reason) -> {Tag, error_reason(Reason)};
error_reason(_Reason) -> redacted.

filters_redacted(#kura_query{from = From} = Query) ->
    names_field(Query#kura_query.wheres, redacted_of(From)) orelse
        names_field(Query#kura_query.havings, redacted_of(From)) orelse
        any_body(Query#kura_query.ctes) orelse
        any_body(Query#kura_query.combinations) orelse
        any_subquery(Query#kura_query.wheres) orelse
        any_subquery(Query#kura_query.havings).

redacted_of(From) when is_atom(From), From =/= undefined -> kura_schema:redacted_fields(From);
redacted_of(_From) -> [].

names_field(_Conditions, []) ->
    false;
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
        Fields when is_list(Fields) -> any_member(Fields, Redacted);
        _ -> false
    end;
condition_names(_Condition, _Redacted) ->
    false.

any_body([{_NameOrKind, #kura_query{} = Body} | Rest]) ->
    filters_redacted(Body) orelse any_body(Rest);
any_body([_ | Rest]) ->
    any_body(Rest);
any_body(_) ->
    false.

any_subquery([Condition | Rest]) ->
    subquery_filters(Condition) orelse any_subquery(Rest);
any_subquery(_) ->
    false.

subquery_filters({Op, Conditions}) when Op =:= 'and'; Op =:= 'or' ->
    any_subquery(Conditions);
subquery_filters({'not', Inner}) ->
    subquery_filters(Inner);
subquery_filters({_Field, in, {subquery, #kura_query{} = Sub}}) ->
    filters_redacted(Sub);
subquery_filters({Exists, {subquery, #kura_query{} = Sub}}) when
    Exists =:= exists; Exists =:= not_exists
->
    filters_redacted(Sub);
subquery_filters(_Condition) ->
    false.
