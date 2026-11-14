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

%% What telemetry may show of the parameters of a compiled `Query`: `Params`
%% itself when no schema it reads has a redacted field, else
%% {redacted, Shown}. `Shown` holds one ~"[redacted]" per parameter when the
%% query, or a query nested in it, filters on a redacted field, and is
%% `Params` otherwise.
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
    case redacted_read(Query) of
        [] ->
            plain;
        Redacted ->
            case names_anywhere(Query, Redacted) of
                true -> hidden;
                false -> redacted
            end
    end.

-doc false.
-spec shown(mode(), [term()]) -> shown().
shown(plain, Params) -> Params;
shown(redacted, Params) -> {redacted, Params};
shown(hidden, Params) -> {redacted, all_redacted(Params)}.

%% What telemetry may show of parameters the caller says are sensitive: one
%% ~"[redacted]" for each.
-doc false.
-spec sensitive_params([term()]) -> shown().
sensitive_params(Params) ->
    {redacted, all_redacted(Params)}.

filtered_params(Query, Params) ->
    case filters_redacted(Query) of
        true -> all_redacted(Params);
        false -> Params
    end.

%% What telemetry may show of the parameters of an update_all of `Query`
%% that sets `SetFields`. As query_params/2, and every parameter is replaced
%% when a set field is redacted too.
-doc false.
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

%% What telemetry may show of the parameters of a write to `Schema`:
%% `Params` itself when the schema has no redacted field, else
%% {redacted, Shown}. `Fields` says what `Params` hold: a list of fields
%% when the parameters are those fields' values in that order, once or once
%% per row, or {touches, Fields} when the statement binds values for those
%% fields somewhere among its parameters.
-doc false.
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

%% What telemetry may show of the error of a failed statement: the error's
%% code and the names in it, never its message or detail, which can quote a
%% value.
-doc false.
-spec error_reason(term()) -> term().
error_reason(Reason) when is_map(Reason) -> maps:with(?ERROR_KEYS, Reason);
error_reason(Reason) when is_atom(Reason) -> Reason;
error_reason({Tag, Reason}) when is_atom(Tag), is_map(Reason) -> {Tag, error_reason(Reason)};
error_reason(_Reason) -> redacted.

filters_redacted(Query) ->
    names_anywhere(Query, redacted_read(Query)).

%% The redacted fields of every source the query reads, at any depth. A
%% condition is judged against all of them and not only those of its own
%% level: the database resolves a name a subquery's sources lack to the
%% outer query's column, and a CTE hands its columns to whatever reads it.
redacted_read(Query) ->
    lists:usort(
        lists:append([
            redacted_of(Source)
         || {Role, Source} <- kura_query:sources(Query), Role =/= preload
        ])
    ).

names_anywhere(_Query, []) ->
    false;
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
        {distance, Field, _Metric, _Vector} -> lists:member(Field, Redacted);
        _ -> false
    end;
condition_names(_Condition, _Redacted) ->
    false.
