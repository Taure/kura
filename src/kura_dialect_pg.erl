-module(kura_dialect_pg).
-moduledoc """
PostgreSQL dialect. Translates a `#kura_query{}` AST into parameterized
SQL using PostgreSQL's `$N` placeholder convention.

This module is an implementation of `kura_dialect`. Public callers go
through `kura_query_compiler`, which delegates to the dialect
configured for the running app.
""".

-behaviour(kura_dialect).

-include("kura.hrl").

-type renames() :: #{atom() => binary()}.
-type qualified_columns() :: undefined | {binary(), #{atom() => kura_types:kura_type()}}.
-type column_context() :: qualified_columns() | {renamed, renames(), qualified_columns()}.

%% `x IN ()` is not SQL. An empty list matches no row, NULL included, and
%% its negation every row, so each compiles to a constant predicate.
%% A text match escapes its value in SQL, so the value is bound untouched and
%% the statement is the same for every value. The escape character is `!`
%% rather than a backslash, whose reading inside a string literal depends on
%% the server's standard_conforming_strings.
-define(ESCAPED_TEXT_OPEN, ~"replace(replace(replace($").
-define(ESCAPED_TEXT_CLOSE, ~", '!', '!!'), '%', '!%'), '_', '!_')").
-define(LIKE_ESCAPE, ~") ESCAPE '!'").

-define(MAX_BIND_PARAMETERS, 65535).

-define(NO_ROWS, ~"1 = 0").
-define(EVERY_ROW, ~"1 = 1").

-export([
    to_sql/1,
    to_sql_from/2,
    convert_values/1,
    insert/3,
    insert/4,
    update/4,
    delete/2,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4,
    column_type/1,
    format_default/1
]).

-doc "Compile a query record into `{SQL, Params}`.".
-spec to_sql(#kura_query{}) -> {iodata(), [term()]}.
to_sql(Query) ->
    {SQL, Params, _Counter} = to_sql_from(Query, 1),
    {SQL, Params}.

-doc "Compile a query starting parameter numbering from `StartCounter`. Returns `{SQL, Params, NextCounter}`.".
-spec to_sql_from(#kura_query{}, pos_integer()) -> {iodata(), [term()], pos_integer()}.
to_sql_from(#kura_query{from = From} = Q, StartCounter) when From =/= undefined ->
    %% CTEs
    {CteSQL, CteParams, Counter00} = compile_ctes(Q#kura_query.ctes, StartCounter),

    Table = resolve_table(From),
    Ctx = column_context(Q),
    {SelectSQL, Params0, Counter0} = compile_select(Q, Ctx, Counter00),
    {WhereSQL, Params1, Counter1} = compile_wheres(Q#kura_query.wheres, Ctx, Counter0),
    Prefix = resolve_prefix(Q#kura_query.prefix),
    {JoinSQL, Params2, Counter2} = compile_joins(
        Q#kura_query.joins, Table, renames(From), Prefix, Counter1
    ),
    {GroupSQL, _, Counter3} = compile_group_by(Q#kura_query.group_bys, Ctx, Counter2),
    {HavingSQL, Params3, Counter4} = compile_havings(Q#kura_query.havings, Ctx, Counter3),
    {OrderSQL, OrderParams, Counter5} = compile_order_by(
        Q#kura_query.order_bys, order_context(Q, Ctx), Counter4
    ),
    {LimitSQL, Params4, Counter6} = compile_limit(Q#kura_query.limit, Counter5),
    {OffsetSQL, Params5, Counter7} = compile_offset(Q#kura_query.offset, Counter6),
    LockSQL = compile_lock(Q#kura_query.lock),
    DistinctSQL = compile_distinct(Q#kura_query.distinct, Ctx),

    FromClause = [~"FROM ", qualified_table(Table, Prefix)],

    MainSQL = iolist_to_binary([
        CteSQL,
        ~"SELECT ",
        DistinctSQL,
        SelectSQL,
        ~" ",
        FromClause,
        JoinSQL,
        WhereSQL,
        GroupSQL,
        HavingSQL,
        OrderSQL,
        LimitSQL,
        OffsetSQL,
        LockSQL
    ]),
    MainParams =
        CteParams ++ Params0 ++ Params2 ++ Params1 ++ Params3 ++ OrderParams ++ Params4 ++ Params5,

    %% Combinations (UNION, INTERSECT, EXCEPT)
    {FinalSQL, FinalParams, FinalCounter} = compile_combinations(
        Q#kura_query.combinations, MainSQL, MainParams, Counter7
    ),
    {FinalSQL, FinalParams, FinalCounter}.

%%----------------------------------------------------------------------
%% Where values
%%----------------------------------------------------------------------

-doc """
Convert the values of a query's where conditions to what PostgreSQL would
parse them as. `kura_query_compiler` calls this before it compiles the query
or splits it into a cached shape.

A value compared to a field of a `from` schema is converted only where it
then binds the value the database would parse from it: an atom on an
`{enum, [...]}` field binds its binary (`{status, published}` binds
`~"published"`), and a binary in the form `kura_changeset:cast/4` accepts
binds the value it spells on an `id`, `integer`, `bigint`, `smallint`,
`float`, `boolean`, `date` or `time` field (`~"42"` binds `42`).
A datetime or time binary is cast only without a UTC offset other than
`Z` and without fractional seconds, since the cast drops a fraction and
drops the offset of a time or naive datetime. An
integer binary longer than 20 bytes, or a float binary longer than 64, is
not parsed. This covers `=`, `!=`, `<`, `>`, `<=`, `>=`, `in`, `not_in`
and `between`, under `and`, `or` and `not`, in the query and in every CTE,
combined query and subquery it holds, each against its own `from` schema.

Everything else is bound as given: a value the type rejects, `decimal`
(its cast parses a float, which would lose digits), `string`, `text`,
`binary`, `uuid`, `jsonb`, embed, array, vector and custom fields,
fields of joined schemas, CTEs and bare table names, `like`, `ilike`,
`matches` and fragments. Havings are not converted.

The conversion is PostgreSQL's reading of a value, so it lives here and not
in `kura_query:where/2`: SQLite, for one, would compare a converted boolean
`true` with an `INTEGER` column as the text `'true'`.
""".
-spec convert_values(#kura_query{}) -> #kura_query{}.
convert_values(#kura_query{from = From, wheres = Wheres, ctes = With, combinations = Comb} = Q) ->
    Q#kura_query{
        wheres = dump_conditions(from_types(From), Wheres),
        ctes = convert_named(With),
        combinations = convert_named(Comb)
    }.

%% A hand-built query can hold an improper list, which the encrypted filter
%% guard refuses after this runs; it is left for the guard, since a crash here
%% would carry the query's values.
convert_named(Named) ->
    case is_proper(Named) of
        true -> [convert_one(N) || N <- Named];
        false -> Named
    end.

convert_one({Name, #kura_query{} = Q}) -> {Name, convert_values(Q)};
convert_one(Other) -> Other.

dump_conditions(Types, Conditions) ->
    case is_proper(Conditions) of
        true -> [dump_condition(Types, C) || C <- Conditions];
        false -> Conditions
    end.

is_proper([]) -> true;
is_proper([_ | T]) -> is_proper(T);
is_proper(_) -> false.

from_types(From) when is_atom(From), From =/= undefined ->
    case kura_schema:is_schema(From) of
        true -> kura_schema:field_types(From);
        false -> #{}
    end;
from_types(_From) ->
    #{}.

dump_condition(Types, {'and', Conditions}) when is_list(Conditions) ->
    {'and', dump_conditions(Types, Conditions)};
dump_condition(Types, {'or', Conditions}) when is_list(Conditions) ->
    {'or', dump_conditions(Types, Conditions)};
dump_condition(Types, {'not', Condition}) ->
    {'not', dump_condition(Types, Condition)};
dump_condition(_Types, {Exists, {subquery, #kura_query{} = SubQ}}) when
    Exists =:= exists orelse Exists =:= not_exists
->
    {Exists, {subquery, convert_values(SubQ)}};
dump_condition(_Types, {Field, Op, {subquery, #kura_query{} = SubQ}}) ->
    {Field, Op, {subquery, convert_values(SubQ)}};
dump_condition(Types, Condition) when map_size(Types) =:= 0 ->
    Condition;
dump_condition(Types, {Field, Op, Values}) when
    is_atom(Field) andalso is_list(Values) andalso (Op =:= in orelse Op =:= not_in)
->
    case convertible_type(Types, Field) of
        {ok, Type} -> {Field, Op, where_values(Type, Values)};
        none -> {Field, Op, Values}
    end;
dump_condition(Types, {Field, between, {Low, High}}) when is_atom(Field) ->
    {Field, between, {dump_value(Types, Field, Low), dump_value(Types, Field, High)}};
dump_condition(Types, {Field, Op, Value}) when
    is_atom(Field) andalso
        (Op =:= '=' orelse Op =:= '!=' orelse Op =:= '<' orelse Op =:= '>' orelse Op =:= '<=' orelse
            Op =:= '>=')
->
    {Field, Op, dump_value(Types, Field, Value)};
dump_condition(Types, {Field, Value}) when is_atom(Field) ->
    case ?KURA_IS_UNARY_OP(Value) of
        true -> {Field, Value};
        false -> {Field, dump_value(Types, Field, Value)}
    end;
dump_condition(_Types, Condition) ->
    Condition.

%% A value can reach here before the encrypted filter guard runs (the cached
%% path converts first, and a hand-built query skips where/2). An encrypted
%% field's type is never convertible, so its value is returned untouched.
dump_value(Types, Field, Value) ->
    case convertible_type(Types, Field) of
        {ok, Type} -> where_value(Type, Value);
        none -> Value
    end.

%% A long `in` list of values already in wire form is kept as it is.
where_values(Type, Values) ->
    case is_proper(Values) andalso any_convertible(Type, Values) of
        true -> [where_value(Type, V) || V <- Values];
        false -> Values
    end.

any_convertible(_Type, []) ->
    false;
any_convertible({enum, _} = Type, [V | Rest]) ->
    is_atom(V) orelse any_convertible(Type, Rest);
any_convertible(Type, [V | Rest]) ->
    is_binary(V) orelse any_convertible(Type, Rest).

convertible_type(Types, Field) ->
    case Types of
        #{Field := {enum, _} = Type} ->
            {ok, Type};
        #{Field := Type} when is_atom(Type) ->
            case scalar_type(Type) of
                true -> {ok, Type};
                false -> none
            end;
        #{} ->
            none
    end.

%% Only conversions that bind what the database would parse: an enum
%% atom to its stored binary, and a binary in cast form to the scalar it
%% spells. A datetime binary with an offset or a fraction is left alone,
%% since the cast can drop them. jsonb, embeds, arrays, vectors, strings
%% and custom types are bound as given, as they always were.
where_value(_Type, Value) when Value =:= undefined; Value =:= null; Value =:= nil ->
    Value;
where_value({enum, _}, Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
where_value(Type, Value) when is_binary(Value) ->
    case scalar_type(Type) andalso exact_cast_form(Type, Value) of
        true -> cast_or_keep(Type, Value);
        false -> Value
    end;
where_value(_Type, Value) ->
    Value.

scalar_type(Type) ->
    lists:member(Type, [
        id,
        integer,
        bigint,
        smallint,
        float,
        boolean,
        date,
        time,
        utc_datetime,
        naive_datetime
    ]).

exact_cast_form(Type, Value) when Type =:= utc_datetime; Type =:= naive_datetime ->
    case binary:split(Value, [~"T", ~" "]) of
        [_Date, Time] -> exact_time(Time);
        _ -> false
    end;
exact_cast_form(time, Value) ->
    exact_time(Value);
exact_cast_form(Type, Value) when
    Type =:= id; Type =:= integer; Type =:= bigint; Type =:= smallint
->
    byte_size(Value) =< 20;
exact_cast_form(float, Value) ->
    byte_size(Value) =< 64;
exact_cast_form(_Type, _Value) ->
    true.

exact_time(Time) ->
    Bare =
        case Time of
            <<Rest:(byte_size(Time) - 1)/binary, "Z">> -> Rest;
            _ -> Time
        end,
    binary:match(Bare, [~".", ~",", ~"+", ~"-"]) =:= nomatch.

%% cast/2 is not total (it reaches the datetime parsers unguarded), and a
%% value it rejects is bound as given, as before.
cast_or_keep(Type, Value) ->
    try kura_types:cast(Type, Value) of
        {ok, Cast} -> Cast;
        {error, _} -> Value
    catch
        _:_ -> Value
    end.

%%----------------------------------------------------------------------
%% INSERT
%%----------------------------------------------------------------------

-spec insert(kura_dialect:table(), [atom()], map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data) ->
    Table = resolve_table(SchemaOrTable),
    {Cols, Placeholders, Params, _} = build_insert_parts(
        Fields, Data, renames(SchemaOrTable), 1
    ),
    SQL = iolist_to_binary([
        ~"INSERT INTO ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" (",
        join_comma(Cols),
        ~") VALUES (",
        join_comma(Placeholders),
        ~") RETURNING *"
    ]),
    {SQL, Params}.

-spec insert(kura_dialect:table(), [atom()], map(), map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data, #{on_conflict := OnConflict0}) ->
    Table = resolve_table(SchemaOrTable),
    Renames = renames(SchemaOrTable),
    {OnConflict, Guard} = conflict_guard(OnConflict0),
    {Cols, Placeholders, Params, Counter} = build_insert_parts(Fields, Data, Renames, 1),
    {ConflictSQL0, ConflictParams} = compile_on_conflict(
        OnConflict, Fields, Data, Renames, Counter
    ),
    ConflictSQL = [ConflictSQL0, guard_sql(Table, OnConflict, Guard, Renames)],
    SQL = iolist_to_binary([
        ~"INSERT INTO ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" (",
        join_comma(Cols),
        ~") VALUES (",
        join_comma(Placeholders),
        ~")",
        ConflictSQL,
        ~" RETURNING *"
    ]),
    {SQL, Params ++ ConflictParams};
insert(SchemaOrTable, Fields, Data, _Opts) ->
    insert(SchemaOrTable, Fields, Data).

%%----------------------------------------------------------------------
%% UPDATE
%%----------------------------------------------------------------------

-spec update(kura_dialect:table(), [atom()], map(), [{atom(), term()}]) -> {iodata(), [term()]}.
update(SchemaOrTable, Fields, Changes, KeyClauses) ->
    Table = resolve_table(SchemaOrTable),
    Renames = renames(SchemaOrTable),
    {Sets, Params, Counter} = build_set_parts(Fields, Changes, Renames, 1),
    {KeyConds, KeyParams, _} = key_where(KeyClauses, Renames, Counter),
    SQL = iolist_to_binary([
        ~"UPDATE ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" SET ",
        join_comma(Sets),
        ~" WHERE ",
        lists:join(~" AND ", KeyConds),
        ~" RETURNING *"
    ]),
    {SQL, Params ++ KeyParams}.

%% One `col = $n` condition per key column, params in key order.
key_where(KeyClauses, Renames, Start) ->
    key_where(KeyClauses, Renames, Start, [], []).

key_where([], _Renames, Counter, Conds, Params) ->
    {lists:reverse(Conds), lists:reverse(Params), Counter};
key_where([{Field, Value} | Rest], Renames, Counter, Conds, Params) ->
    Cond = [column_ident(Field, Renames), ~" = $", integer_to_binary(Counter)],
    key_where(Rest, Renames, Counter + 1, [Cond | Conds], [Value | Params]).

%% Render one `($n, ...)` group per value-tuple for a row-value IN, params
%% flattened in row-then-column order.
compile_row_tuples([], _Arity, Counter, SQLs, Params) ->
    {lists:reverse(SQLs), lists:reverse(Params), Counter};
compile_row_tuples([Tuple | Rest], Arity, Counter, SQLs, Params) when tuple_size(Tuple) =:= Arity ->
    {Placeholders, NextCounter} = row_placeholders(Arity, Counter, []),
    RowSQL = [~"(", join_comma(Placeholders), ~")"],
    compile_row_tuples(
        Rest, Arity, NextCounter, [RowSQL | SQLs], lists:reverse(tuple_to_list(Tuple), Params)
    );
compile_row_tuples([Tuple | _], Arity, _Counter, _SQLs, _Params) ->
    error({row_arity_mismatch, Arity, Tuple}).

text_match_keyword(Op) when Op =:= contains; Op =:= starts_with; Op =:= ends_with ->
    ~" LIKE (";
text_match_keyword(Op) when Op =:= icontains; Op =:= istarts_with; Op =:= iends_with ->
    ~" ILIKE (".

text_match_prefix(Op) when Op =:= starts_with; Op =:= istarts_with -> <<>>;
text_match_prefix(_Op) -> ~"'%' || ".

text_match_suffix(Op) when Op =:= ends_with; Op =:= iends_with -> <<>>;
text_match_suffix(_Op) -> ~" || '%'".

unary_sql(is_nil) -> ~" IS NULL";
unary_sql(is_not_nil) -> ~" IS NOT NULL".

single_value({Value}) -> Value;
single_value(Tuple) -> error({row_arity_mismatch, 1, Tuple}).

placeholders(Counter, Count) ->
    [[~"$", integer_to_binary(N)] || N <- lists:seq(Counter, Counter + Count - 1)].

row_placeholders(0, Counter, Acc) ->
    {lists:reverse(Acc), Counter};
row_placeholders(N, Counter, Acc) ->
    row_placeholders(N - 1, Counter + 1, [[~"$", integer_to_binary(Counter)] | Acc]).

%%----------------------------------------------------------------------
%% DELETE
%%----------------------------------------------------------------------

-spec delete(kura_dialect:table(), [{atom(), term()}]) -> {iodata(), [term()]}.
delete(SchemaOrTable, KeyClauses) ->
    Table = resolve_table(SchemaOrTable),
    {KeyConds, Params, _} = key_where(KeyClauses, renames(SchemaOrTable), 1),
    SQL = iolist_to_binary([
        ~"DELETE FROM ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" WHERE ",
        lists:join(~" AND ", KeyConds),
        ~" RETURNING *"
    ]),
    {SQL, Params}.

%%----------------------------------------------------------------------
%% UPDATE ALL (bulk)
%%----------------------------------------------------------------------

-spec update_all(#kura_query{}, map()) -> {iodata(), [term()]}.
update_all(#kura_query{from = From, wheres = Wheres, prefix = QPrefix}, SetMap) ->
    Table = resolve_table(From),
    Prefix = resolve_prefix(QPrefix),
    Fields = maps:keys(SetMap),
    Renames = renames(From),
    {Sets, Params, Counter} = build_set_parts(Fields, SetMap, Renames, 1),
    {WhereSQL, WhereParams, _} = compile_wheres(
        Wheres, renamed_context(Renames, undefined), Counter
    ),
    SQL = iolist_to_binary([
        ~"UPDATE ",
        qualified_table(Table, Prefix),
        ~" SET ",
        join_comma(Sets),
        WhereSQL
    ]),
    {SQL, Params ++ WhereParams}.

%%----------------------------------------------------------------------
%% DELETE ALL (bulk)
%%----------------------------------------------------------------------

-spec delete_all(#kura_query{}) -> {iodata(), [term()]}.
delete_all(#kura_query{from = From, wheres = Wheres, prefix = QPrefix}) ->
    Table = resolve_table(From),
    Prefix = resolve_prefix(QPrefix),
    {WhereSQL, WhereParams, _} = compile_wheres(
        Wheres, renamed_context(renames(From), undefined), 1
    ),
    SQL = iolist_to_binary([
        ~"DELETE FROM ",
        qualified_table(Table, Prefix),
        WhereSQL
    ]),
    {SQL, WhereParams}.

%%----------------------------------------------------------------------
%% INSERT ALL (bulk)
%%----------------------------------------------------------------------

-spec insert_all(kura_dialect:table(), [atom()], [map()]) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows) ->
    ok = check_parameter_count(length(Rows) * length(Fields)),
    Table = resolve_table(SchemaOrTable),
    Renames = renames(SchemaOrTable),
    Cols = [column_ident(F, Renames) || F <- Fields],
    {ValueGroups, AllParams, _} = build_value_groups(Rows, Fields, 1),
    SQL = iolist_to_binary([
        ~"INSERT INTO ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" (",
        join_comma(Cols),
        ~") VALUES ",
        join_comma(ValueGroups)
    ]),
    {SQL, AllParams}.

%% The Bind message counts its parameters in an unsigned 16-bit field.
check_parameter_count(Count) when Count =< ?MAX_BIND_PARAMETERS -> ok;
check_parameter_count(Count) ->
    erlang:error({kura, {too_many_parameters, Count, ?MAX_BIND_PARAMETERS}}).

-spec insert_all(kura_dialect:table(), [atom()], [map()], map()) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows, Opts) ->
    {BaseSQL, Params} = insert_all(SchemaOrTable, Fields, Rows),
    Renames = renames(SchemaOrTable),
    SQL = [
        BaseSQL,
        guarded_bulk_on_conflict(resolve_table(SchemaOrTable), Opts, Fields, Renames),
        bulk_returning(Opts, Renames)
    ],
    {iolist_to_binary(SQL), Params}.

guarded_bulk_on_conflict(Table, #{on_conflict := OnConflict0} = Opts, Fields, Renames) ->
    {OnConflict, Guard} = conflict_guard(OnConflict0),
    [
        bulk_on_conflict(Opts#{on_conflict => OnConflict}, Fields, Renames),
        guard_sql(Table, OnConflict, Guard, Renames)
    ];
guarded_bulk_on_conflict(_Table, Opts, Fields, Renames) ->
    bulk_on_conflict(Opts, Fields, Renames).

%% A bulk upsert applies one DO UPDATE clause to every row, so the SET values
%% cannot come from any single row's parameters the way the single-row form
%% builds them. EXCLUDED names the row that failed to insert, which is what
%% makes one clause correct for all of them - and it binds no parameters.
bulk_on_conflict(#{on_conflict := {Target, nothing}}, _Fields, Renames) ->
    [conflict_target(Target, Renames), ~" DO NOTHING"];
bulk_on_conflict(#{on_conflict := {Target, replace_all}}, Fields, Renames) ->
    bulk_on_conflict(
        #{on_conflict => {Target, {replace, replaceable(Target, Fields)}}}, Fields, Renames
    );
bulk_on_conflict(#{on_conflict := {Target, {replace, UpdateFields}}}, _Fields, Renames) ->
    [
        conflict_target(Target, Renames),
        ~" DO UPDATE SET ",
        join_comma([excluded_set(F, Renames) || F <- UpdateFields])
    ];
bulk_on_conflict(#{}, _Fields, _Renames) ->
    [].

excluded_set(Field, Renames) ->
    Col = column_ident(Field, Renames),
    [Col, ~" = EXCLUDED.", Col].

bulk_returning(#{returning := true}, _Renames) ->
    ~" RETURNING *";
bulk_returning(#{returning := RetFields}, Renames) when is_list(RetFields) ->
    [~" RETURNING ", join_comma([column_ident(F, Renames) || F <- RetFields])];
bulk_returning(#{}, _Renames) ->
    [].

%%----------------------------------------------------------------------
%% Internal: SELECT clause
%%----------------------------------------------------------------------

compile_select(#kura_query{select = []}, _Ctx, Counter) ->
    {~"*", [], Counter};
compile_select(#kura_query{select = {exprs, Exprs}}, Ctx, Counter) ->
    {Parts, AllParams, NewCounter} = compile_select_exprs(Exprs, Ctx, Counter),
    {join_comma(Parts), AllParams, NewCounter};
compile_select(#kura_query{select = Fields}, Ctx, Counter) ->
    Parts = [compile_select_field(F, Ctx) || F <- Fields],
    {join_comma(Parts), [], Counter}.

compile_select_field({Agg, '*'}, _Ctx) when Agg =:= count ->
    [atom_to_binary(Agg, utf8), ~"(*)", ~" AS ", quote_ident(atom_to_binary(Agg, utf8))];
compile_select_field({Agg, Field}, Ctx) when
    Agg =:= count; Agg =:= sum; Agg =:= avg; Agg =:= min; Agg =:= max
->
    [
        atom_to_binary(Agg, utf8),
        ~"(",
        column(Field, Ctx),
        ~")",
        ~" AS ",
        quote_ident(atom_to_binary(Agg, utf8))
    ];
compile_select_field(Field, Ctx) when is_atom(Field) ->
    column(Field, Ctx).

%%----------------------------------------------------------------------
%% Internal: WHERE clause
%%----------------------------------------------------------------------

compile_wheres([], _Ctx, Counter) ->
    {<<>>, [], Counter};
compile_wheres(Conditions, Ctx, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Ctx, Counter),
    SQL = [~" WHERE ", join_and(Parts)],
    {iolist_to_binary(SQL), Params, NewCounter}.

compile_conditions([], _Ctx, Counter) ->
    {[], [], Counter};
compile_conditions([Cond | Rest], Ctx, Counter) ->
    {Part, Vars, Counter1} = compile_condition(Cond, Ctx, Counter),
    {Parts, MoreVars, Counter2} = compile_conditions(Rest, Ctx, Counter1),
    {[Part | Parts], Vars ++ MoreVars, Counter2}.

compile_condition({'and', Conditions}, Ctx, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Ctx, Counter),
    {[~"(", join_and(Parts), ~")"], Params, NewCounter};
compile_condition({'or', Conditions}, Ctx, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Ctx, Counter),
    {[~"(", join_or(Parts), ~")"], Params, NewCounter};
compile_condition({'not', Condition}, Ctx, Counter) ->
    {Part, Params, NewCounter} = compile_condition(Condition, Ctx, Counter),
    {[~"NOT (", Part, ~")"], Params, NewCounter};
compile_condition({fragment, SQL, Params}, _Ctx, Counter) ->
    {RewrittenSQL, NewCounter} = rewrite_fragment_placeholders(SQL, Params, Counter),
    {RewrittenSQL, Params, NewCounter};
compile_condition({{distance, _, _, _} = Distance, Op, Threshold}, Ctx, Counter) when
    ?KURA_IS_THRESHOLD_OP(Op)
->
    {DistanceSQL, DistanceParams, Next} = compile_distance(Distance, Ctx, Counter),
    {
        [DistanceSQL, threshold_op(Op), integer_to_binary(Next)],
        DistanceParams ++ [Threshold],
        Next + 1
    };
compile_condition({Field, Op}, Ctx, Counter) when is_atom(Field), ?KURA_IS_UNARY_OP(Op) ->
    {[column(Field, Ctx), unary_sql(Op)], [], Counter};
compile_condition({Field, '=', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" = $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '!=', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" != $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" < $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" > $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<=', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" <= $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>=', Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" >= $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, like, Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" LIKE $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, ilike, Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" ILIKE $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, Op, Value}, Ctx, Counter) when
    is_atom(Field), ?KURA_IS_TEXT_MATCH_OP(Op)
->
    {
        [
            column(Field, Ctx),
            text_match_keyword(Op),
            text_match_prefix(Op),
            ?ESCAPED_TEXT_OPEN,
            integer_to_binary(Counter),
            ?ESCAPED_TEXT_CLOSE,
            text_match_suffix(Op),
            ?LIKE_ESCAPE
        ],
        [Value],
        Counter + 1
    };
compile_condition({Field, matches, Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [
            ~"to_tsvector(",
            column(Field, Ctx),
            ~") @@ plainto_tsquery($",
            integer_to_binary(Counter),
            ~")"
        ],
        [Value],
        Counter + 1
    };
compile_condition({Field, in, {subquery, SubQ}}, Ctx, Counter) when is_atom(Field) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[column(Field, Ctx), ~" IN (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({exists, {subquery, SubQ}}, _Ctx, Counter) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[~"EXISTS (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({not_exists, {subquery, SubQ}}, _Ctx, Counter) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[~"NOT EXISTS (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({Columns, in, []}, _Ctx, Counter) when is_atom(Columns); is_list(Columns) ->
    {?NO_ROWS, [], Counter};
compile_condition({Field, not_in, []}, _Ctx, Counter) when is_atom(Field) ->
    {?EVERY_ROW, [], Counter};
compile_condition({[Col], in, Tuples}, Ctx, Counter) when is_atom(Col), is_list(Tuples) ->
    compile_condition({Col, in, [single_value(T) || T <- Tuples]}, Ctx, Counter);
compile_condition({Cols, in, Tuples}, Ctx, Counter) when is_list(Cols), is_list(Tuples) ->
    ColSQL = [
        ~"(",
        join_comma([column(C, Ctx) || C <- Cols]),
        ~")"
    ],
    {RowSQLs, Params, NewCounter} = compile_row_tuples(Tuples, length(Cols), Counter, [], []),
    {[ColSQL, ~" IN (", join_comma(RowSQLs), ~")"], Params, NewCounter};
compile_condition({Field, in, Values}, Ctx, Counter) when is_atom(Field), is_list(Values) ->
    Placeholders = placeholders(Counter, length(Values)),
    NewCounter = Counter + length(Values),
    {
        [column(Field, Ctx), ~" IN (", join_comma(Placeholders), ~")"],
        Values,
        NewCounter
    };
compile_condition({Field, not_in, Values}, Ctx, Counter) when is_atom(Field), is_list(Values) ->
    Placeholders = placeholders(Counter, length(Values)),
    NewCounter = Counter + length(Values),
    {
        [
            column(Field, Ctx),
            ~" NOT IN (",
            join_comma(Placeholders),
            ~")"
        ],
        Values,
        NewCounter
    };
compile_condition({Field, encrypted_in, Frames}, Ctx, Counter) when
    is_atom(Field), is_list(Frames)
->
    compile_condition({Field, in, Frames}, Ctx, Counter);
compile_condition({Field, encrypted_not_in, Frames}, Ctx, Counter) when
    is_atom(Field), is_list(Frames)
->
    compile_condition({Field, not_in, Frames}, Ctx, Counter);
compile_condition({Field, between, {Low, High}}, Ctx, Counter) when is_atom(Field) ->
    {
        [
            column(Field, Ctx),
            ~" BETWEEN $",
            integer_to_binary(Counter),
            ~" AND $",
            integer_to_binary(Counter + 1)
        ],
        [Low, High],
        Counter + 2
    };
compile_condition({Field, Value}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), ~" = $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    }.

%%----------------------------------------------------------------------
%% Internal: JOIN clause
%%----------------------------------------------------------------------

compile_joins([], _Table, _Renames, _Prefix, Counter) ->
    {<<>>, [], Counter};
compile_joins(Joins, FromTable, Renames, Prefix, Counter) ->
    Parts = compile_joins_loop(Joins, FromTable, Renames, Prefix),
    {iolist_to_binary(Parts), [], Counter}.

join_type(inner) -> ~"INNER JOIN";
join_type(left) -> ~"LEFT JOIN";
join_type(right) -> ~"RIGHT JOIN";
join_type(full) -> ~"FULL JOIN".

%%----------------------------------------------------------------------
%% Internal: ORDER BY clause
%%----------------------------------------------------------------------

compile_order_by([], _Ctx, Counter) ->
    {<<>>, [], Counter};
compile_order_by(Orders, Ctx, Counter) ->
    {Parts, Params, NewCounter} = compile_orders(Orders, Ctx, Counter),
    {iolist_to_binary([~" ORDER BY ", join_comma(Parts)]), Params, NewCounter}.

compile_orders([], _Ctx, Counter) ->
    {[], [], Counter};
compile_orders([{Expr, Dir} | Rest], Ctx, Counter) ->
    {ExprSQL, Params, C1} = compile_order_expr(Expr, Ctx, Counter),
    {Parts, MoreParams, C2} = compile_orders(Rest, Ctx, C1),
    {[[ExprSQL, ~" ", dir_to_sql(Dir)] | Parts], Params ++ MoreParams, C2}.

compile_order_expr({distance, _Field, _Metric, _Vector} = Distance, Ctx, Counter) ->
    compile_distance(Distance, Ctx, Counter);
compile_order_expr(Field, Ctx, Counter) when is_atom(Field) ->
    {column(Field, Ctx), [], Counter}.

%% No cast on the parameter: the server infers it from the column, so the
%% same SQL serves any pgvector column type.
compile_distance({distance, Field, Metric, Vector}, Ctx, Counter) when is_atom(Field) ->
    {
        [column(Field, Ctx), distance_op(Metric), integer_to_binary(Counter)],
        [Vector],
        Counter + 1
    }.

distance_op(l2) -> ~" <-> $";
distance_op(cosine) -> ~" <=> $";
distance_op(inner_product) -> ~" <#> $".

threshold_op('<') -> ~" < $";
threshold_op('<=') -> ~" <= $";
threshold_op('>') -> ~" > $";
threshold_op('>=') -> ~" >= $".

dir_to_sql(asc) -> ~"ASC";
dir_to_sql(desc) -> ~"DESC".

%%----------------------------------------------------------------------
%% Internal: GROUP BY clause
%%----------------------------------------------------------------------

compile_group_by([], _Ctx, Counter) ->
    {<<>>, [], Counter};
compile_group_by(Fields, Ctx, Counter) ->
    Parts = [column(F, Ctx) || F <- Fields],
    {iolist_to_binary([~" GROUP BY ", join_comma(Parts)]), [], Counter}.

%%----------------------------------------------------------------------
%% Internal: HAVING clause
%%----------------------------------------------------------------------

compile_havings([], _Ctx, Counter) ->
    {<<>>, [], Counter};
compile_havings(Conditions, Ctx, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Ctx, Counter),
    SQL = [~" HAVING ", join_and(Parts)],
    {iolist_to_binary(SQL), Params, NewCounter}.

%%----------------------------------------------------------------------
%% Internal: LIMIT / OFFSET
%%----------------------------------------------------------------------

compile_limit(undefined, Counter) ->
    {<<>>, [], Counter};
compile_limit(N, Counter) ->
    {iolist_to_binary([~" LIMIT $", integer_to_binary(Counter)]), [N], Counter + 1}.

compile_offset(undefined, Counter) ->
    {<<>>, [], Counter};
compile_offset(N, Counter) ->
    {iolist_to_binary([~" OFFSET $", integer_to_binary(Counter)]), [N], Counter + 1}.

%%----------------------------------------------------------------------
%% Internal: LOCK / DISTINCT
%%----------------------------------------------------------------------

compile_lock(undefined) -> <<>>;
compile_lock(Lock) -> [~" ", lock_sql(kura_query:lock_clause(Lock))].

lock_sql({fragment, SQL}) -> SQL;
lock_sql({Strength, nowait}) -> [lock_sql(Strength), ~" NOWAIT"];
lock_sql({Strength, skip_locked}) -> [lock_sql(Strength), ~" SKIP LOCKED"];
lock_sql(for_update) -> ~"FOR UPDATE";
lock_sql(for_no_key_update) -> ~"FOR NO KEY UPDATE";
lock_sql(for_share) -> ~"FOR SHARE";
lock_sql(for_key_share) -> ~"FOR KEY SHARE".

compile_distinct(false, _Ctx) ->
    <<>>;
compile_distinct(true, _Ctx) ->
    ~"DISTINCT ";
compile_distinct(Fields, Ctx) when is_list(Fields) ->
    Parts = [column(F, Ctx) || F <- Fields],
    iolist_to_binary([~"DISTINCT ON (", join_comma(Parts), ~") "]).

%%----------------------------------------------------------------------
%% Internal: helpers
%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Internal: CTE (WITH) compilation
%%----------------------------------------------------------------------

compile_ctes([], Counter) ->
    {<<>>, [], Counter};
compile_ctes(CTEs, Counter) ->
    {Parts, AllParams, NewCounter} = compile_ctes_loop(CTEs, Counter),
    {[~"WITH ", join_comma(Parts), ~" "], AllParams, NewCounter}.

%%----------------------------------------------------------------------
%% Internal: UNION / INTERSECT / EXCEPT compilation
%%----------------------------------------------------------------------

compile_combinations([], MainSQL, MainParams, Counter) ->
    {MainSQL, MainParams, Counter};
compile_combinations(Combinations, MainSQL, MainParams, Counter) ->
    {CombParts, AllParams, NewCounter} = compile_combinations_loop(Combinations, Counter),
    FinalSQL = iolist_to_binary([MainSQL | CombParts]),
    {FinalSQL, MainParams ++ AllParams, NewCounter}.

combination_type(union) -> ~"UNION";
combination_type(union_all) -> ~"UNION ALL";
combination_type(intersect) -> ~"INTERSECT";
combination_type(except) -> ~"EXCEPT".

%%----------------------------------------------------------------------
%% Internal: ON CONFLICT
%%----------------------------------------------------------------------

compile_on_conflict({Target, nothing}, _Fields, _Data, Renames, _Counter) ->
    {[conflict_target(Target, Renames), ~" DO NOTHING"], []};
compile_on_conflict({Target, replace_all}, Fields, Data, Renames, Counter) ->
    compile_on_conflict(
        {Target, {replace, replaceable(Target, Fields)}}, Fields, Data, Renames, Counter
    );
compile_on_conflict({Target, {replace, UpdateFields}}, _Fields, Data, Renames, Counter) ->
    {Sets, Params, _} = build_set_parts(UpdateFields, Data, Renames, Counter),
    {[conflict_target(Target, Renames), ~" DO UPDATE SET ", join_comma(Sets)], Params}.

%% A row the guard refuses is left as it is and not returned, as with DO NOTHING.
conflict_guard({Target, Action, #{guard := Fields} = Opts}) when
    is_list(Fields), map_size(Opts) =:= 1
->
    {{Target, Action}, Fields};
conflict_guard({_Target, _Action, Opts}) ->
    erlang:error({kura, {invalid_on_conflict_opts, Opts}});
conflict_guard(OnConflict) ->
    {OnConflict, []}.

guard_sql(_Table, {_Target, nothing}, _Fields, _Renames) ->
    [];
guard_sql(_Table, _OnConflict, [], _Renames) ->
    [];
guard_sql(Table, _OnConflict, Fields, Renames) ->
    [~" WHERE ", lists:join(~" AND ", [guard_condition(Table, F, Renames) || F <- Fields])].

guard_condition(Table, Field, Renames) ->
    Column = column_ident(Field, Renames),
    [quote_ident(Table), ~".", Column, ~" = EXCLUDED.", Column].

conflict_target(Field, Renames) when is_atom(Field) ->
    [~" ON CONFLICT (", column_ident(Field, Renames), ~")"];
conflict_target({columns, Columns}, Renames) when is_list(Columns) ->
    [~" ON CONFLICT (", columns_target(Columns, Renames), ~")"];
conflict_target({columns, Columns, Opts}, Renames) when is_list(Columns), is_map(Opts) ->
    [~" ON CONFLICT (", columns_target(Columns, Renames), ~")", index_predicate(Opts)];
conflict_target({constraint, Name}, _Renames) when is_binary(Name) ->
    [~" ON CONFLICT ON CONSTRAINT ", quote_ident(Name)].

%% The columns the conflict target already pins cannot be in the SET list.
replaceable(Field, Fields) when is_atom(Field) ->
    [F || F <- Fields, F =/= Field];
replaceable({columns, Columns}, Fields) when is_list(Columns) ->
    [F || F <- Fields, not lists:member(F, Columns)];
replaceable({columns, Columns, _Opts}, Fields) when is_list(Columns) ->
    [F || F <- Fields, not lists:member(F, Columns)];
replaceable({constraint, _Name}, Fields) ->
    Fields.

columns_target(Columns, Renames) ->
    join_comma([column_ident(C, Renames) || C <- Columns]).

index_predicate(#{where := Expr}) when is_binary(Expr) ->
    [~" WHERE ", Expr];
index_predicate(Opts) when map_size(Opts) =:= 0 ->
    [];
index_predicate(Opts) ->
    erlang:error({kura, {invalid_on_conflict_opts, Opts}}).

resolve_table(Table) when is_binary(Table) ->
    Table;
resolve_table(Mod) when is_atom(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            case erlang:function_exported(Mod, table, 0) of
                true -> Mod:table();
                false -> atom_to_binary(Mod, utf8)
            end;
        _ ->
            atom_to_binary(Mod, utf8)
    end.

%% Which columns to qualify is decided by
%% `kura_dialect:qualified_columns/1`; the dialect only renders it.
-spec column_context(#kura_query{}) -> column_context().
column_context(#kura_query{from = From} = Q) ->
    Qualified =
        case kura_dialect:qualified_columns(Q) of
            undefined -> undefined;
            {Table, Fields} -> {quote_ident(Table), Fields}
        end,
    renamed_context(renames(From), Qualified).

%% Only a schema that names a column differently from its field pays for
%% the lookup in column/2.
-spec renamed_context(renames(), qualified_columns()) -> column_context().
renamed_context(Renames, Qualified) when map_size(Renames) =:= 0 -> Qualified;
renamed_context(Renames, Qualified) -> {renamed, Renames, Qualified}.

-spec renames(term()) -> renames().
renames(Mod) when is_atom(Mod) -> kura_schema:renamed_columns(Mod);
renames(_Table) -> #{}.

-spec column_ident(atom(), renames()) -> iodata().
column_ident(Field, Renames) ->
    case Renames of
        #{Field := Column} -> quote_ident(Column);
        #{} -> quote_ident(atom_to_binary(Field, utf8))
    end.

%% ORDER BY resolves a name to a select_expr output column before an input
%% column, so an alias is left bare to keep naming the expression.
-spec order_context(#kura_query{}, column_context()) -> column_context().
order_context(#kura_query{select = {exprs, Exprs}} = Q, {renamed, Renames, Qualified}) ->
    Unaliased = maps:without([Alias || {Alias, _} <- Exprs], Renames),
    renamed_context(Unaliased, order_context(Q, Qualified));
order_context(#kura_query{select = {exprs, Exprs}}, {Qualifier, Fields}) ->
    {Qualifier, maps:without([Alias || {Alias, _} <- Exprs], Fields)};
order_context(_Q, Ctx) ->
    Ctx.

-spec column(atom(), column_context()) -> iodata().
column(Field, {renamed, Renames, Qualified}) ->
    qualified(Field, column_ident(Field, Renames), Qualified);
column(Field, Qualified) ->
    qualified(Field, quote_ident(atom_to_binary(Field, utf8)), Qualified).

qualified(Field, Ident, {Qualifier, Fields}) when is_map_key(Field, Fields) ->
    [Qualifier, ~".", Ident];
qualified(_Field, Ident, _Qualified) ->
    Ident.

quote_ident(Name) when is_binary(Name) ->
    Escaped = binary:replace(Name, ~"\"", ~"\"\"", [global]),
    <<$", Escaped/binary, $">>.

join_comma(Parts) ->
    lists:join(~", ", Parts).

join_and(Parts) ->
    lists:join(~" AND ", Parts).

join_or(Parts) ->
    lists:join(~" OR ", Parts).

%% Each `?` of a fragment becomes the next `$N`. A `?` inside a string
%% literal, a quoted identifier, a dollar-quoted string or a comment is text,
%% not a placeholder, and is left as it is. The count has to match the
%% parameters: a fragment that binds more or fewer than it names would shift
%% every parameter after it.
rewrite_fragment_placeholders(SQL, Params, StartCounter) when is_binary(SQL), is_list(Params) ->
    {Rewritten, NextCounter} = fragment_text(SQL, StartCounter, <<>>),
    case NextCounter - StartCounter =:= length(Params) of
        true ->
            {Rewritten, NextCounter};
        false ->
            erlang:error(
                {kura, {fragment_placeholder_mismatch, NextCounter - StartCounter, length(Params)}}
            )
    end.

fragment_text(<<>>, Counter, Acc) ->
    {Acc, Counter};
fragment_text(<<"?", Rest/binary>>, Counter, Acc) ->
    fragment_text(Rest, Counter + 1, <<Acc/binary, "$", (integer_to_binary(Counter))/binary>>);
fragment_text(<<"'", Rest/binary>>, Counter, Acc) ->
    Escapes = escape_string_prefix(Acc),
    fragment_quoted(Rest, $', Escapes, Counter, <<Acc/binary, "'">>);
fragment_text(<<"\"", Rest/binary>>, Counter, Acc) ->
    fragment_quoted(Rest, $", false, Counter, <<Acc/binary, "\"">>);
fragment_text(<<"--", Rest/binary>>, Counter, Acc) ->
    fragment_line_comment(Rest, Counter, <<Acc/binary, "--">>);
fragment_text(<<"/*", Rest/binary>>, Counter, Acc) ->
    fragment_block_comment(Rest, 1, Counter, <<Acc/binary, "/*">>);
fragment_text(<<"$", Rest/binary>>, Counter, Acc) ->
    case dollar_quote_tag(Rest, Acc) of
        {ok, Tag, AfterTag} ->
            fragment_dollar_quoted(AfterTag, Tag, Counter, <<Acc/binary, Tag/binary>>);
        none ->
            fragment_text(Rest, Counter, <<Acc/binary, "$">>)
    end;
fragment_text(<<Byte, Rest/binary>>, Counter, Acc) ->
    fragment_text(Rest, Counter, <<Acc/binary, Byte>>).

%% A doubled quote stays inside the literal. In an escape string (`E'...'`)
%% a backslash escapes the byte after it, a quote included.
fragment_quoted(<<Quote, Quote, Rest/binary>>, Quote, Escapes, Counter, Acc) ->
    fragment_quoted(Rest, Quote, Escapes, Counter, <<Acc/binary, Quote, Quote>>);
fragment_quoted(<<Quote, Rest/binary>>, Quote, _Escapes, Counter, Acc) ->
    fragment_text(Rest, Counter, <<Acc/binary, Quote>>);
fragment_quoted(<<"\\", Byte, Rest/binary>>, Quote, true, Counter, Acc) ->
    fragment_quoted(Rest, Quote, true, Counter, <<Acc/binary, "\\", Byte>>);
fragment_quoted(<<Byte, Rest/binary>>, Quote, Escapes, Counter, Acc) ->
    fragment_quoted(Rest, Quote, Escapes, Counter, <<Acc/binary, Byte>>);
fragment_quoted(<<>>, _Quote, _Escapes, Counter, Acc) ->
    {Acc, Counter}.

fragment_line_comment(<<"\n", Rest/binary>>, Counter, Acc) ->
    fragment_text(Rest, Counter, <<Acc/binary, "\n">>);
fragment_line_comment(<<Byte, Rest/binary>>, Counter, Acc) ->
    fragment_line_comment(Rest, Counter, <<Acc/binary, Byte>>);
fragment_line_comment(<<>>, Counter, Acc) ->
    {Acc, Counter}.

%% PostgreSQL nests block comments.
fragment_block_comment(<<"*/", Rest/binary>>, 1, Counter, Acc) ->
    fragment_text(Rest, Counter, <<Acc/binary, "*/">>);
fragment_block_comment(<<"*/", Rest/binary>>, Depth, Counter, Acc) ->
    fragment_block_comment(Rest, Depth - 1, Counter, <<Acc/binary, "*/">>);
fragment_block_comment(<<"/*", Rest/binary>>, Depth, Counter, Acc) ->
    fragment_block_comment(Rest, Depth + 1, Counter, <<Acc/binary, "/*">>);
fragment_block_comment(<<Byte, Rest/binary>>, Depth, Counter, Acc) ->
    fragment_block_comment(Rest, Depth, Counter, <<Acc/binary, Byte>>);
fragment_block_comment(<<>>, _Depth, Counter, Acc) ->
    {Acc, Counter}.

fragment_dollar_quoted(Text, Tag, Counter, Acc) ->
    case binary:split(Text, Tag) of
        [Body, Rest] -> fragment_text(Rest, Counter, <<Acc/binary, Body/binary, Tag/binary>>);
        [Unterminated] -> {<<Acc/binary, Unterminated/binary>>, Counter}
    end.

%% `$$` or `$tag$` opens a dollar-quoted string, unless the `$` continues an
%% identifier. `$1` is a parameter, not a tag.
dollar_quote_tag(Rest, Before) ->
    case identifier_byte(last_byte(Before)) of
        true -> none;
        false -> dollar_quote_tag(Rest, <<"$">>, first)
    end.

dollar_quote_tag(<<"$", Rest/binary>>, Tag, _Position) ->
    {ok, <<Tag/binary, "$">>, Rest};
dollar_quote_tag(<<Byte, _/binary>>, _Tag, first) when Byte >= $0, Byte =< $9 ->
    none;
dollar_quote_tag(<<Byte, Rest/binary>>, Tag, _Position) ->
    case identifier_byte(Byte) of
        true -> dollar_quote_tag(Rest, <<Tag/binary, Byte>>, rest);
        false -> none
    end;
dollar_quote_tag(<<>>, _Tag, _Position) ->
    none.

escape_string_prefix(Before) ->
    case Before of
        <<Head:(byte_size(Before) - 1)/binary, E>> when E =:= $E; E =:= $e ->
            not identifier_byte(last_byte(Head));
        _ ->
            false
    end.

last_byte(<<>>) -> $\s;
last_byte(Binary) -> binary:last(Binary).

identifier_byte(Byte) ->
    (Byte >= $a andalso Byte =< $z) orelse (Byte >= $A andalso Byte =< $Z) orelse
        (Byte >= $0 andalso Byte =< $9) orelse Byte =:= $_ orelse Byte >= 128.

qualified_table(Table, undefined) ->
    quote_ident(Table);
qualified_table(Table, Prefix) ->
    [quote_ident(Prefix), ~".", quote_ident(Table)].

resolve_prefix(undefined) ->
    case kura_tenant:get_tenant() of
        {prefix, P} -> P;
        _ -> undefined
    end;
resolve_prefix(Prefix) ->
    Prefix.

%%----------------------------------------------------------------------
%% Internal: typed recursive helpers (replacing lists:foldl to preserve
%% accumulator types for eqWAlizer)
%%----------------------------------------------------------------------

-spec build_insert_parts([atom()], map(), renames(), pos_integer()) ->
    {[iodata()], [iodata()], [term()], pos_integer()}.
build_insert_parts([], _Data, _Renames, N) ->
    {[], [], [], N};
build_insert_parts([Field | Rest], Data, Renames, N) ->
    Value = maps:get(Field, Data),
    Col = column_ident(Field, Renames),
    Placeholder = [~"$", integer_to_binary(N)],
    {Cols, Placeholders, Params, N2} = build_insert_parts(Rest, Data, Renames, N + 1),
    {[Col | Cols], [Placeholder | Placeholders], [Value | Params], N2}.

-spec build_set_parts([atom()], map(), renames(), pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
build_set_parts([], _Data, _Renames, N) ->
    {[], [], N};
build_set_parts([Field | Rest], Data, Renames, N) ->
    Value = maps:get(Field, Data),
    Set = [column_ident(Field, Renames), ~" = $", integer_to_binary(N)],
    {Sets, Params, N2} = build_set_parts(Rest, Data, Renames, N + 1),
    {[Set | Sets], [Value | Params], N2}.

-spec build_value_groups([map()], [atom()], pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
build_value_groups([], _Fields, N) ->
    {[], [], N};
build_value_groups([Row | Rest], Fields, N) ->
    {Placeholders, RowParams, N2} = build_row_placeholders(Fields, Row, N),
    Group = [~"(", join_comma(Placeholders), ~")"],
    {Groups, MoreParams, N3} = build_value_groups(Rest, Fields, N2),
    {[Group | Groups], RowParams ++ MoreParams, N3}.

-spec build_row_placeholders([atom()], map(), pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
build_row_placeholders([], _Row, N) ->
    {[], [], N};
build_row_placeholders([Field | Rest], Row, N) ->
    Value = maps:get(Field, Row),
    Placeholder = [~"$", integer_to_binary(N)],
    {Placeholders, Params, N2} = build_row_placeholders(Rest, Row, N + 1),
    {[Placeholder | Placeholders], [Value | Params], N2}.

-spec compile_select_exprs(
    [
        {
            atom(),
            {fragment, binary(), [term()]}
            | {over, kura_query:window_fn(), kura_query:window_spec()}
            | kura_query:distance()
            | atom()
        }
    ],
    column_context(),
    pos_integer()
) ->
    {[iodata()], [term()], pos_integer()}.
compile_select_exprs([], _Ctx, Counter) ->
    {[], [], Counter};
compile_select_exprs([{Alias, {fragment, SQL, Params}} | Rest], Ctx, Counter) ->
    {RewrittenSQL, C2} = rewrite_fragment_placeholders(SQL, Params, Counter),
    Part = [RewrittenSQL, ~" AS ", quote_ident(atom_to_binary(Alias, utf8))],
    {Parts, MoreParams, C3} = compile_select_exprs(Rest, Ctx, C2),
    {[Part | Parts], Params ++ MoreParams, C3};
compile_select_exprs([{Alias, {distance, _, _, _} = Distance} | Rest], Ctx, Counter) ->
    {DistanceSQL, Params, C2} = compile_distance(Distance, Ctx, Counter),
    Part = [DistanceSQL, ~" AS ", quote_ident(atom_to_binary(Alias, utf8))],
    {Parts, MoreParams, C3} = compile_select_exprs(Rest, Ctx, C2),
    {[Part | Parts], Params ++ MoreParams, C3};
compile_select_exprs([{Alias, {over, WindowFn, Spec}} | Rest], Ctx, Counter) ->
    Part = [
        compile_window_fn(WindowFn, Ctx),
        ~" OVER (",
        compile_over_clause(Spec, Ctx),
        ~")",
        ~" AS ",
        quote_ident(atom_to_binary(Alias, utf8))
    ],
    {Parts, MoreParams, C2} = compile_select_exprs(Rest, Ctx, Counter),
    {[Part | Parts], MoreParams, C2};
compile_select_exprs([{Alias, Field} | Rest], Ctx, Counter) when is_atom(Field) ->
    Part = [
        column(Field, Ctx),
        ~" AS ",
        quote_ident(atom_to_binary(Alias, utf8))
    ],
    {Parts, MoreParams, C2} = compile_select_exprs(Rest, Ctx, Counter),
    {[Part | Parts], MoreParams, C2}.

compile_window_fn({count, '*'}, _Ctx) ->
    [~"count(*)"];
compile_window_fn({Agg, Field}, Ctx) when
    Agg =:= count; Agg =:= sum; Agg =:= avg; Agg =:= min; Agg =:= max
->
    [atom_to_binary(Agg, utf8), ~"(", column(Field, Ctx), ~")"];
compile_window_fn(Fn, _Ctx) when Fn =:= row_number; Fn =:= rank; Fn =:= dense_rank ->
    [atom_to_binary(Fn, utf8), ~"()"].

compile_over_clause(Spec, Ctx) ->
    Partition =
        case maps:get(partition_by, Spec, []) of
            [] ->
                [];
            PFields ->
                [
                    ~"PARTITION BY ",
                    join_comma([column(F, Ctx) || F <- PFields])
                ]
        end,
    Order =
        case maps:get(order_by, Spec, []) of
            [] ->
                [];
            Orders ->
                [
                    ~"ORDER BY ",
                    join_comma([
                        [column(F, Ctx), ~" ", dir_to_sql(D)]
                     || {F, D} <- Orders
                    ])
                ]
        end,
    case {Partition, Order} of
        {[], []} -> [];
        {[], O} -> O;
        {P, []} -> P;
        {P, O} -> [P, ~" ", O]
    end.

-spec compile_joins_loop(
    [{atom(), module() | binary(), {atom(), atom()}, atom() | undefined}],
    binary(),
    renames(),
    binary() | undefined
) -> [iodata()].
compile_joins_loop([], _PrevTable, _PrevRenames, _Prefix) ->
    [];
compile_joins_loop(
    [{Type, JoinTable, {LeftCol, RightCol}, As} | Rest], PrevTable, PrevRenames, Prefix
) ->
    JoinTableBin = resolve_table(JoinTable),
    JoinRenames = renames(JoinTable),
    TableRef =
        case As of
            undefined ->
                qualified_table(JoinTableBin, Prefix);
            Alias ->
                [
                    qualified_table(JoinTableBin, Prefix),
                    ~" AS ",
                    quote_ident(atom_to_binary(Alias, utf8))
                ]
        end,
    JoinRef =
        case As of
            undefined -> JoinTableBin;
            Alias2 -> atom_to_binary(Alias2, utf8)
        end,
    TypeBin = join_type(Type),
    Part = [
        ~" ",
        TypeBin,
        ~" ",
        TableRef,
        ~" ON ",
        quote_ident(PrevTable),
        ~".",
        column_ident(LeftCol, PrevRenames),
        ~" = ",
        quote_ident(JoinRef),
        ~".",
        column_ident(RightCol, JoinRenames)
    ],
    [Part | compile_joins_loop(Rest, JoinRef, JoinRenames, Prefix)].

-spec compile_ctes_loop([{binary(), #kura_query{}}], pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
compile_ctes_loop([], Counter) ->
    {[], [], Counter};
compile_ctes_loop([{Name, CteQuery} | Rest], Counter) ->
    {CteSQL, CteParams, C2} = to_sql_from(CteQuery, Counter),
    Part = [quote_ident(Name), ~" AS (", CteSQL, ~")"],
    {Parts, MoreParams, C3} = compile_ctes_loop(Rest, C2),
    {[Part | Parts], CteParams ++ MoreParams, C3}.

-spec compile_combinations_loop(
    [{union | union_all | intersect | except, #kura_query{}}], pos_integer()
) ->
    {[iodata()], [term()], pos_integer()}.
compile_combinations_loop([], Counter) ->
    {[], [], Counter};
compile_combinations_loop([{Type, Q2} | Rest], Counter) ->
    {SQL2, Params2, C2} = to_sql_from(Q2, Counter),
    TypeBin = combination_type(Type),
    Part = [~" ", TypeBin, ~" ", SQL2],
    {Parts, MoreParams, C3} = compile_combinations_loop(Rest, C2),
    {[Part | Parts], Params2 ++ MoreParams, C3}.

%%----------------------------------------------------------------------
%% DDL: column type + default value emit (used by kura_migrator)
%%----------------------------------------------------------------------

-spec column_type(kura_types:kura_type()) -> binary().
column_type(Type) ->
    kura_types:to_pg_type(Type).

-spec format_default(term()) -> binary().
format_default(Val) when is_integer(Val) ->
    integer_to_binary(Val);
format_default(Val) when is_float(Val) ->
    float_to_binary(Val);
format_default(Val) when is_binary(Val) ->
    quote_literal(Val);
format_default(true) ->
    ~"TRUE";
format_default(false) ->
    ~"FALSE";
format_default(Val) when is_atom(Val) ->
    quote_literal(atom_to_binary(Val));
format_default(Val) when is_map(Val) ->
    jsonb_literal(Val);
format_default(Val) when is_list(Val) ->
    jsonb_literal(Val).

%% A jsonb default is whatever term the migration supplied; json:encode/1
%% raises on any part of it that is not JSON, which no guard can check.
-spec jsonb_literal(dynamic()) -> binary().
jsonb_literal(Val) ->
    <<(quote_literal(iolist_to_binary(json:encode(Val))))/binary, "::jsonb">>.

%% A default is DDL text, not a bind parameter, so an embedded quote has to
%% be doubled or it closes the literal early.
quote_literal(Val) ->
    Escaped = binary:replace(Val, ~"'", ~"''", [global]),
    <<$', Escaped/binary, $'>>.
