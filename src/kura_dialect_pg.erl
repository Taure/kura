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

-type column_context() :: undefined | {binary(), #{atom() => kura_types:kura_type()}}.

-export([
    to_sql/1,
    to_sql_from/2,
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
    {JoinSQL, Params2, Counter2} = compile_joins(Q#kura_query.joins, Table, Prefix, Counter1),
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
%% INSERT
%%----------------------------------------------------------------------

-spec insert(kura_dialect:table(), [atom()], map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data) ->
    Table = resolve_table(SchemaOrTable),
    {Cols, Placeholders, Params, _} = build_insert_parts(Fields, Data, 1),
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
    {OnConflict, Guard} = conflict_guard(OnConflict0),
    {Cols, Placeholders, Params, Counter} = build_insert_parts(Fields, Data, 1),
    {ConflictSQL0, ConflictParams} = compile_on_conflict(OnConflict, Fields, Data, Counter),
    ConflictSQL = [ConflictSQL0, guard_sql(Table, OnConflict, Guard)],
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
    {Sets, Params, Counter} = build_set_parts(Fields, Changes, 1),
    {KeyConds, KeyParams, _} = key_where(KeyClauses, Counter),
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
key_where(KeyClauses, Start) ->
    key_where(KeyClauses, Start, [], []).

key_where([], Counter, Conds, Params) ->
    {lists:reverse(Conds), lists:reverse(Params), Counter};
key_where([{Field, Value} | Rest], Counter, Conds, Params) ->
    Cond = [quote_ident(atom_to_binary(Field, utf8)), ~" = $", integer_to_binary(Counter)],
    key_where(Rest, Counter + 1, [Cond | Conds], [Value | Params]).

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
    {KeyConds, Params, _} = key_where(KeyClauses, 1),
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
    {Sets, Params, Counter} = build_set_parts(Fields, SetMap, 1),
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, undefined, Counter),
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
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, undefined, 1),
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
    Table = resolve_table(SchemaOrTable),
    Cols = [quote_ident(atom_to_binary(F, utf8)) || F <- Fields],
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

-spec insert_all(kura_dialect:table(), [atom()], [map()], map()) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows, Opts) ->
    {BaseSQL, Params} = insert_all(SchemaOrTable, Fields, Rows),
    SQL = [
        BaseSQL,
        guarded_bulk_on_conflict(resolve_table(SchemaOrTable), Opts, Fields),
        bulk_returning(Opts)
    ],
    {iolist_to_binary(SQL), Params}.

guarded_bulk_on_conflict(Table, #{on_conflict := OnConflict0} = Opts, Fields) ->
    {OnConflict, Guard} = conflict_guard(OnConflict0),
    [
        bulk_on_conflict(Opts#{on_conflict => OnConflict}, Fields),
        guard_sql(Table, OnConflict, Guard)
    ];
guarded_bulk_on_conflict(_Table, Opts, Fields) ->
    bulk_on_conflict(Opts, Fields).

%% A bulk upsert applies one DO UPDATE clause to every row, so the SET values
%% cannot come from any single row's parameters the way the single-row form
%% builds them. EXCLUDED names the row that failed to insert, which is what
%% makes one clause correct for all of them - and it binds no parameters.
bulk_on_conflict(#{on_conflict := {Target, nothing}}, _Fields) ->
    [conflict_target(Target), ~" DO NOTHING"];
bulk_on_conflict(#{on_conflict := {Target, replace_all}}, Fields) ->
    bulk_on_conflict(#{on_conflict => {Target, {replace, replaceable(Target, Fields)}}}, Fields);
bulk_on_conflict(#{on_conflict := {Target, {replace, UpdateFields}}}, _Fields) ->
    [
        conflict_target(Target),
        ~" DO UPDATE SET ",
        join_comma([excluded_set(F) || F <- UpdateFields])
    ];
bulk_on_conflict(#{}, _Fields) ->
    [].

excluded_set(Field) ->
    Col = quote_ident(atom_to_binary(Field, utf8)),
    [Col, ~" = EXCLUDED.", Col].

bulk_returning(#{returning := true}) ->
    ~" RETURNING *";
bulk_returning(#{returning := RetFields}) when is_list(RetFields) ->
    [~" RETURNING ", join_comma([quote_ident(atom_to_binary(F, utf8)) || F <- RetFields])];
bulk_returning(#{}) ->
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
    {RewrittenSQL, NewCounter} = rewrite_fragment_placeholders(SQL, Counter),
    {RewrittenSQL, Params, NewCounter};
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

compile_joins([], _Table, _Prefix, Counter) ->
    {<<>>, [], Counter};
compile_joins(Joins, FromTable, Prefix, Counter) ->
    Parts = compile_joins_loop(Joins, FromTable, Prefix),
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
compile_lock(Lock) -> <<" ", Lock/binary>>.

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

compile_on_conflict({Target, nothing}, _Fields, _Data, _Counter) ->
    {[conflict_target(Target), ~" DO NOTHING"], []};
compile_on_conflict({Target, replace_all}, Fields, Data, Counter) ->
    compile_on_conflict({Target, {replace, replaceable(Target, Fields)}}, Fields, Data, Counter);
compile_on_conflict({Target, {replace, UpdateFields}}, _Fields, Data, Counter) ->
    compile_on_conflict_update(conflict_target(Target), UpdateFields, Data, Counter).

%% A row the guard refuses is left as it is and not returned, as with DO NOTHING.
conflict_guard({Target, Action, #{guard := Fields} = Opts}) when
    is_list(Fields), map_size(Opts) =:= 1
->
    {{Target, Action}, Fields};
conflict_guard({_Target, _Action, Opts}) ->
    erlang:error({kura, {invalid_on_conflict_opts, Opts}});
conflict_guard(OnConflict) ->
    {OnConflict, []}.

guard_sql(_Table, {_Target, nothing}, _Fields) ->
    [];
guard_sql(_Table, _OnConflict, []) ->
    [];
guard_sql(Table, _OnConflict, Fields) ->
    [~" WHERE ", lists:join(~" AND ", [guard_condition(Table, F) || F <- Fields])].

guard_condition(Table, Field) ->
    Column = quote_ident(atom_to_binary(Field, utf8)),
    [quote_ident(Table), ~".", Column, ~" = EXCLUDED.", Column].

conflict_target(Field) when is_atom(Field) ->
    [~" ON CONFLICT (", quote_ident(atom_to_binary(Field, utf8)), ~")"];
conflict_target({columns, Columns}) when is_list(Columns) ->
    [~" ON CONFLICT (", columns_target(Columns), ~")"];
conflict_target({columns, Columns, Opts}) when is_list(Columns), is_map(Opts) ->
    [~" ON CONFLICT (", columns_target(Columns), ~")", index_predicate(Opts)];
conflict_target({constraint, Name}) when is_binary(Name) ->
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

columns_target(Columns) ->
    join_comma([quote_ident(atom_to_binary(C, utf8)) || C <- Columns]).

index_predicate(#{where := Expr}) when is_binary(Expr) ->
    [~" WHERE ", Expr];
index_predicate(Opts) when map_size(Opts) =:= 0 ->
    [];
index_predicate(Opts) ->
    erlang:error({kura, {invalid_on_conflict_opts, Opts}}).

compile_on_conflict_update(ConflictTarget, UpdateFields, Data, Counter) ->
    {Sets, Params, _} = build_set_parts(UpdateFields, Data, Counter),
    SQL = [ConflictTarget, ~" DO UPDATE SET ", join_comma(Sets)],
    {SQL, Params}.

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
column_context(Q) ->
    case kura_dialect:qualified_columns(Q) of
        undefined -> undefined;
        {Table, Fields} -> {quote_ident(Table), Fields}
    end.

%% ORDER BY resolves a name to a select_expr output column before an input
%% column, so an alias is left bare to keep naming the expression.
-spec order_context(#kura_query{}, column_context()) -> column_context().
order_context(#kura_query{select = {exprs, Exprs}}, {Qualifier, Fields}) ->
    {Qualifier, maps:without([Alias || {Alias, _} <- Exprs], Fields)};
order_context(_Q, Ctx) ->
    Ctx.

-spec column(atom(), column_context()) -> iodata().
column(Field, {Qualifier, Fields}) when is_map_key(Field, Fields) ->
    [Qualifier, ~".", quote_ident(atom_to_binary(Field, utf8))];
column(Field, _Ctx) ->
    quote_ident(atom_to_binary(Field, utf8)).

quote_ident(Name) when is_binary(Name) ->
    Escaped = binary:replace(Name, ~"\"", ~"\"\"", [global]),
    <<$", Escaped/binary, $">>.

join_comma(Parts) ->
    lists:join(~", ", Parts).

join_and(Parts) ->
    lists:join(~" AND ", Parts).

join_or(Parts) ->
    lists:join(~" OR ", Parts).

rewrite_fragment_placeholders(SQL, StartCounter) when is_binary(SQL) ->
    rewrite_fragment(SQL, StartCounter, <<>>).

rewrite_fragment(<<>>, Counter, Acc) ->
    {Acc, Counter};
rewrite_fragment(<<"?", Rest/binary>>, Counter, Acc) ->
    Placeholder = [~"$", integer_to_binary(Counter)],
    rewrite_fragment(Rest, Counter + 1, <<Acc/binary, (iolist_to_binary(Placeholder))/binary>>);
rewrite_fragment(<<C, Rest/binary>>, Counter, Acc) ->
    rewrite_fragment(Rest, Counter, <<Acc/binary, C>>).

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

-spec build_insert_parts([atom()], map(), pos_integer()) ->
    {[iodata()], [iodata()], [term()], pos_integer()}.
build_insert_parts([], _Data, N) ->
    {[], [], [], N};
build_insert_parts([Field | Rest], Data, N) ->
    Value = maps:get(Field, Data),
    Col = quote_ident(atom_to_binary(Field, utf8)),
    Placeholder = [~"$", integer_to_binary(N)],
    {Cols, Placeholders, Params, N2} = build_insert_parts(Rest, Data, N + 1),
    {[Col | Cols], [Placeholder | Placeholders], [Value | Params], N2}.

-spec build_set_parts([atom()], map(), pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
build_set_parts([], _Data, N) ->
    {[], [], N};
build_set_parts([Field | Rest], Data, N) ->
    Value = maps:get(Field, Data),
    Set = [quote_ident(atom_to_binary(Field, utf8)), ~" = $", integer_to_binary(N)],
    {Sets, Params, N2} = build_set_parts(Rest, Data, N + 1),
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
    {RewrittenSQL, C2} = rewrite_fragment_placeholders(SQL, Counter),
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
    binary() | undefined
) -> [iodata()].
compile_joins_loop([], _PrevTable, _Prefix) ->
    [];
compile_joins_loop([{Type, JoinTable, {LeftCol, RightCol}, As} | Rest], PrevTable, Prefix) ->
    JoinTableBin = resolve_table(JoinTable),
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
        quote_ident(atom_to_binary(LeftCol, utf8)),
        ~" = ",
        quote_ident(JoinRef),
        ~".",
        quote_ident(atom_to_binary(RightCol, utf8))
    ],
    [Part | compile_joins_loop(Rest, JoinRef, Prefix)].

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
