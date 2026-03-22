-module(kura_query_compiler).
-moduledoc """
Compiles `kura_query` records into parameterized PostgreSQL SQL.

This is an internal module. Use `kura_repo_worker` for executing queries.
""".

-include("kura.hrl").

-export([
    to_sql/1,
    to_sql_cached/1,
    to_sql_from/2,
    insert/3,
    insert/4,
    update/4,
    delete/3,
    update_all/2,
    delete_all/1,
    insert_all/3,
    insert_all/4
]).

%% eqWAlizer: compile_condition/2 has >16 clauses narrowing on condition union
-eqwalizer({nowarn_function, compile_condition/2}).

-doc "Compile a query record into `{SQL, Params}`.".
-spec to_sql(#kura_query{}) -> {iodata(), [term()]}.
to_sql(Query) ->
    {SQL, Params, _Counter} = to_sql_from(Query, 1),
    {SQL, Params}.

-doc """
Cached version of `to_sql/1`. Caches `{SQL, Params}` keyed by the
query record hash. Identical queries (same structure and values) hit
the cache, avoiding recompilation.
""".
-spec to_sql_cached(#kura_query{}) -> {iodata(), [term()]}.
to_sql_cached(Query) ->
    Key = erlang:phash2(Query),
    case kura_query_cache:get(Key) of
        {ok, Result} ->
            Result;
        miss ->
            Result = to_sql(Query),
            kura_query_cache:put(Key, Result),
            Result
    end.

-doc "Compile a query starting parameter numbering from `StartCounter`. Returns `{SQL, Params, NextCounter}`.".
-spec to_sql_from(#kura_query{}, pos_integer()) -> {iodata(), [term()], pos_integer()}.
to_sql_from(#kura_query{from = From} = Q, StartCounter) when From =/= undefined ->
    %% CTEs
    {CteSQL, CteParams, Counter00} = compile_ctes(Q#kura_query.ctes, StartCounter),

    Table = resolve_table(From),
    {SelectSQL, Params0, Counter0} = compile_select(Q, Counter00),
    {WhereSQL, Params1, Counter1} = compile_wheres(Q#kura_query.wheres, Counter0),
    Prefix = resolve_prefix(Q#kura_query.prefix),
    {JoinSQL, Params2, Counter2} = compile_joins(Q#kura_query.joins, Table, Prefix, Counter1),
    {GroupSQL, _, Counter3} = compile_group_by(Q#kura_query.group_bys, Counter2),
    {HavingSQL, Params3, Counter4} = compile_havings(Q#kura_query.havings, Counter3),
    {OrderSQL, _, Counter5} = compile_order_by(Q#kura_query.order_bys, Counter4),
    {LimitSQL, Params4, Counter6} = compile_limit(Q#kura_query.limit, Counter5),
    {OffsetSQL, Params5, Counter7} = compile_offset(Q#kura_query.offset, Counter6),
    LockSQL = compile_lock(Q#kura_query.lock),
    DistinctSQL = compile_distinct(Q#kura_query.distinct),

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
    MainParams = CteParams ++ Params0 ++ Params2 ++ Params1 ++ Params3 ++ Params4 ++ Params5,

    %% Combinations (UNION, INTERSECT, EXCEPT)
    {FinalSQL, FinalParams, FinalCounter} = compile_combinations(
        Q#kura_query.combinations, MainSQL, MainParams, Counter7
    ),
    {FinalSQL, FinalParams, FinalCounter}.

%%----------------------------------------------------------------------
%% INSERT
%%----------------------------------------------------------------------

-spec insert(atom() | module(), [atom()], map()) -> {iodata(), [term()]}.
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

-spec insert(atom() | module(), [atom()], map(), map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data, #{on_conflict := OnConflict}) ->
    Table = resolve_table(SchemaOrTable),
    {Cols, Placeholders, Params, Counter} = build_insert_parts(Fields, Data, 1),
    {ConflictSQL, ConflictParams} = compile_on_conflict(OnConflict, Fields, Data, Counter),
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

-spec update(atom() | module(), [atom()], map(), {atom(), term()}) -> {iodata(), [term()]}.
update(SchemaOrTable, Fields, Changes, {PKField, PKValue}) ->
    Table = resolve_table(SchemaOrTable),
    {Sets, Params, Counter} = build_set_parts(Fields, Changes, 1),
    PKPlaceholder = [~"$", integer_to_binary(Counter)],
    SQL = iolist_to_binary([
        ~"UPDATE ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" SET ",
        join_comma(Sets),
        ~" WHERE ",
        quote_ident(atom_to_binary(PKField, utf8)),
        ~" = ",
        PKPlaceholder,
        ~" RETURNING *"
    ]),
    {SQL, Params ++ [PKValue]}.

%%----------------------------------------------------------------------
%% DELETE
%%----------------------------------------------------------------------

-spec delete(atom() | module(), atom(), term()) -> {iodata(), [term()]}.
delete(SchemaOrTable, PKField, PKValue) ->
    Table = resolve_table(SchemaOrTable),
    SQL = iolist_to_binary([
        ~"DELETE FROM ",
        qualified_table(Table, resolve_prefix(undefined)),
        ~" WHERE ",
        quote_ident(atom_to_binary(PKField, utf8)),
        ~" = $1",
        ~" RETURNING *"
    ]),
    {SQL, [PKValue]}.

%%----------------------------------------------------------------------
%% UPDATE ALL (bulk)
%%----------------------------------------------------------------------

-spec update_all(#kura_query{}, map()) -> {iodata(), [term()]}.
update_all(#kura_query{from = From, wheres = Wheres, prefix = QPrefix}, SetMap) ->
    Table = resolve_table(From),
    Prefix = resolve_prefix(QPrefix),
    Fields = maps:keys(SetMap),
    {Sets, Params, Counter} = build_set_parts(Fields, SetMap, 1),
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, Counter),
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
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, 1),
    SQL = iolist_to_binary([
        ~"DELETE FROM ",
        qualified_table(Table, Prefix),
        WhereSQL
    ]),
    {SQL, WhereParams}.

%%----------------------------------------------------------------------
%% INSERT ALL (bulk)
%%----------------------------------------------------------------------

-spec insert_all(atom() | module(), [atom()], [map()]) -> {iodata(), [term()]}.
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

-spec insert_all(atom() | module(), [atom()], [map()], map()) -> {iodata(), [term()]}.
insert_all(SchemaOrTable, Fields, Rows, #{returning := true}) ->
    {BaseSQL, Params} = insert_all(SchemaOrTable, Fields, Rows),
    {iolist_to_binary([BaseSQL, ~" RETURNING *"]), Params};
insert_all(SchemaOrTable, Fields, Rows, #{returning := RetFields}) when is_list(RetFields) ->
    {BaseSQL, Params} = insert_all(SchemaOrTable, Fields, Rows),
    Cols = join_comma([quote_ident(atom_to_binary(F, utf8)) || F <- RetFields]),
    {iolist_to_binary([BaseSQL, ~" RETURNING ", Cols]), Params};
insert_all(SchemaOrTable, Fields, Rows, _Opts) ->
    insert_all(SchemaOrTable, Fields, Rows).

%%----------------------------------------------------------------------
%% Internal: SELECT clause
%%----------------------------------------------------------------------

compile_select(#kura_query{select = []}, Counter) ->
    {~"*", [], Counter};
compile_select(#kura_query{select = {exprs, Exprs}}, Counter) ->
    {Parts, AllParams, NewCounter} = compile_select_exprs(Exprs, Counter),
    {join_comma(Parts), AllParams, NewCounter};
compile_select(#kura_query{select = Fields}, Counter) ->
    Parts = [compile_select_field(F) || F <- Fields],
    {join_comma(Parts), [], Counter}.

compile_select_field({Agg, '*'}) when Agg =:= count ->
    [atom_to_binary(Agg, utf8), ~"(*)", ~" AS ", quote_ident(atom_to_binary(Agg, utf8))];
compile_select_field({Agg, Field}) when
    Agg =:= count; Agg =:= sum; Agg =:= avg; Agg =:= min; Agg =:= max
->
    [
        atom_to_binary(Agg, utf8),
        ~"(",
        quote_ident(atom_to_binary(Field, utf8)),
        ~")",
        ~" AS ",
        quote_ident(atom_to_binary(Agg, utf8))
    ];
compile_select_field(Field) when is_atom(Field) ->
    quote_ident(atom_to_binary(Field, utf8)).

%%----------------------------------------------------------------------
%% Internal: WHERE clause
%%----------------------------------------------------------------------

compile_wheres([], Counter) ->
    {<<>>, [], Counter};
compile_wheres(Conditions, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    SQL = [~" WHERE ", join_and(Parts)],
    {iolist_to_binary(SQL), Params, NewCounter}.

compile_conditions([], Counter) ->
    {[], [], Counter};
compile_conditions([Cond | Rest], Counter) ->
    {Part, Vars, Counter1} = compile_condition(Cond, Counter),
    {Parts, MoreVars, Counter2} = compile_conditions(Rest, Counter1),
    {[Part | Parts], Vars ++ MoreVars, Counter2}.

compile_condition({'and', Conditions}, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    {[~"(", join_and(Parts), ~")"], Params, NewCounter};
compile_condition({'or', Conditions}, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    {[~"(", join_or(Parts), ~")"], Params, NewCounter};
compile_condition({'not', Condition}, Counter) ->
    {Part, Params, NewCounter} = compile_condition(Condition, Counter),
    {[~"NOT (", Part, ~")"], Params, NewCounter};
compile_condition({fragment, SQL, Params}, Counter) ->
    {RewrittenSQL, NewCounter} = rewrite_fragment_placeholders(SQL, Counter),
    {RewrittenSQL, Params, NewCounter};
compile_condition({Field, is_nil}, Counter) when is_atom(Field) ->
    {[quote_ident(atom_to_binary(Field, utf8)), ~" IS NULL"], [], Counter};
compile_condition({Field, is_not_nil}, Counter) when is_atom(Field) ->
    {[quote_ident(atom_to_binary(Field, utf8)), ~" IS NOT NULL"], [], Counter};
compile_condition({Field, '=', Value}, Counter) when is_atom(Field) ->
    compile_condition({Field, Value}, Counter);
compile_condition({Field, '!=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" != $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" < $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" > $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" <= $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" >= $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, like, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" LIKE $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, ilike, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" ILIKE $", integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, in, {subquery, SubQ}}, Counter) when is_atom(Field) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[quote_ident(atom_to_binary(Field, utf8)), ~" IN (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({exists, {subquery, SubQ}}, Counter) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[~"EXISTS (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({not_exists, {subquery, SubQ}}, Counter) ->
    {SubSQL, SubParams, Counter2} = to_sql_from(SubQ, Counter),
    {[~"NOT EXISTS (", SubSQL, ~")"], SubParams, Counter2};
compile_condition({Field, in, Values}, Counter) when is_atom(Field), is_list(Values) ->
    {Placeholders, NewCounter} = lists:foldl(
        fun(_, {Acc, N}) ->
            {Acc ++ [[~"$", integer_to_binary(N)]], N + 1}
        end,
        {[], Counter},
        Values
    ),
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" IN (", join_comma(Placeholders), ~")"],
        Values,
        NewCounter
    };
compile_condition({Field, not_in, Values}, Counter) when is_atom(Field), is_list(Values) ->
    {Placeholders, NewCounter} = lists:foldl(
        fun(_, {Acc, N}) ->
            {Acc ++ [[~"$", integer_to_binary(N)]], N + 1}
        end,
        {[], Counter},
        Values
    ),
    {
        [
            quote_ident(atom_to_binary(Field, utf8)),
            ~" NOT IN (",
            join_comma(Placeholders),
            ~")"
        ],
        Values,
        NewCounter
    };
compile_condition({Field, between, {Low, High}}, Counter) when is_atom(Field) ->
    {
        [
            quote_ident(atom_to_binary(Field, utf8)),
            ~" BETWEEN $",
            integer_to_binary(Counter),
            ~" AND $",
            integer_to_binary(Counter + 1)
        ],
        [Low, High],
        Counter + 2
    };
compile_condition({Field, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), ~" = $", integer_to_binary(Counter)],
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

compile_order_by([], Counter) ->
    {<<>>, [], Counter};
compile_order_by(Orders, Counter) ->
    Parts = [
        [quote_ident(atom_to_binary(Field, utf8)), ~" ", dir_to_sql(Dir)]
     || {Field, Dir} <- Orders
    ],
    {iolist_to_binary([~" ORDER BY ", join_comma(Parts)]), [], Counter}.

dir_to_sql(asc) -> ~"ASC";
dir_to_sql(desc) -> ~"DESC".

%%----------------------------------------------------------------------
%% Internal: GROUP BY clause
%%----------------------------------------------------------------------

compile_group_by([], Counter) ->
    {<<>>, [], Counter};
compile_group_by(Fields, Counter) ->
    Parts = [quote_ident(atom_to_binary(F, utf8)) || F <- Fields],
    {iolist_to_binary([~" GROUP BY ", join_comma(Parts)]), [], Counter}.

%%----------------------------------------------------------------------
%% Internal: HAVING clause
%%----------------------------------------------------------------------

compile_havings([], Counter) ->
    {<<>>, [], Counter};
compile_havings(Conditions, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
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

compile_distinct(false) ->
    <<>>;
compile_distinct(true) ->
    ~"DISTINCT ";
compile_distinct(Fields) when is_list(Fields) ->
    Parts = [quote_ident(atom_to_binary(F, utf8)) || F <- Fields],
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

compile_on_conflict({Field, nothing}, _Fields, _Data, _Counter) when is_atom(Field) ->
    {[~" ON CONFLICT (", quote_ident(atom_to_binary(Field, utf8)), ~") DO NOTHING"], []};
compile_on_conflict({{constraint, Name}, nothing}, _Fields, _Data, _Counter) ->
    {[~" ON CONFLICT ON CONSTRAINT ", quote_ident(Name), ~" DO NOTHING"], []};
compile_on_conflict({Field, replace_all}, Fields, Data, Counter) when is_atom(Field) ->
    UpdateFields = [F || F <- Fields, F =/= Field],
    compile_on_conflict({Field, {replace, UpdateFields}}, Fields, Data, Counter);
compile_on_conflict({{constraint, Name}, replace_all}, Fields, Data, Counter) ->
    compile_on_conflict_update(
        [~" ON CONFLICT ON CONSTRAINT ", quote_ident(Name)], Fields, Data, Counter
    );
compile_on_conflict({Field, {replace, UpdateFields}}, _Fields, Data, Counter) when is_atom(Field) ->
    compile_on_conflict_update(
        [~" ON CONFLICT (", quote_ident(atom_to_binary(Field, utf8)), ~")"],
        UpdateFields,
        Data,
        Counter
    );
compile_on_conflict({{constraint, Name}, {replace, UpdateFields}}, _Fields, Data, Counter) ->
    compile_on_conflict_update(
        [~" ON CONFLICT ON CONSTRAINT ", quote_ident(Name)],
        UpdateFields,
        Data,
        Counter
    ).

compile_on_conflict_update(ConflictTarget, UpdateFields, Data, Counter) ->
    {Sets, Params, _} = build_set_parts(UpdateFields, Data, Counter),
    SQL = [ConflictTarget, ~" DO UPDATE SET ", join_comma(Sets)],
    {SQL, Params}.

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

quote_ident(Name) when is_binary(Name) ->
    <<$", Name/binary, $">>.

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

-spec compile_select_exprs([{atom(), {fragment, binary(), [term()]}}], pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
compile_select_exprs([], Counter) ->
    {[], [], Counter};
compile_select_exprs([{Alias, {fragment, SQL, Params}} | Rest], Counter) ->
    {RewrittenSQL, C2} = rewrite_fragment_placeholders(SQL, Counter),
    Part = [RewrittenSQL, ~" AS ", quote_ident(atom_to_binary(Alias, utf8))],
    {Parts, MoreParams, C3} = compile_select_exprs(Rest, C2),
    {[Part | Parts], Params ++ MoreParams, C3}.

-spec compile_joins_loop(
    [{atom(), atom() | module(), {atom(), atom()}, atom() | undefined}],
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
    Part = [Name, ~" AS (", CteSQL, ~")"],
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
