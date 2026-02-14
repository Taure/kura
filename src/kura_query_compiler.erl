-module(kura_query_compiler).
-moduledoc """
Compiles `kura_query` records into parameterized PostgreSQL SQL.

This is an internal module. Use `kura_repo_worker` for executing queries.
""".

-include("kura.hrl").

-export([to_sql/1, insert/3, insert/4, update/4, delete/3, update_all/2, delete_all/1, insert_all/3]).

-doc "Compile a query record into `{SQL, Params}`.".
-spec to_sql(#kura_query{}) -> {iodata(), [term()]}.
to_sql(#kura_query{from = From} = Q) when From =/= undefined ->
    Table = resolve_table(From),
    {SelectSQL, Params0, Counter0} = compile_select(Q, 1),
    {WhereSQL, Params1, Counter1} = compile_wheres(Q#kura_query.wheres, Counter0),
    {JoinSQL, Params2, Counter2} = compile_joins(Q#kura_query.joins, Table, Counter1),
    {GroupSQL, _, Counter3} = compile_group_by(Q#kura_query.group_bys, Counter2),
    {HavingSQL, Params3, Counter4} = compile_havings(Q#kura_query.havings, Counter3),
    {OrderSQL, _, Counter5} = compile_order_by(Q#kura_query.order_bys, Counter4),
    {LimitSQL, Params4, Counter6} = compile_limit(Q#kura_query.limit, Counter5),
    {OffsetSQL, Params5, Counter7} = compile_offset(Q#kura_query.offset, Counter6),
    LockSQL = compile_lock(Q#kura_query.lock),
    DistinctSQL = compile_distinct(Q#kura_query.distinct),
    _ = Counter7,

    FromClause =
        case Q#kura_query.prefix of
            undefined -> [<<"FROM ">>, quote_ident(Table)];
            Prefix -> [<<"FROM ">>, quote_ident(Prefix), <<".">>, quote_ident(Table)]
        end,

    SQL = iolist_to_binary([
        <<"SELECT ">>,
        DistinctSQL,
        SelectSQL,
        <<" ">>,
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
    AllParams = Params0 ++ Params2 ++ Params1 ++ Params3 ++ Params4 ++ Params5,
    {SQL, AllParams}.

%%----------------------------------------------------------------------
%% INSERT
%%----------------------------------------------------------------------

-spec insert(atom() | module(), [atom()], map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data) ->
    Table = resolve_table(SchemaOrTable),
    {Cols, Placeholders, Params, _} = lists:foldl(
        fun(Field, {CAcc, PAcc, VAcc, N}) ->
            Value = maps:get(Field, Data),
            Col = quote_ident(atom_to_binary(Field, utf8)),
            Placeholder = [<<"$">>, integer_to_binary(N)],
            {[Col | CAcc], [Placeholder | PAcc], [Value | VAcc], N + 1}
        end,
        {[], [], [], 1},
        Fields
    ),
    SQL = iolist_to_binary([
        <<"INSERT INTO ">>,
        quote_ident(Table),
        <<" (">>,
        join_comma(lists:reverse(Cols)),
        <<") VALUES (">>,
        join_comma(lists:reverse(Placeholders)),
        <<") RETURNING *">>
    ]),
    {SQL, lists:reverse(Params)}.

-spec insert(atom() | module(), [atom()], map(), map()) -> {iodata(), [term()]}.
insert(SchemaOrTable, Fields, Data, #{on_conflict := OnConflict}) ->
    Table = resolve_table(SchemaOrTable),
    {Cols, Placeholders, Params, Counter} = lists:foldl(
        fun(Field, {CAcc, PAcc, VAcc, N}) ->
            Value = maps:get(Field, Data),
            Col = quote_ident(atom_to_binary(Field, utf8)),
            Placeholder = [<<"$">>, integer_to_binary(N)],
            {[Col | CAcc], [Placeholder | PAcc], [Value | VAcc], N + 1}
        end,
        {[], [], [], 1},
        Fields
    ),
    {ConflictSQL, ConflictParams} = compile_on_conflict(OnConflict, Fields, Data, Counter),
    SQL = iolist_to_binary([
        <<"INSERT INTO ">>,
        quote_ident(Table),
        <<" (">>,
        join_comma(lists:reverse(Cols)),
        <<") VALUES (">>,
        join_comma(lists:reverse(Placeholders)),
        <<")">>,
        ConflictSQL,
        <<" RETURNING *">>
    ]),
    {SQL, lists:reverse(Params) ++ ConflictParams};
insert(SchemaOrTable, Fields, Data, _Opts) ->
    insert(SchemaOrTable, Fields, Data).

%%----------------------------------------------------------------------
%% UPDATE
%%----------------------------------------------------------------------

-spec update(atom() | module(), [atom()], map(), {atom(), term()}) -> {iodata(), [term()]}.
update(SchemaOrTable, Fields, Changes, {PKField, PKValue}) ->
    Table = resolve_table(SchemaOrTable),
    {Sets, Params, Counter} = lists:foldl(
        fun(Field, {SAcc, PAcc, N}) ->
            Value = maps:get(Field, Changes),
            Set = [quote_ident(atom_to_binary(Field, utf8)), <<" = $">>, integer_to_binary(N)],
            {[Set | SAcc], [Value | PAcc], N + 1}
        end,
        {[], [], 1},
        Fields
    ),
    PKPlaceholder = [<<"$">>, integer_to_binary(Counter)],
    SQL = iolist_to_binary([
        <<"UPDATE ">>,
        quote_ident(Table),
        <<" SET ">>,
        join_comma(lists:reverse(Sets)),
        <<" WHERE ">>,
        quote_ident(atom_to_binary(PKField, utf8)),
        <<" = ">>,
        PKPlaceholder,
        <<" RETURNING *">>
    ]),
    {SQL, lists:reverse(Params) ++ [PKValue]}.

%%----------------------------------------------------------------------
%% DELETE
%%----------------------------------------------------------------------

-spec delete(atom() | module(), atom(), term()) -> {iodata(), [term()]}.
delete(SchemaOrTable, PKField, PKValue) ->
    Table = resolve_table(SchemaOrTable),
    SQL = iolist_to_binary([
        <<"DELETE FROM ">>,
        quote_ident(Table),
        <<" WHERE ">>,
        quote_ident(atom_to_binary(PKField, utf8)),
        <<" = $1">>,
        <<" RETURNING *">>
    ]),
    {SQL, [PKValue]}.

%%----------------------------------------------------------------------
%% UPDATE ALL (bulk)
%%----------------------------------------------------------------------

-spec update_all(#kura_query{}, map()) -> {iodata(), [term()]}.
update_all(#kura_query{from = From, wheres = Wheres}, SetMap) ->
    Table = resolve_table(From),
    Fields = maps:keys(SetMap),
    {Sets, Params, Counter} = lists:foldl(
        fun(Field, {SAcc, PAcc, N}) ->
            Value = maps:get(Field, SetMap),
            Set = [quote_ident(atom_to_binary(Field, utf8)), <<" = $">>, integer_to_binary(N)],
            {[Set | SAcc], [Value | PAcc], N + 1}
        end,
        {[], [], 1},
        Fields
    ),
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, Counter),
    SQL = iolist_to_binary([
        <<"UPDATE ">>,
        quote_ident(Table),
        <<" SET ">>,
        join_comma(lists:reverse(Sets)),
        WhereSQL
    ]),
    {SQL, lists:reverse(Params) ++ WhereParams}.

%%----------------------------------------------------------------------
%% DELETE ALL (bulk)
%%----------------------------------------------------------------------

-spec delete_all(#kura_query{}) -> {iodata(), [term()]}.
delete_all(#kura_query{from = From, wheres = Wheres}) ->
    Table = resolve_table(From),
    {WhereSQL, WhereParams, _} = compile_wheres(Wheres, 1),
    SQL = iolist_to_binary([
        <<"DELETE FROM ">>,
        quote_ident(Table),
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
    {ValueGroups, AllParams, _} = lists:foldl(
        fun(Row, {GroupAcc, ParamAcc, N}) ->
            {Placeholders, RowParams, N2} = lists:foldl(
                fun(Field, {PlAcc, RPAcc, Cnt}) ->
                    Value = maps:get(Field, Row),
                    Placeholder = [<<"$">>, integer_to_binary(Cnt)],
                    {[Placeholder | PlAcc], [Value | RPAcc], Cnt + 1}
                end,
                {[], [], N},
                Fields
            ),
            Group = [<<"(">>, join_comma(lists:reverse(Placeholders)), <<")">>],
            {[Group | GroupAcc], ParamAcc ++ lists:reverse(RowParams), N2}
        end,
        {[], [], 1},
        Rows
    ),
    SQL = iolist_to_binary([
        <<"INSERT INTO ">>,
        quote_ident(Table),
        <<" (">>,
        join_comma(Cols),
        <<") VALUES ">>,
        join_comma(lists:reverse(ValueGroups))
    ]),
    {SQL, AllParams}.

%%----------------------------------------------------------------------
%% Internal: SELECT clause
%%----------------------------------------------------------------------

compile_select(#kura_query{select = []}, Counter) ->
    {<<"*">>, [], Counter};
compile_select(#kura_query{select = Fields}, Counter) ->
    Parts = [compile_select_field(F) || F <- Fields],
    {join_comma(Parts), [], Counter}.

compile_select_field({Agg, '*'}) when Agg =:= count ->
    [atom_to_binary(Agg, utf8), <<"(*)">>, <<" AS ">>, quote_ident(atom_to_binary(Agg, utf8))];
compile_select_field({Agg, Field}) when
    Agg =:= count; Agg =:= sum; Agg =:= avg; Agg =:= min; Agg =:= max
->
    [
        atom_to_binary(Agg, utf8),
        <<"(">>,
        quote_ident(atom_to_binary(Field, utf8)),
        <<")">>,
        <<" AS ">>,
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
    SQL = [<<" WHERE ">>, join_and(Parts)],
    {iolist_to_binary(SQL), Params, NewCounter}.

compile_conditions(Conditions, Counter) ->
    lists:foldl(
        fun(Cond, {PAcc, VarAcc, C}) ->
            {Part, Vars, NewC} = compile_condition(Cond, C),
            {PAcc ++ [Part], VarAcc ++ Vars, NewC}
        end,
        {[], [], Counter},
        Conditions
    ).

compile_condition({'and', Conditions}, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    {[<<"(">>, join_and(Parts), <<")">>], Params, NewCounter};
compile_condition({'or', Conditions}, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    {[<<"(">>, join_or(Parts), <<")">>], Params, NewCounter};
compile_condition({'not', Condition}, Counter) ->
    {Part, Params, NewCounter} = compile_condition(Condition, Counter),
    {[<<"NOT (">>, Part, <<")">>], Params, NewCounter};
compile_condition({fragment, SQL, Params}, Counter) ->
    {RewrittenSQL, NewCounter} = rewrite_fragment_placeholders(SQL, Counter),
    {RewrittenSQL, Params, NewCounter};
compile_condition({Field, is_nil}, Counter) when is_atom(Field) ->
    {[quote_ident(atom_to_binary(Field, utf8)), <<" IS NULL">>], [], Counter};
compile_condition({Field, is_not_nil}, Counter) when is_atom(Field) ->
    {[quote_ident(atom_to_binary(Field, utf8)), <<" IS NOT NULL">>], [], Counter};
compile_condition({Field, '=', Value}, Counter) when is_atom(Field) ->
    compile_condition({Field, Value}, Counter);
compile_condition({Field, '!=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" != $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" < $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" > $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '<=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" <= $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, '>=', Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" >= $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, like, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" LIKE $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, ilike, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" ILIKE $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    };
compile_condition({Field, in, Values}, Counter) when is_atom(Field), is_list(Values) ->
    {Placeholders, NewCounter} = lists:foldl(
        fun(_, {Acc, N}) ->
            {Acc ++ [[<<"$">>, integer_to_binary(N)]], N + 1}
        end,
        {[], Counter},
        Values
    ),
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" IN (">>, join_comma(Placeholders), <<")">>],
        Values,
        NewCounter
    };
compile_condition({Field, not_in, Values}, Counter) when is_atom(Field), is_list(Values) ->
    {Placeholders, NewCounter} = lists:foldl(
        fun(_, {Acc, N}) ->
            {Acc ++ [[<<"$">>, integer_to_binary(N)]], N + 1}
        end,
        {[], Counter},
        Values
    ),
    {
        [
            quote_ident(atom_to_binary(Field, utf8)),
            <<" NOT IN (">>,
            join_comma(Placeholders),
            <<")">>
        ],
        Values,
        NewCounter
    };
compile_condition({Field, between, {Low, High}}, Counter) when is_atom(Field) ->
    {
        [
            quote_ident(atom_to_binary(Field, utf8)),
            <<" BETWEEN $">>,
            integer_to_binary(Counter),
            <<" AND $">>,
            integer_to_binary(Counter + 1)
        ],
        [Low, High],
        Counter + 2
    };
compile_condition({Field, Value}, Counter) when is_atom(Field) ->
    {
        [quote_ident(atom_to_binary(Field, utf8)), <<" = $">>, integer_to_binary(Counter)],
        [Value],
        Counter + 1
    }.

%%----------------------------------------------------------------------
%% Internal: JOIN clause
%%----------------------------------------------------------------------

compile_joins([], _Table, Counter) ->
    {<<>>, [], Counter};
compile_joins(Joins, _FromTable, Counter) ->
    {Parts, Params, NewCounter} = lists:foldl(
        fun({Type, JoinTable, {LeftCol, RightCol}, As}, {Acc, PAcc, C}) ->
            JoinTableBin = atom_to_binary(JoinTable, utf8),
            TableRef =
                case As of
                    undefined ->
                        quote_ident(JoinTableBin);
                    Alias ->
                        [
                            quote_ident(JoinTableBin),
                            <<" AS ">>,
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
                <<" ">>,
                TypeBin,
                <<" ">>,
                TableRef,
                <<" ON ">>,
                quote_ident(JoinRef),
                <<".">>,
                quote_ident(atom_to_binary(RightCol, utf8)),
                <<" = ">>,
                quote_ident(JoinTableBin),
                <<".">>,
                quote_ident(atom_to_binary(LeftCol, utf8))
            ],
            {Acc ++ [Part], PAcc, C}
        end,
        {[], [], Counter},
        Joins
    ),
    {iolist_to_binary(Parts), Params, NewCounter}.

join_type(inner) -> <<"INNER JOIN">>;
join_type(left) -> <<"LEFT JOIN">>;
join_type(right) -> <<"RIGHT JOIN">>;
join_type(full) -> <<"FULL JOIN">>.

%%----------------------------------------------------------------------
%% Internal: ORDER BY clause
%%----------------------------------------------------------------------

compile_order_by([], Counter) ->
    {<<>>, [], Counter};
compile_order_by(Orders, Counter) ->
    Parts = [
        [quote_ident(atom_to_binary(Field, utf8)), <<" ">>, dir_to_sql(Dir)]
     || {Field, Dir} <- Orders
    ],
    {iolist_to_binary([<<" ORDER BY ">>, join_comma(Parts)]), [], Counter}.

dir_to_sql(asc) -> <<"ASC">>;
dir_to_sql(desc) -> <<"DESC">>.

%%----------------------------------------------------------------------
%% Internal: GROUP BY clause
%%----------------------------------------------------------------------

compile_group_by([], Counter) ->
    {<<>>, [], Counter};
compile_group_by(Fields, Counter) ->
    Parts = [quote_ident(atom_to_binary(F, utf8)) || F <- Fields],
    {iolist_to_binary([<<" GROUP BY ">>, join_comma(Parts)]), [], Counter}.

%%----------------------------------------------------------------------
%% Internal: HAVING clause
%%----------------------------------------------------------------------

compile_havings([], Counter) ->
    {<<>>, [], Counter};
compile_havings(Conditions, Counter) ->
    {Parts, Params, NewCounter} = compile_conditions(Conditions, Counter),
    SQL = [<<" HAVING ">>, join_and(Parts)],
    {iolist_to_binary(SQL), Params, NewCounter}.

%%----------------------------------------------------------------------
%% Internal: LIMIT / OFFSET
%%----------------------------------------------------------------------

compile_limit(undefined, Counter) ->
    {<<>>, [], Counter};
compile_limit(N, Counter) ->
    {iolist_to_binary([<<" LIMIT $">>, integer_to_binary(Counter)]), [N], Counter + 1}.

compile_offset(undefined, Counter) ->
    {<<>>, [], Counter};
compile_offset(N, Counter) ->
    {iolist_to_binary([<<" OFFSET $">>, integer_to_binary(Counter)]), [N], Counter + 1}.

%%----------------------------------------------------------------------
%% Internal: LOCK / DISTINCT
%%----------------------------------------------------------------------

compile_lock(undefined) -> <<>>;
compile_lock(Lock) -> <<" ", Lock/binary>>.

compile_distinct(false) ->
    <<>>;
compile_distinct(true) ->
    <<"DISTINCT ">>;
compile_distinct(Fields) when is_list(Fields) ->
    Parts = [quote_ident(atom_to_binary(F, utf8)) || F <- Fields],
    iolist_to_binary([<<"DISTINCT ON (">>, join_comma(Parts), <<") ">>]).

%%----------------------------------------------------------------------
%% Internal: helpers
%%----------------------------------------------------------------------

compile_on_conflict({Field, nothing}, _Fields, _Data, _Counter) when is_atom(Field) ->
    {[<<" ON CONFLICT (">>, quote_ident(atom_to_binary(Field, utf8)), <<") DO NOTHING">>], []};
compile_on_conflict({{constraint, Name}, nothing}, _Fields, _Data, _Counter) ->
    {[<<" ON CONFLICT ON CONSTRAINT ">>, quote_ident(Name), <<" DO NOTHING">>], []};
compile_on_conflict({Field, replace_all}, Fields, Data, Counter) when is_atom(Field) ->
    UpdateFields = [F || F <- Fields, F =/= Field],
    compile_on_conflict({Field, {replace, UpdateFields}}, Fields, Data, Counter);
compile_on_conflict({{constraint, Name}, replace_all}, Fields, Data, Counter) ->
    compile_on_conflict_update(
        [<<" ON CONFLICT ON CONSTRAINT ">>, quote_ident(Name)], Fields, Fields, Data, Counter
    );
compile_on_conflict({Field, {replace, UpdateFields}}, _Fields, Data, Counter) when is_atom(Field) ->
    compile_on_conflict_update(
        [<<" ON CONFLICT (">>, quote_ident(atom_to_binary(Field, utf8)), <<")">>],
        UpdateFields,
        UpdateFields,
        Data,
        Counter
    );
compile_on_conflict({{constraint, Name}, {replace, UpdateFields}}, _Fields, Data, Counter) ->
    compile_on_conflict_update(
        [<<" ON CONFLICT ON CONSTRAINT ">>, quote_ident(Name)],
        UpdateFields,
        UpdateFields,
        Data,
        Counter
    ).

compile_on_conflict_update(ConflictTarget, UpdateFields, _AllFields, Data, Counter) ->
    {Sets, Params, _} = lists:foldl(
        fun(F, {SAcc, PAcc, N}) ->
            Value = maps:get(F, Data),
            Set = [quote_ident(atom_to_binary(F, utf8)), <<" = $">>, integer_to_binary(N)],
            {[Set | SAcc], [Value | PAcc], N + 1}
        end,
        {[], [], Counter},
        UpdateFields
    ),
    SQL = [ConflictTarget, <<" DO UPDATE SET ">>, join_comma(lists:reverse(Sets))],
    {SQL, lists:reverse(Params)}.

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
    lists:join(<<", ">>, Parts).

join_and(Parts) ->
    lists:join(<<" AND ">>, Parts).

join_or(Parts) ->
    lists:join(<<" OR ">>, Parts).

rewrite_fragment_placeholders(SQL, StartCounter) when is_binary(SQL) ->
    rewrite_fragment(SQL, StartCounter, <<>>).

rewrite_fragment(<<>>, Counter, Acc) ->
    {Acc, Counter};
rewrite_fragment(<<"?", Rest/binary>>, Counter, Acc) ->
    Placeholder = [<<"$">>, integer_to_binary(Counter)],
    rewrite_fragment(Rest, Counter + 1, <<Acc/binary, (iolist_to_binary(Placeholder))/binary>>);
rewrite_fragment(<<C, Rest/binary>>, Counter, Acc) ->
    rewrite_fragment(Rest, Counter, <<Acc/binary, C>>).
