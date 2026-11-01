-module(kura_query).
-moduledoc """
Composable, functional query builder.

Build queries by piping through `from/1`, `where/2`, `select/2`, `order_by/2`,
etc. Queries are compiled to parameterized SQL by `kura_query_compiler`.

```erlang
Q = kura_query:from(my_user),
Q1 = kura_query:where(Q, {active, true}),
Q2 = kura_query:order_by(Q1, [{name, asc}]),
Q3 = kura_query:limit(Q2, 10).
```
""".

-include("kura.hrl").

-export([
    from/1,
    select/2,
    select_expr/2,
    over/2,
    where/2,
    join/4, join/5,
    order_by/2,
    group_by/2,
    having/2,
    limit/2,
    offset/2,
    distinct/1, distinct/2,
    lock/2,
    prefix/2,
    preload/2,
    with_cte/3,
    union/2,
    union_all/2,
    intersect/2,
    except/2,
    scope/2,
    with_deleted/1,
    only_deleted/1,
    count/1, count/2,
    sum/2,
    avg/2,
    min/2,
    max/2,
    check_encrypted/1
]).

-export_type([window_fn/0, window_spec/0]).

%% Stands for every column of a CTE whose ciphertext columns cannot be named.
-define(ANY_COLUMN, '$kura_any_column').

-type window_fn() ::
    {count | sum | avg | min | max, atom()} | row_number | rank | dense_rank.
-type window_spec() ::
    #{partition_by => [atom()], order_by => [{atom(), asc | desc}]}.

-doc "Start a query from the given schema module or table atom.".
-spec from(atom() | module()) -> #kura_query{}.
from(Source) ->
    #kura_query{from = Source}.

-doc """
Set the SELECT fields. Pass atoms for columns or `{agg, field}` tuples for
aggregates.

`sum`, `avg`, `min` or `max` of an encrypted field raises
`{kura, {encrypted_field_in_aggregate, Field}}`: it would be computed over
ciphertexts. `count` is allowed.
""".
-spec select(#kura_query{}, [atom() | term()]) -> #kura_query{}.
select(Q, Fields) ->
    refuse_select(Fields, guard_scope(Q, [], [])),
    Q#kura_query{select = Fields}.

-doc """
Set SELECT expressions with aliases. Each expression is `{Alias, Expr}`
where `Expr` is a plain field atom, a `{fragment, SQL, Params}`, or a
window expression from `over/2`.

```erlang
Q = kura_query:select_expr(kura_query:from(sales), [
    {category, category},
    {rn, kura_query:over(row_number, #{partition_by => [category], order_by => [{amount, desc}]})},
    {running_total, kura_query:over({sum, amount}, #{partition_by => [category], order_by => [{day, asc}]})}
]).
```
""".
-spec select_expr(#kura_query{}, [term()]) -> #kura_query{}.
select_expr(Q, Exprs) ->
    Selected = Q#kura_query{select = {exprs, Exprs}},
    Scope = guard_scope(Selected, [], []),
    refuse_select({exprs, Exprs}, Scope),
    refuse_output(Selected, Scope),
    Selected.

-doc """
Build a window-function expression for use inside `select_expr/2`.

`WindowFn` is an aggregate (`{count, '*'}`, `{count, Field}`, `{sum, Field}`,
`{avg, Field}`, `{min, Field}`, `{max, Field}`) or a ranking function atom
(`row_number`, `rank`, `dense_rank`). `Spec` is a map with optional
`partition_by => [Field]` and `order_by => [{Field, asc | desc}]`.

Window functions require a backend declaring the `window_functions`
capability (PostgreSQL, or SQLite 3.25+).
""".
-spec over(window_fn(), window_spec()) -> {over, window_fn(), window_spec()}.
over(WindowFn, Spec) when is_map(Spec) ->
    {over, WindowFn, Spec}.

-doc """
Add a WHERE condition. Conditions: `{field, value}`, `{field, op, value}`,
`{'and', [...]}`, etc.

A condition that compares an encrypted field (see
`kura_types:encryption/1`) to a value raises
`{kura, {encrypted_field_in_where, Field}}`, wherever the field appears: under
`and`, `or` or `not`, in an `in` list, or as one column of a row-value `in`.
Encryption uses a random nonce, so a freshly encrypted value never equals the
stored ciphertext: the condition cannot match, and it would ship the plaintext
to the server as a bind parameter, into `pg_stat_statements`, the query log
and telemetry. Fields of the `from` schema and of every joined schema are
checked. So are the conditions of an `in` or `exists` subquery, against the
subquery's own schemas and against the outer query's encrypted fields that
its schemas do not declare: the database resolves such a name to the outer
column. `{Field, is_nil}` and `{Field, is_not_nil}` bind nothing and are
allowed. A `{fragment, SQL, Params}` is not inspected: what it binds is the
caller's responsibility. A query over a bare table name has no schema and is
not checked. A `{subquery, Q}` anywhere but `exists`, `not_exists` or a
single-column `in` raises `{kura, unrecognised_condition}`.
""".
-spec where(#kura_query{}, term()) -> #kura_query{}.
where(Q = #kura_query{wheres = W}, Condition) ->
    refuse_each([Condition], guard_scope(Q, [], []), encrypted_field_in_where),
    Q#kura_query{wheres = W ++ [Condition]}.

%% The encrypted field names a condition of Q can reach, and the CTEs it can
%% read from: its own, then those of the queries it is nested in.
guard_scope(#kura_query{ctes = Own} = Q, OuterEncrypted, OuterCtes) ->
    Ctes = Own ++ OuterCtes,
    {encrypted_in_scope(Q, Ctes) ++ unshadowed(OuterEncrypted, Q, Ctes), Ctes}.

encrypted_in_scope(Q, Ctes) ->
    lists:append([source_encrypted(S, Ctes) || S <- sources(Q)]).

source_encrypted(Source, Ctes) ->
    encrypted_fields(Source) ++ cte_encrypted(Source, Ctes).

encrypted_fields(Source) when is_atom(Source), Source =/= undefined ->
    kura_schema:encrypted_fields(Source);
encrypted_fields(_Source) ->
    [].

%% A source whose name, or whose schema's table name, is a CTE name reads
%% from that CTE: the database resolves a CTE before a table. Every CTE of
%% that name counts, and so does the schema's own encryption. A CTE is
%% dropped from the scope it is resolved in, so a self-reference ends.
cte_encrypted(_Source, []) ->
    [];
cte_encrypted(Source, Ctes) ->
    Names = source_names(Source),
    lists:append([
        cte_exports(Body, [C || {N, _} = C <- Ctes, N =/= Name])
     || {Name, Body} <- Ctes, lists:member(Name, Names)
    ]).

source_names(Source) when is_binary(Source) ->
    [Source];
source_names(Source) when is_atom(Source) ->
    case kura_schema:is_schema(Source) of
        true -> [atom_to_binary(Source), Source:table()];
        false -> [atom_to_binary(Source)]
    end;
source_names(_Source) ->
    [].

%% The column names of a CTE that may hold ciphertext: every encrypted field
%% of its sources, each alias it selects one under, and, for a union branch,
%% the name its first query gives the position where the branch selects
%% one. When positions cannot be matched (a SELECT * on either side), every
%% column of the CTE counts.
cte_exports(#kura_query{ctes = Own, select = Select, combinations = C} = Body, OuterCtes) ->
    Ctes = Own ++ OuterCtes,
    Encrypted = encrypted_in_scope(Body, Ctes),
    Main = Encrypted ++ ciphertext_aliases(Select, Encrypted),
    Main ++ lists:append([branch_exports(Body, Branch, Ctes) || {_, Branch} <- C]).

branch_exports(Main, Branch, Ctes) ->
    case cte_exports(Branch, Ctes) of
        [] -> [];
        Exports -> positional_exports(Main, Branch, Exports)
    end.

positional_exports(
    #kura_query{select = [], joins = J} = Main,
    #kura_query{select = [], joins = J} = Branch,
    Exports
) when
    Main#kura_query.from =:= Branch#kura_query.from
->
    Exports;
positional_exports(#kura_query{select = MainSelect}, #kura_query{select = BranchSelect}, Exports) ->
    case {column_names(MainSelect), column_names(BranchSelect)} of
        {{ok, Names}, {ok, BranchNames}} ->
            [
                N
             || {N, B} <- lists:zip(Names, BranchNames, trim),
                ciphertext_column(B, Exports)
            ];
        _ ->
            [?ANY_COLUMN]
    end.

column_names([]) -> unknown;
column_names({exprs, Exprs}) -> names([A || {A, _} <- Exprs], length(Exprs));
column_names(Fields) -> names([column_name(F) || F <- Fields], length(Fields)).

column_name(Field) when is_atom(Field) -> Field;
column_name({Agg, _}) when is_atom(Agg) -> Agg;
column_name(_) -> undefined.

names(Names, Count) ->
    case length(Names) =:= Count andalso not lists:member(undefined, Names) of
        true -> {ok, Names};
        false -> unknown
    end.

ciphertext_aliases({exprs, Exprs}, Encrypted) ->
    [Alias || {Alias, Field} <- Exprs, is_atom(Field), ciphertext_column(Field, Encrypted)];
ciphertext_aliases(_Fields, _Encrypted) ->
    [].

ciphertext_column(Field, Encrypted) ->
    lists:member(Field, Encrypted) orelse lists:member(?ANY_COLUMN, Encrypted).

%% Explicit recursion rather than lists:foreach/2, so an improper list raises
%% a reason of its own instead of a function_clause carrying the values.
refuse_each([], _Scope, _Reason) ->
    ok;
refuse_each([Cond | Rest], Scope, Reason) ->
    refuse_encrypted_cond(Cond, Scope, Reason),
    refuse_each(Rest, Scope, Reason);
refuse_each(_Improper, _Scope, _Reason) ->
    erlang:error({kura, unrecognised_condition}).

%% An allow-list over the condition grammar kura_dialect_pg compiles. Every
%% form that names a field is checked; a shape not listed here is refused
%% rather than passed, so a new form cannot slip a value past the check.
refuse_encrypted_cond({Op, Conds}, Scope, Reason) when Op =:= 'and'; Op =:= 'or' ->
    refuse_each(Conds, Scope, Reason);
refuse_encrypted_cond({'not', Cond}, Scope, Reason) ->
    refuse_encrypted_cond(Cond, Scope, Reason);
refuse_encrypted_cond({fragment, _SQL, _Params}, _Scope, _Reason) ->
    ok;
refuse_encrypted_cond({Exists, {subquery, #kura_query{} = Sub}}, Scope, _Reason) when
    Exists =:= exists; Exists =:= not_exists
->
    refuse_encrypted_subquery(Sub, Scope);
refuse_encrypted_cond({Field, in, {subquery, #kura_query{} = Sub}}, Scope, Reason) when
    is_atom(Field)
->
    refuse_field(Field, Scope, Reason),
    refuse_encrypted_subquery(Sub, Scope);
refuse_encrypted_cond({Field, Op}, _Scope, _Reason) when
    is_atom(Field), ?KURA_IS_UNARY_OP(Op)
->
    ok;
refuse_encrypted_cond({Field, Value}, Scope, Reason) when is_atom(Field) ->
    refuse_field(Field, Scope, Reason),
    refuse_subquery_value(Value);
refuse_encrypted_cond({Field, between, {Low, High}}, Scope, Reason) when is_atom(Field) ->
    refuse_field(Field, Scope, Reason),
    refuse_subquery_value(Low),
    refuse_subquery_value(High);
refuse_encrypted_cond({Field, _Op, Values}, Scope, Reason) when is_atom(Field), is_list(Values) ->
    refuse_field(Field, Scope, Reason),
    refuse_subquery_values(Values);
refuse_encrypted_cond({Field, _Op, Value}, Scope, Reason) when is_atom(Field) ->
    refuse_field(Field, Scope, Reason),
    refuse_subquery_value(Value);
refuse_encrypted_cond({Fields, _Op, Rows}, Scope, Reason) when is_list(Fields), is_list(Rows) ->
    refuse_fields(Fields, Scope, Reason),
    refuse_subquery_rows(Rows);
refuse_encrypted_cond({Fields, _Op, Value}, Scope, Reason) when is_list(Fields) ->
    refuse_fields(Fields, Scope, Reason),
    refuse_subquery_value(Value);
refuse_encrypted_cond(_Cond, {[], _Ctes}, _Reason) ->
    ok;
refuse_encrypted_cond(_Cond, _Scope, _Reason) ->
    erlang:error({kura, unrecognised_condition}).

%% Ordering, windowing and aggregating by an encrypted field bind nothing,
%% but would order or compute over ciphertexts, so they are refused too.
%% Malformed entries are left to the dialect: they carry no values.
refuse_orders([{Field, _Dir} | Rest], Scope) ->
    refuse_field(Field, Scope, encrypted_field_in_order_by),
    refuse_orders(Rest, Scope);
refuse_orders([_ | Rest], Scope) ->
    refuse_orders(Rest, Scope);
refuse_orders(_, _Scope) ->
    ok.

refuse_select({exprs, Exprs}, Scope) ->
    refuse_exprs(Exprs, Scope);
refuse_select(Fields, Scope) ->
    refuse_aggregates(Fields, Scope).

refuse_exprs([{_Alias, {over, Fn, Spec}} | Rest], Scope) ->
    refuse_aggregates([Fn], Scope),
    refuse_window(Spec, Scope),
    refuse_exprs(Rest, Scope);
refuse_exprs([_ | Rest], Scope) ->
    refuse_exprs(Rest, Scope);
refuse_exprs(_, _Scope) ->
    ok.

refuse_aggregates([{Agg, Field} | Rest], Scope) when
    Agg =:= sum; Agg =:= avg; Agg =:= min; Agg =:= max
->
    refuse_field(Field, Scope, encrypted_field_in_aggregate),
    refuse_aggregates(Rest, Scope);
refuse_aggregates([_ | Rest], Scope) ->
    refuse_aggregates(Rest, Scope);
refuse_aggregates(_, _Scope) ->
    ok.

refuse_window(#{} = Spec, Scope) ->
    refuse_columns(maps:get(partition_by, Spec, []), Scope, encrypted_field_in_window),
    refuse_columns(maps:get(order_by, Spec, []), Scope, encrypted_field_in_window);
refuse_window(_Spec, _Scope) ->
    ok.

refuse_columns([{Field, _Dir} | Rest], Scope, Reason) ->
    refuse_field(Field, Scope, Reason),
    refuse_columns(Rest, Scope, Reason);
refuse_columns([Field | Rest], Scope, Reason) ->
    refuse_field(Field, Scope, Reason),
    refuse_columns(Rest, Scope, Reason);
refuse_columns(_, _Scope, _Reason) ->
    ok.

%% ORDER BY, GROUP BY and DISTINCT ON also resolve the query's own output
%% aliases, so an alias of an encrypted field is ciphertext there too.
output_scope(#kura_query{select = Select}, {Encrypted, Ctes}) ->
    {Encrypted ++ ciphertext_aliases(Select, Encrypted), Ctes}.

refuse_output(Q, Scope) ->
    Output = output_scope(Q, Scope),
    refuse_orders(Q#kura_query.order_bys, Output),
    refuse_columns(Q#kura_query.group_bys, Output, encrypted_field_in_group_by),
    refuse_columns(Q#kura_query.distinct, Output, encrypted_field_in_distinct).

%% The dialect qualifies join columns: the left one with the source before
%% the join, the right one with the joined table.
refuse_joins([{_Type, Table, {Left, Right}, _As} | Rest], Previous, Ctes) ->
    refuse_field(Left, {source_encrypted(Previous, Ctes), Ctes}, encrypted_field_in_join),
    refuse_field(Right, {source_encrypted(Table, Ctes), Ctes}, encrypted_field_in_join),
    refuse_joins(Rest, Table, Ctes);
refuse_joins([_ | Rest], Previous, Ctes) ->
    refuse_joins(Rest, Previous, Ctes);
refuse_joins(_, _Previous, _Ctes) ->
    ok.

%% kura_dialect_pg compiles a subquery only in the clauses above; in any
%% other value position it would be bound as a parameter.
refuse_subquery_value({subquery, _}) ->
    erlang:error({kura, unrecognised_condition});
refuse_subquery_value(_Value) ->
    ok.

refuse_subquery_values([Value | Rest]) ->
    refuse_subquery_value(Value),
    refuse_subquery_values(Rest);
refuse_subquery_values(Tail) ->
    refuse_subquery_value(Tail).

refuse_subquery_rows([Row | Rest]) when is_tuple(Row) ->
    refuse_subquery_values(tuple_to_list(Row)),
    refuse_subquery_rows(Rest);
refuse_subquery_rows([Value | Rest]) ->
    refuse_subquery_value(Value),
    refuse_subquery_rows(Rest);
refuse_subquery_rows(Tail) ->
    refuse_subquery_value(Tail).

refuse_encrypted_subquery(Sub, {Encrypted, Ctes}) ->
    refuse_encrypted_query(Sub, Encrypted, Ctes).

%% Walk every condition of Q: its wheres and havings, the bodies of its CTEs
%% and its union branches, each with the outer scope it can see. A subquery's
%% conditions see the outer query's columns too: the dialect emits
%% unqualified names, so the database resolves one the subquery's own sources
%% lack to the outer column.
refuse_encrypted_query(
    #kura_query{wheres = W, havings = H, ctes = Own, combinations = C} = Q,
    OuterEncrypted,
    OuterCtes
) ->
    {_, Ctes} = Scope = guard_scope(Q, OuterEncrypted, OuterCtes),
    refuse_each(W, Scope, encrypted_field_in_where),
    refuse_each(H, Scope, encrypted_field_in_having),
    refuse_output(Q, Scope),
    refuse_select(Q#kura_query.select, Scope),
    refuse_joins(Q#kura_query.joins, Q#kura_query.from, Ctes),
    refuse_bodies(Own ++ C, OuterEncrypted, Ctes).

refuse_bodies([], _OuterEncrypted, _Ctes) ->
    ok;
refuse_bodies([{_, #kura_query{} = Body} | Rest], OuterEncrypted, Ctes) ->
    refuse_encrypted_query(Body, OuterEncrypted, Ctes),
    refuse_bodies(Rest, OuterEncrypted, Ctes);
refuse_bodies(_Malformed, _OuterEncrypted, _Ctes) ->
    erlang:error({kura, unrecognised_condition}).

%% Only a column the subquery's schemas declare shadows an outer one. A bare
%% table's or a CTE's columns are not known here, so it shadows nothing.
unshadowed([], _Q, _Ctes) ->
    [];
unshadowed(OuterEncrypted, Q, Ctes) ->
    Defined = lists:append([columns(S, Ctes) || S <- sources(Q)]),
    [F || F <- OuterEncrypted, not lists:member(F, Defined)].

sources(#kura_query{from = From, joins = Joins}) ->
    [From | [T || {_, T, _, _} <- Joins]].

columns(Source, Ctes) when is_atom(Source) ->
    Names = source_names(Source),
    case
        kura_schema:is_schema(Source) andalso
            [N || {N, _} <- Ctes, lists:member(N, Names)] =:= []
    of
        true -> kura_schema:non_virtual_fields(Source);
        false -> []
    end;
columns(_Source, _Ctes) ->
    [].

refuse_fields([], _Scope, _Reason) ->
    ok;
refuse_fields([Field | Rest], Scope, Reason) ->
    refuse_field(Field, Scope, Reason),
    refuse_fields(Rest, Scope, Reason);
refuse_fields(_Improper, _Scope, _Reason) ->
    erlang:error({kura, unrecognised_condition}).

refuse_field(Field, {Encrypted, _Ctes}, Reason) ->
    case ciphertext_column(Field, Encrypted) of
        true -> erlang:error({kura, {Reason, Field}});
        false -> ok
    end.

-doc """
Add a JOIN clause. `Table` can be a schema module or raw table atom.
Conditions already added with `where/2` or `having/2` are checked against the
joined schema's encrypted fields, as if added after the join.
`On` is `{LeftCol, RightCol}` where LeftCol is on the previous table
(FROM for the first join, or the last joined table) and RightCol is on
the joined table. For chained joins, the left side automatically advances.
A column of `On` that is encrypted on its side raises
`{kura, {encrypted_field_in_join, Field}}`.

```erlang
Q = kura_query:from(user_schema),
Q1 = kura_query:join(Q, inner, post_schema, {id, user_id}),
%% => users.id = posts.user_id
Q2 = kura_query:join(Q1, inner, comment_schema, {id, post_id}).
%% => posts.id = comments.post_id
```
""".
-spec join(#kura_query{}, inner | left | right | full, atom(), {atom(), atom()}) -> #kura_query{}.
join(Q, Type, Table, On) ->
    join(Q, Type, Table, On, undefined).

-spec join(
    #kura_query{}, inner | left | right | full, atom(), {atom(), atom()}, atom() | undefined
) -> #kura_query{}.
join(Q = #kura_query{joins = J}, Type, Table, On, As) ->
    recheck_encrypted(Q#kura_query{joins = J ++ [{Type, Table, On, As}]}).

-doc """
Set ORDER BY clauses as `[{field, asc | desc}]`.

Ordering by an encrypted field raises
`{kura, {encrypted_field_in_order_by, Field}}`: the order of ciphertexts
says nothing about the order of the values.
""".
-spec order_by(#kura_query{}, [{atom(), asc | desc}]) -> #kura_query{}.
order_by(Q, Orders) ->
    refuse_orders(Orders, output_scope(Q, guard_scope(Q, [], []))),
    Q#kura_query{order_bys = Orders}.

-doc """
Set GROUP BY fields. Grouping by an encrypted field raises
`{kura, {encrypted_field_in_group_by, Field}}`: equal values encrypt to
different ciphertexts, so each row would be its own group.
""".
-spec group_by(#kura_query{}, [atom()]) -> #kura_query{}.
group_by(Q, Fields) ->
    refuse_columns(Fields, output_scope(Q, guard_scope(Q, [], [])), encrypted_field_in_group_by),
    Q#kura_query{group_bys = Fields}.

-doc """
Add a HAVING condition, in the same forms as `where/2`. A condition that
compares an encrypted field to a value raises
`{kura, {encrypted_field_in_having, Field}}`, as `where/2` does.
""".
-spec having(#kura_query{}, term()) -> #kura_query{}.
having(Q = #kura_query{havings = H}, Condition) ->
    refuse_each([Condition], guard_scope(Q, [], []), encrypted_field_in_having),
    Q#kura_query{havings = H ++ [Condition]}.

-spec limit(#kura_query{}, non_neg_integer()) -> #kura_query{}.
limit(Q, N) ->
    Q#kura_query{limit = N}.

-spec offset(#kura_query{}, non_neg_integer()) -> #kura_query{}.
offset(Q, N) ->
    Q#kura_query{offset = N}.

-spec distinct(#kura_query{}) -> #kura_query{}.
distinct(Q) ->
    Q#kura_query{distinct = true}.

-doc """
Set DISTINCT ON fields. An encrypted field raises
`{kura, {encrypted_field_in_distinct, Field}}`, as `group_by/2` does.
""".
-spec distinct(#kura_query{}, [atom()]) -> #kura_query{}.
distinct(Q, Fields) ->
    refuse_columns(Fields, output_scope(Q, guard_scope(Q, [], [])), encrypted_field_in_distinct),
    Q#kura_query{distinct = Fields}.

-spec lock(#kura_query{}, binary()) -> #kura_query{}.
lock(Q, LockExpr) ->
    Q#kura_query{lock = LockExpr}.

-spec prefix(#kura_query{}, binary()) -> #kura_query{}.
prefix(Q, Schema) ->
    Q#kura_query{prefix = Schema}.

-doc "Add associations to preload after query execution.".
-spec preload(#kura_query{}, [atom() | {atom(), list()}]) -> #kura_query{}.
preload(Q = #kura_query{preloads = P}, Assocs) ->
    Q#kura_query{preloads = P ++ Assocs}.

-doc """
Add a Common Table Expression (WITH clause).

A query, subquery or CTE that reads `Name` (by its name, or by a schema whose
table is named `Name`) gets the CTE's encrypted fields in its filter guard
scope: every encrypted field of `CteQuery`'s sources, and every alias it
selects one under. The conditions already in the query, and the bodies of
its CTEs, are checked again, so the order of `where/2` and `with_cte/3` does
not matter. A name used by more than one CTE, or by a CTE and a schema, gets
the encrypted fields of all of them.
""".
-spec with_cte(#kura_query{}, binary(), #kura_query{}) -> #kura_query{}.
with_cte(Q = #kura_query{ctes = CTEs}, Name, CteQuery) ->
    recheck_encrypted(Q#kura_query{ctes = CTEs ++ [{Name, CteQuery}]}).

%% A join or a CTE can bring encrypted fields into the scope of conditions
%% already added, so the whole query is walked again.
recheck_encrypted(Q) ->
    check_encrypted(Q),
    Q.

-doc """
Raise if any condition of `Query` would bind an encrypted field's plaintext,
with the reasons `where/2` and `having/2` raise, wherever the condition sits:
in the query's wheres or havings, a subquery, a CTE body or a union branch.
It also raises where `order_by/2`, `group_by/2`, `distinct/2`, `join/5`,
`select/2` and `select_expr/2` would: on ordering, grouping, DISTINCT ON,
joining, windowing or aggregating (`sum`, `avg`, `min`, `max`) by an
encrypted field or an alias of one.

The verdict depends only on the query's structure (sources, selects, CTEs and
the shape of every condition), never on a bound value. `kura_query_compiler`
relies on that: it calls this on every query it compiles, except a cache hit
in `to_sql_cached/2`, whose shape already passed. So a `#kura_query{}` built
or updated without `where/2` is checked too.
""".
-spec check_encrypted(#kura_query{}) -> ok.
check_encrypted(Q) ->
    refuse_encrypted_query(Q, [], []).

-doc "Combine queries with UNION (removes duplicates).".
-spec union(#kura_query{}, #kura_query{}) -> #kura_query{}.
union(Q = #kura_query{combinations = C}, Q2) ->
    Q#kura_query{combinations = C ++ [{union, Q2}]}.

-doc "Combine queries with UNION ALL (keeps duplicates).".
-spec union_all(#kura_query{}, #kura_query{}) -> #kura_query{}.
union_all(Q = #kura_query{combinations = C}, Q2) ->
    Q#kura_query{combinations = C ++ [{union_all, Q2}]}.

-doc "Combine queries with INTERSECT.".
-spec intersect(#kura_query{}, #kura_query{}) -> #kura_query{}.
intersect(Q = #kura_query{combinations = C}, Q2) ->
    Q#kura_query{combinations = C ++ [{intersect, Q2}]}.

-doc "Combine queries with EXCEPT.".
-spec except(#kura_query{}, #kura_query{}) -> #kura_query{}.
except(Q = #kura_query{combinations = C}, Q2) ->
    Q#kura_query{combinations = C ++ [{except, Q2}]}.

-doc "Apply a composable query transform function.".
-spec scope(#kura_query{}, fun((#kura_query{}) -> #kura_query{})) -> #kura_query{}.
scope(Query, Fun) when is_function(Fun, 1) ->
    Fun(Query).

-doc "Include soft-deleted records in query results.".
-spec with_deleted(#kura_query{}) -> #kura_query{}.
with_deleted(Q) ->
    Q#kura_query{include_deleted = true}.

-doc "Return only soft-deleted records.".
-spec only_deleted(#kura_query{}) -> #kura_query{}.
only_deleted(Q) ->
    Q1 = Q#kura_query{include_deleted = true},
    where(Q1, {deleted_at, is_not_nil}).

-doc "Set query to SELECT COUNT(*).".
-spec count(#kura_query{}) -> #kura_query{}.
count(Q) ->
    Q#kura_query{select = [{count, '*'}]}.

-spec count(#kura_query{}, atom()) -> #kura_query{}.
count(Q, Field) ->
    Q#kura_query{select = [{count, Field}]}.

-spec sum(#kura_query{}, atom()) -> #kura_query{}.
sum(Q, Field) ->
    select(Q, [{sum, Field}]).

-spec avg(#kura_query{}, atom()) -> #kura_query{}.
avg(Q, Field) ->
    select(Q, [{avg, Field}]).

-spec min(#kura_query{}, atom()) -> #kura_query{}.
min(Q, Field) ->
    select(Q, [{min, Field}]).

-spec max(#kura_query{}, atom()) -> #kura_query{}.
max(Q, Field) ->
    select(Q, [{max, Field}]).
