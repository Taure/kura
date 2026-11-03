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
    distance/3,
    check_distances/1,
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

-export_type([window_fn/0, window_spec/0, distance/0, distance_metric/0, order/0]).

%% Stands for every column of a CTE whose ciphertext columns cannot be named.
-define(ANY_COLUMN, '$kura_any_column').

-type window_fn() ::
    {count | sum | avg | min | max, atom()} | row_number | rank | dense_rank.
-type window_spec() ::
    #{partition_by => [atom()], order_by => [{atom(), asc | desc}]}.
-type distance_metric() :: l2 | cosine | inner_product.
-type distance() :: {distance, atom(), distance_metric(), binary()}.
-type order() :: {atom() | distance(), asc | desc}.

-define(IS_METRIC(M), (M =:= l2 orelse M =:= cosine orelse M =:= inner_product)).
%% pgvector's VECTOR_MAX_DIM.
-define(MAX_DIMENSIONS, 16000).

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
    reject_selected_distances(Fields),
    refuse_select(Fields, guard_scope(Q, [], [])),
    Q#kura_query{select = Fields}.

-doc """
Set SELECT expressions with aliases. Each expression is `{Alias, Expr}`
where `Expr` is a plain field atom, a `{fragment, SQL, Params}`, a
window expression from `over/2`, or a vector distance from `distance/3`.

A distance over a field that may hold ciphertext, such as any column of a
CTE whose encrypted columns cannot be named, raises
`{kura, {encrypted_field_in_select, Field}}`: the distance would be
computed over ciphertexts.

```erlang
Q = kura_query:select_expr(kura_query:from(sales), [
    {category, category},
    {rn, kura_query:over(row_number, #{partition_by => [category], order_by => [{amount, desc}]})},
    {running_total, kura_query:over({sum, amount}, #{partition_by => [category], order_by => [{day, asc}]})}
]).
```
""".
-spec select_expr(#kura_query{}, [term()]) -> #kura_query{}.
select_expr(Q = #kura_query{from = From}, Exprs) ->
    check_distances(From, expr_distances(Exprs)),
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
    reject_window_distances(WindowFn, Spec),
    {over, WindowFn, Spec}.

reject_window_distances(WindowFn, Spec) ->
    reject_window_fn_distance(WindowFn),
    reject_window_spec_distances(Spec).

reject_window_fn_distance({_Agg, Field}) -> reject_distance(over, Field);
reject_window_fn_distance(_Fn) -> ok.

reject_window_spec_distances(Spec) when is_map(Spec) ->
    reject_distances(over, maps:get(partition_by, Spec, [])),
    reject_order_distances(over, maps:get(order_by, Spec, []));
reject_window_spec_distances(_Spec) ->
    ok.

-doc """
Build a pgvector distance between the vector field `Field` and `Vector`,
for use in `order_by/2`, `select_expr/2`, and as a threshold
`{Distance, Op, Number}` in `where/2` and `having/2`, where `Op` is
`'<'`, `'<='`, `'>'` or `'>='`.

`Metric` is `l2`, `cosine` or `inner_product`. For every metric a smaller
value is nearer: `inner_product` is the negative inner product, as
pgvector's `<#>` returns it. Order `asc` to get the nearest rows first;
`desc` cannot use an approximate-nearest-neighbour index.

`Vector` is a list of numbers. It is cast and dumped to pgvector's text
form here, so only a binary enters the query. A list that is empty or
holds anything but numbers raises.

The query must be `from/1` a schema in which `Field` is a `vector` or
`{vector, N}` field, and for `{vector, N}` the vector must have `N`
elements. `order_by/2`, `select_expr/2`, `where/2` and `having/2` raise
otherwise. A threshold that is not a number raises `{kura,
{invalid_distance_threshold, Field}}`.

A distance anywhere else, in any other condition of `where/2` or
`having/2`, `group_by/2`, `distinct/2`, `select/2` (and the aggregate
helpers), `count/2`, a `join` column or a window from `over/2`, raises
`{kura, {distance_not_supported_here, Clause}}`.

```erlang
D = kura_query:distance(embedding, cosine, QueryVector),
Q = kura_query:select_expr(kura_query:from(memory), [{id, id}, {dist, D}]),
Q1 = kura_query:where(Q, {D, '<', 0.35}),
Q2 = kura_query:limit(kura_query:order_by(Q1, [{dist, asc}]), 10).
```

Ordering by the alias of a selected distance sends the vector once and
still uses an index. Order by `D` itself when the distance is not
selected.
""".
-spec distance(atom(), distance_metric(), [number()]) -> distance().
distance(Field, Metric, Vector) when is_atom(Field), ?IS_METRIC(Metric) ->
    case length_upto(Vector, ?MAX_DIMENSIONS) of
        {ok, 0} -> invalid_vector(Field, ~"is empty");
        {ok, _} -> {distance, Field, Metric, cast_vector(Field, Vector)};
        too_long -> invalid_vector(Field, too_many_dimensions());
        improper -> invalid_vector(Field, ~"is not a list of numbers")
    end;
distance(Field, Metric, _Vector) when is_atom(Field) ->
    erlang:error({kura, {invalid_distance_metric, Metric}}).

too_many_dimensions() ->
    iolist_to_binary([~"has more than ", integer_to_binary(?MAX_DIMENSIONS), ~" dimensions"]).

%% Stops at Max, so an oversized list costs Max steps, not its length.
length_upto(List, Max) ->
    length_upto(List, Max, 0).

length_upto([], _Max, N) -> {ok, N};
length_upto([_ | _], Max, Max) -> too_long;
length_upto([_ | Rest], Max, N) -> length_upto(Rest, Max, N + 1);
length_upto(_NotAList, _Max, _N) -> improper.

%% float/1 raises badarg on an integer beyond the double range.
cast_vector(Field, Vector) ->
    try kura_types:cast(vector, Vector) of
        {ok, Floats} when is_list(Floats) -> vector_text(Floats);
        {error, Reason} -> invalid_vector(Field, Reason)
    catch
        error:badarg -> invalid_vector(Field, ~"has an element outside the float range")
    end.

-spec invalid_vector(atom(), binary()) -> no_return().
invalid_vector(Field, Reason) ->
    erlang:error({kura, {invalid_vector, Field, Reason}}).

vector_text(Floats) ->
    case kura_types:dump(vector, Floats) of
        {ok, Text} when is_binary(Text) -> Text
    end.

%% Called by kura_query_compiler before it compiles. Checks the distance
%% nodes of select expressions (windows included), order_bys and the
%% thresholds of wheres and havings, in the query and in its CTEs,
%% combinations and condition subqueries, as the builders do. It walks
%% structure only, never bound values, so a distance in a value position
%% or in a hand-built group_by, distinct, plain select or join is not
%% inspected: where/2 and having/2 refuse those.
-doc false.
-spec check_distances(#kura_query{}) -> ok.
check_distances(#kura_query{
    from = From,
    select = Select,
    wheres = Wheres,
    havings = Havings,
    order_bys = [],
    ctes = [],
    combinations = []
}) when
    is_list(Select)
->
    case query_thresholds(Wheres) ++ query_thresholds(Havings) of
        [] -> ok;
        Thresholds -> check_thresholds(From, Thresholds)
    end;
check_distances(#kura_query{
    from = From,
    select = Select,
    wheres = Wheres,
    havings = Havings,
    order_bys = Orders,
    ctes = Ctes,
    combinations = Combinations
}) ->
    Thresholds = query_thresholds(Wheres) ++ query_thresholds(Havings),
    check_distances(
        From,
        select_distances(Select) ++ [D || {D, _} <- Thresholds] ++ order_distances(Orders)
    ),
    check_threshold_values(Thresholds),
    check_bodies(Ctes),
    check_bodies(Combinations).

select_distances({exprs, Exprs}) -> expr_distances(Exprs);
select_distances(_Select) -> [].

%% Explicit recursion rather than list comprehensions, so a malformed list
%% raises a reason of its own instead of a bad_generator carrying the
%% distance and its vector.
expr_distances([]) ->
    [];
expr_distances([{_Alias, {over, Fn, Spec}} | Rest]) ->
    reject_window_distances(Fn, Spec),
    expr_distances(Rest);
expr_distances([{_Alias, {distance, _, _, _} = D} | Rest]) ->
    [D | expr_distances(Rest)];
expr_distances([_ | Rest]) ->
    expr_distances(Rest);
expr_distances(_Improper) ->
    erlang:error({kura, malformed_select}).

order_distances([]) ->
    [];
order_distances([{{distance, _, _, _} = D, _Dir} | Rest]) ->
    [D | order_distances(Rest)];
order_distances([_ | Rest]) ->
    order_distances(Rest);
order_distances(_Improper) ->
    erlang:error({kura, malformed_order_by}).

%% A malformed CTE or combination is left to check_encrypted/1, which
%% refuses it.
check_bodies([{_Name, #kura_query{} = Q} | Rest]) ->
    check_distances(Q),
    check_bodies(Rest);
check_bodies([_ | Rest]) ->
    check_bodies(Rest);
check_bodies(_) ->
    ok.

check_distances(_From, []) ->
    ok;
check_distances(From, Distances) ->
    case is_atom(From) andalso From =/= undefined andalso kura_schema:is_schema(From) of
        true -> check_each_distance(kura_schema:field_types(From), Distances);
        false -> erlang:error({kura, {distance_requires_schema, From}})
    end.

check_each_distance(_Types, []) ->
    ok;
check_each_distance(Types, [D | Rest]) ->
    check_distance(Types, D),
    check_each_distance(Types, Rest).

check_distance(Types, {distance, Field, Metric, Text}) when
    is_atom(Field), ?IS_METRIC(Metric), is_binary(Text)
->
    case maps:get(Field, Types, undefined) of
        vector ->
            ok;
        {vector, Dim} ->
            case dimension(Text) of
                Dim -> ok;
                Got -> erlang:error({kura, {vector_dimension_mismatch, Field, Dim, Got}})
            end;
        _ ->
            erlang:error({kura, {not_a_vector_field, Field}})
    end;
check_distance(_Types, {distance, Field, _Metric, _Vector}) when is_atom(Field) ->
    erlang:error({kura, {invalid_distance, Field}});
check_distance(_Types, _Distance) ->
    erlang:error({kura, invalid_distance}).

dimension(Text) ->
    count_commas(Text, 1).

count_commas(<<$,, Rest/binary>>, N) -> count_commas(Rest, N + 1);
count_commas(<<_, Rest/binary>>, N) -> count_commas(Rest, N);
count_commas(<<>>, N) -> N.

%% An improper tail is checked as one more term, so a distance there is
%% refused too and a malformed list is left to the guards that follow.
reject_distances(Clause, [Term | Rest]) ->
    reject_distance(Clause, Term),
    reject_distances(Clause, Rest);
reject_distances(Clause, Tail) ->
    reject_distance(Clause, Tail).

reject_distance(Clause, {distance, _Field, _Metric, _Vector}) ->
    erlang:error({kura, {distance_not_supported_here, Clause}});
reject_distance(_Clause, _Term) ->
    ok.

reject_order_distances(Clause, [{Expr, _Dir} | Rest]) ->
    reject_distance(Clause, Expr),
    reject_order_distances(Clause, Rest);
reject_order_distances(Clause, [Term | Rest]) ->
    reject_distance(Clause, Term),
    reject_order_distances(Clause, Rest);
reject_order_distances(Clause, Tail) ->
    reject_distance(Clause, Tail).

reject_selected_distances([{Agg, Field} | Rest]) when is_atom(Agg) ->
    reject_distance(select, Field),
    reject_selected_distances(Rest);
reject_selected_distances([Field | Rest]) ->
    reject_distance(select, Field),
    reject_selected_distances(Rest);
reject_selected_distances(Tail) ->
    reject_distance(select, Tail).

reject_join_distances({Left, Right}) ->
    reject_distance(join, Left),
    reject_distance(join, Right);
reject_join_distances(On) ->
    reject_distance(join, On).

%% A distance anywhere but a threshold would be bound as a parameter
%% wherever it sits, so where/2 and having/2 refuse it.
cond_thresholds(Clause, [Cond | Rest]) ->
    cond_thresholds(Clause, Cond) ++ cond_thresholds(Clause, Rest);
cond_thresholds(_Clause, []) ->
    [];
cond_thresholds(Clause, {Op, Conds}) when (Op =:= 'and' orelse Op =:= 'or'), is_list(Conds) ->
    cond_thresholds(Clause, Conds);
cond_thresholds(Clause, {'not', Cond}) ->
    cond_thresholds(Clause, Cond);
cond_thresholds(_Clause, {fragment, _SQL, _Params}) ->
    [];
cond_thresholds(_Clause, {{distance, _, _, _} = D, Op, Threshold}) when
    ?KURA_IS_THRESHOLD_OP(Op)
->
    [{D, Threshold}];
cond_thresholds(_Clause, {Exists, {subquery, #kura_query{} = Sub}}) when
    Exists =:= exists; Exists =:= not_exists
->
    check_distances(Sub),
    [];
cond_thresholds(Clause, {Field, in, {subquery, #kura_query{} = Sub}}) ->
    reject_distance_field(Clause, Field),
    check_distances(Sub),
    [];
cond_thresholds(Clause, {Field, Value}) ->
    reject_distance_field(Clause, Field),
    reject_distance_value(Clause, '=', Value),
    [];
cond_thresholds(Clause, {Field, Op, Value}) ->
    reject_distance_field(Clause, Field),
    reject_distance(Clause, Op),
    reject_distance_value(Clause, Op, Value),
    [];
cond_thresholds(Clause, Cond) ->
    reject_distance(Clause, Cond),
    [].

%% The compiler's walk: thresholds and subqueries, and nothing in a bound
%% position, so its cost does not grow with the values.
query_thresholds([Cond | Rest]) ->
    query_thresholds(Cond) ++ query_thresholds(Rest);
query_thresholds({Op, Conds}) when (Op =:= 'and' orelse Op =:= 'or'), is_list(Conds) ->
    query_thresholds(Conds);
query_thresholds({'not', Cond}) ->
    query_thresholds(Cond);
query_thresholds({{distance, _, _, _} = D, Op, Threshold}) when ?KURA_IS_THRESHOLD_OP(Op) ->
    [{D, Threshold}];
query_thresholds({Exists, {subquery, #kura_query{} = Sub}}) when
    Exists =:= exists; Exists =:= not_exists
->
    check_distances(Sub),
    [];
query_thresholds({_Field, in, {subquery, #kura_query{} = Sub}}) ->
    check_distances(Sub),
    [];
query_thresholds(_Cond) ->
    [].

%% The distances first, so a malformed node raises the distance's reason
%% and a threshold reason always names an atom field.
check_thresholds(From, Thresholds) ->
    check_distances(From, [D || {D, _} <- Thresholds]),
    check_threshold_values(Thresholds).

check_threshold_values([]) ->
    ok;
check_threshold_values([{_D, Threshold} | Rest]) when is_number(Threshold) ->
    check_threshold_values(Rest);
check_threshold_values([{{distance, Field, _Metric, _Vector}, _Threshold} | _]) ->
    erlang:error({kura, {invalid_distance_threshold, Field}}).

reject_distance_field(Clause, Fields) when is_list(Fields) ->
    reject_distances(Clause, Fields);
reject_distance_field(Clause, Field) ->
    reject_distance(Clause, Field).

reject_distance_value(Clause, between, {Low, High}) ->
    reject_distance(Clause, Low),
    reject_distance(Clause, High);
reject_distance_value(Clause, _Op, Values) when is_list(Values) ->
    reject_distance_values(Clause, Values);
reject_distance_value(Clause, _Op, Value) ->
    reject_distance(Clause, Value).

%% An element may be a row of a row-value `in`.
reject_distance_values(_Clause, []) ->
    ok;
reject_distance_values(Clause, [{distance, _, _, _} = D | Rest]) ->
    reject_distance(Clause, D),
    reject_distance_values(Clause, Rest);
reject_distance_values(Clause, [Row | Rest]) when is_tuple(Row) ->
    reject_distances(Clause, tuple_to_list(Row)),
    reject_distance_values(Clause, Rest);
reject_distance_values(Clause, [Value | Rest]) ->
    reject_distance(Clause, Value),
    reject_distance_values(Clause, Rest);
reject_distance_values(_Clause, _Improper) ->
    erlang:error({kura, unrecognised_condition}).

-doc """
Add a WHERE condition. Conditions: `{field, value}`, `{field, op, value}`,
`{'and', [...]}`, etc.

`{Distance, Op, Threshold}` keeps the rows whose vector distance from
`distance/3` compares to the number `Threshold` by `'<'`, `'<='`, `'>'`
or `'>='`. The distance is checked against the schema as in
`order_by/2`, and its column by the encrypted-field guard below.

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
where(Q = #kura_query{from = From, wheres = W}, Condition) ->
    check_thresholds(From, cond_thresholds(where, Condition)),
    refuse_each([Condition], guard_scope(Q, [], []), encrypted_field_in_where),
    Q#kura_query{wheres = W ++ [Condition]}.

%% A nested query reads its own CTEs before those of the queries around it.
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

%% A union exposes its first query's column names, so a branch's ciphertext
%% column is exported under the name at the same position. When positions
%% cannot be matched (a SELECT * on either side), fail closed: every column
%% of the CTE counts.
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
refuse_encrypted_cond({{distance, Field, _Metric, _Vector}, Op, _Threshold}, Scope, Reason) when
    is_atom(Field), ?KURA_IS_THRESHOLD_OP(Op)
->
    refuse_field(Field, Scope, Reason);
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
%% Malformed entries are left to the dialect: they carry no values. A
%% distance is checked by its column, so the query vector stays out of the
%% error.
refuse_orders([{{distance, Field, _Metric, _Vector}, _Dir} | Rest], Scope) ->
    refuse_field(Field, Scope, encrypted_field_in_order_by),
    refuse_orders(Rest, Scope);
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

refuse_exprs([{_Alias, {distance, Field, _Metric, _Vector}} | Rest], Scope) ->
    refuse_field(Field, Scope, encrypted_field_in_select),
    refuse_exprs(Rest, Scope);
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

%% A term that is not a column, such as a distance, is named by what it is
%% not, so its values stay out of the reason.
refuse_field(Field, {Encrypted, _Ctes}, Reason) ->
    case ciphertext_column(Field, Encrypted) of
        true when is_atom(Field) -> erlang:error({kura, {Reason, Field}});
        true -> erlang:error({kura, {Reason, not_a_column}});
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
    reject_join_distances(On),
    recheck_encrypted(Q#kura_query{joins = J ++ [{Type, Table, On, As}]}).

-doc """
Set ORDER BY clauses as `[{Expr, asc | desc}]`, where `Expr` is a field
atom or a vector distance from `distance/3`.

Ordering by an encrypted field raises
`{kura, {encrypted_field_in_order_by, Field}}`: the order of ciphertexts
says nothing about the order of the values. A distance is checked by its
column, so the reason names the field and never carries the vector.
""".
-spec order_by(#kura_query{}, [order()]) -> #kura_query{}.
order_by(Q = #kura_query{from = From}, Orders) ->
    check_distances(From, order_distances(Orders)),
    refuse_orders(Orders, output_scope(Q, guard_scope(Q, [], []))),
    Q#kura_query{order_bys = Orders}.

-doc """
Set GROUP BY fields. Grouping by an encrypted field raises
`{kura, {encrypted_field_in_group_by, Field}}`: equal values encrypt to
different ciphertexts, so each row would be its own group.
""".
-spec group_by(#kura_query{}, [atom()]) -> #kura_query{}.
group_by(Q, Fields) ->
    reject_distances(group_by, Fields),
    refuse_columns(Fields, output_scope(Q, guard_scope(Q, [], [])), encrypted_field_in_group_by),
    Q#kura_query{group_bys = Fields}.

-doc """
Add a HAVING condition, in the same forms as `where/2`. A condition that
compares an encrypted field to a value raises
`{kura, {encrypted_field_in_having, Field}}`, as `where/2` does.
""".
-spec having(#kura_query{}, term()) -> #kura_query{}.
having(Q = #kura_query{from = From, havings = H}, Condition) ->
    check_thresholds(From, cond_thresholds(having, Condition)),
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
    reject_distances(distinct, Fields),
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
encrypted field or an alias of one, and on selecting a distance of one
(`{kura, {encrypted_field_in_select, Field}}`). A term in a column
position that is not a column, such as a distance, is reported as
`not_a_column` in place of the field.

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
    reject_distance(count, Field),
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
