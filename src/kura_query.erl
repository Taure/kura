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
    max/2
]).

-export_type([window_fn/0, window_spec/0]).

-type window_fn() ::
    {count | sum | avg | min | max, atom()} | row_number | rank | dense_rank.
-type window_spec() ::
    #{partition_by => [atom()], order_by => [{atom(), asc | desc}]}.

-doc "Start a query from the given schema module or table atom.".
-spec from(atom() | module()) -> #kura_query{}.
from(Source) ->
    #kura_query{from = Source}.

-doc "Set the SELECT fields. Pass atoms for columns or `{agg, field}` tuples for aggregates.".
-spec select(#kura_query{}, [atom() | term()]) -> #kura_query{}.
select(Q, Fields) ->
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
    Q#kura_query{select = {exprs, Exprs}}.

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
not checked.
""".
-spec where(#kura_query{}, term()) -> #kura_query{}.
where(Q = #kura_query{wheres = W}, Condition) ->
    refuse_encrypted(guard_scope(Q, [], []), [Condition], encrypted_field_in_where),
    Q#kura_query{wheres = W ++ [Condition]}.

%% The encrypted field names a condition of Q can reach, and the CTEs it can
%% read from: its own, then those of the queries it is nested in.
guard_scope(#kura_query{ctes = Own} = Q, OuterEncrypted, OuterCtes) ->
    Ctes = Own ++ OuterCtes,
    {encrypted_in_scope(Q, Ctes) ++ unshadowed(OuterEncrypted, Q, Ctes), Ctes}.

encrypted_in_scope(Q, Ctes) ->
    lists:append([encrypted_fields(S) ++ cte_encrypted(S, Ctes) || S <- sources(Q)]).

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
%% of its sources, and each alias it selects one under.
cte_exports(#kura_query{ctes = Own, select = Select} = Body, OuterCtes) ->
    Encrypted = encrypted_in_scope(Body, Own ++ OuterCtes),
    Encrypted ++ ciphertext_aliases(Select, Encrypted).

ciphertext_aliases({exprs, Exprs}, Encrypted) ->
    [Alias || {Alias, Expr} <- Exprs, ciphertext_expr(Expr, Encrypted)];
ciphertext_aliases(Fields, Encrypted) ->
    [Agg || {Agg, _} = Expr <- Fields, ciphertext_expr(Expr, Encrypted)].

ciphertext_expr(Field, Encrypted) when is_atom(Field) ->
    lists:member(Field, Encrypted);
ciphertext_expr({Agg, Field}, Encrypted) when Agg =:= min; Agg =:= max ->
    lists:member(Field, Encrypted);
ciphertext_expr({over, Fn, _Spec}, Encrypted) ->
    ciphertext_expr(Fn, Encrypted);
ciphertext_expr(_Expr, _Encrypted) ->
    false.

refuse_encrypted({[], []}, _Conditions, _Reason) ->
    ok;
refuse_encrypted(Scope, Conditions, Reason) ->
    refuse_each(Conditions, Scope, Reason).

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
refuse_encrypted_cond({Field, _Value}, Scope, Reason) when is_atom(Field) ->
    refuse_field(Field, Scope, Reason);
refuse_encrypted_cond({Field, _Op, _Value}, Scope, Reason) when is_atom(Field) ->
    refuse_field(Field, Scope, Reason);
refuse_encrypted_cond({Fields, _Op, _Value}, Scope, Reason) when is_list(Fields) ->
    refuse_fields(Fields, Scope, Reason);
refuse_encrypted_cond(_Cond, _Scope, _Reason) ->
    erlang:error({kura, unrecognised_condition}).

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
    refuse_encrypted(Scope, W, encrypted_field_in_where),
    refuse_encrypted(Scope, H, encrypted_field_in_having),
    lists:foreach(
        fun({_, Body}) -> refuse_encrypted_query(Body, OuterEncrypted, Ctes) end, Own ++ C
    ).

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
    case lists:member(Field, Encrypted) of
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

-doc "Set ORDER BY clauses as `[{field, asc | desc}]`.".
-spec order_by(#kura_query{}, [{atom(), asc | desc}]) -> #kura_query{}.
order_by(Q, Orders) ->
    Q#kura_query{order_bys = Orders}.

-spec group_by(#kura_query{}, [atom()]) -> #kura_query{}.
group_by(Q, Fields) ->
    Q#kura_query{group_bys = Fields}.

-doc """
Add a HAVING condition, in the same forms as `where/2`. A condition that
compares an encrypted field to a value raises
`{kura, {encrypted_field_in_having, Field}}`, as `where/2` does.
""".
-spec having(#kura_query{}, term()) -> #kura_query{}.
having(Q = #kura_query{havings = H}, Condition) ->
    refuse_encrypted(guard_scope(Q, [], []), [Condition], encrypted_field_in_having),
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

-spec distinct(#kura_query{}, [atom()]) -> #kura_query{}.
distinct(Q, Fields) ->
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
    refuse_encrypted_query(Q, [], []),
    Q.

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
    Q#kura_query{select = [{sum, Field}]}.

-spec avg(#kura_query{}, atom()) -> #kura_query{}.
avg(Q, Field) ->
    Q#kura_query{select = [{avg, Field}]}.

-spec min(#kura_query{}, atom()) -> #kura_query{}.
min(Q, Field) ->
    Q#kura_query{select = [{min, Field}]}.

-spec max(#kura_query{}, atom()) -> #kura_query{}.
max(Q, Field) ->
    Q#kura_query{select = [{max, Field}]}.
