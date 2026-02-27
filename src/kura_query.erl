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
    count/1, count/2,
    sum/2,
    avg/2,
    min/2,
    max/2
]).

-doc "Start a query from the given schema module or table atom.".
-spec from(atom() | module()) -> #kura_query{}.
from(Source) ->
    #kura_query{from = Source}.

-doc "Set the SELECT fields. Pass atoms for columns or `{agg, field}` tuples for aggregates.".
-spec select(#kura_query{}, [atom() | term()]) -> #kura_query{}.
select(Q, Fields) ->
    Q#kura_query{select = Fields}.

-doc "Set raw SQL expressions in SELECT with aliases. Exprs = `[{Alias, {fragment, SQL, Params}}]`.".
-spec select_expr(#kura_query{}, [term()]) -> #kura_query{}.
select_expr(Q, Exprs) ->
    Q#kura_query{select = {exprs, Exprs}}.

-doc "Add a WHERE condition. Conditions: `{field, value}`, `{field, op, value}`, `{'and', [...]}`, etc.".
-spec where(#kura_query{}, term()) -> #kura_query{}.
where(Q = #kura_query{wheres = W}, Condition) ->
    Q#kura_query{wheres = W ++ [Condition]}.

-doc "Add a JOIN clause. `On` is `{LeftCol, RightCol}`.".
-spec join(#kura_query{}, inner | left | right | full, atom(), {atom(), atom()}) -> #kura_query{}.
join(Q, Type, Table, On) ->
    join(Q, Type, Table, On, undefined).

-spec join(
    #kura_query{}, inner | left | right | full, atom(), {atom(), atom()}, atom() | undefined
) -> #kura_query{}.
join(Q = #kura_query{joins = J}, Type, Table, On, As) ->
    Q#kura_query{joins = J ++ [{Type, Table, On, As}]}.

-doc "Set ORDER BY clauses as `[{field, asc | desc}]`.".
-spec order_by(#kura_query{}, [{atom(), asc | desc}]) -> #kura_query{}.
order_by(Q, Orders) ->
    Q#kura_query{order_bys = Orders}.

-spec group_by(#kura_query{}, [atom()]) -> #kura_query{}.
group_by(Q, Fields) ->
    Q#kura_query{group_bys = Fields}.

-spec having(#kura_query{}, term()) -> #kura_query{}.
having(Q = #kura_query{havings = H}, Condition) ->
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

-doc "Add a Common Table Expression (WITH clause).".
-spec with_cte(#kura_query{}, binary(), #kura_query{}) -> #kura_query{}.
with_cte(Q = #kura_query{ctes = CTEs}, Name, CteQuery) ->
    Q#kura_query{ctes = CTEs ++ [{Name, CteQuery}]}.

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
