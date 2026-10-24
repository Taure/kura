-module(kura_query_shape).
-moduledoc """
Separates a query's shape from the values bound into it, so the query
cache can key on the shape (kura#163).

`split/1` replaces every bound value in a `#kura_query{}` with a marker
`{'$kura_param', N}` and returns the values in a tuple. Compiling the
shape yields SQL plus a parameter template in which each marker stands
where its value would be. `bind/2` puts the values back.

Bound positions, in the query and in every CTE, combination and
subquery it contains:

- the right-hand side of a comparison, including `like`, `ilike` and
  `matches`, and of `{Field, Value}`;
- the elements of an `in`/`not_in` list, and of each row tuple in a
  row-value `in` whose tuples all match the column count;
- both ends of a `between`;
- the parameters of a condition fragment, in `wheres` or `havings`;
- `limit` and `offset`.

`nil` and `undefined` stay in the shape wherever they appear, so a
dialect that compiles them differently from other values still gets them.
Unary conditions (`?KURA_IS_UNARY_OP`), select-expression fragments and
anything not recognised also stay in the shape, so an unrecognised
condition costs a cache entry per value set rather than correctness.

A dialect is free to emit any SQL for a shape, but it must pass a bound
value through to the parameter list untouched. `valid_template/2` checks
the half of that a template can show: every marker appears exactly once,
at the top level. A template that fails is never cached.

This module is internal. The marker format is not part of kura's API.
""".

-include("kura.hrl").

-export([split/1, valid_template/2, bind/2]).

-define(MARKER, '$kura_param').
-define(IS_COMPARISON(Op),
    (Op =:= '=' orelse Op =:= '!=' orelse Op =:= '<' orelse Op =:= '>' orelse Op =:= '<=' orelse
        Op =:= '>=' orelse Op =:= like orelse Op =:= ilike orelse Op =:= matches)
).

-type acc() :: {pos_integer(), [term()]}.

-doc "Split `Query` into its shape and the tuple of values bound into it.".
-spec split(#kura_query{}) -> {#kura_query{}, tuple()}.
split(Query) ->
    {Shape, {_, Values}} = query(Query, {1, []}),
    {Shape, list_to_tuple(lists:reverse(Values))}.

-doc """
True when `Params`, compiled from a shape with `Arity` bound values,
holds each marker once as a top-level element and nowhere else.
""".
-spec valid_template([term()], non_neg_integer()) -> boolean().
valid_template(Params, Arity) ->
    Indices = [N || {?MARKER, N} <- Params],
    Rest = [P || P <- Params, not is_marker(P)],
    lists:sort(Indices) =:= lists:seq(1, Arity) andalso not lists:any(fun has_marker/1, Rest).

-doc "Substitute `Values` for the markers in a template produced by a shape.".
-spec bind([term()], tuple()) -> [term()].
bind(Template, Values) ->
    [bind_param(P, Values) || P <- Template].

-spec bind_param(term(), tuple()) -> term().
bind_param({?MARKER, N}, Values) when is_integer(N) -> element(N, Values);
bind_param(P, _Values) -> P.

-spec query(#kura_query{}, acc()) -> {#kura_query{}, acc()}.
query(Q, Acc0) ->
    {Wheres, Acc1} = conditions(Q#kura_query.wheres, Acc0),
    {Havings, Acc2} = conditions(Q#kura_query.havings, Acc1),
    {Limit, Acc3} = value(Q#kura_query.limit, Acc2),
    {Offset, Acc4} = value(Q#kura_query.offset, Acc3),
    {Ctes, Acc5} = named_queries(Q#kura_query.ctes, Acc4),
    {Combinations, Acc6} = named_queries(Q#kura_query.combinations, Acc5),
    Shape = Q#kura_query{
        wheres = Wheres,
        havings = Havings,
        limit = Limit,
        offset = Offset,
        ctes = Ctes,
        combinations = Combinations
    },
    {Shape, Acc6}.

-spec named_queries([{Name, #kura_query{}}], acc()) -> {[{Name, #kura_query{}}], acc()}.
named_queries([], Acc) ->
    {[], Acc};
named_queries([{Name, Q} | Rest], Acc0) ->
    {Shape, Acc1} = query(Q, Acc0),
    {Shapes, Acc2} = named_queries(Rest, Acc1),
    {[{Name, Shape} | Shapes], Acc2}.

-spec conditions([term()], acc()) -> {[term()], acc()}.
conditions([], Acc) ->
    {[], Acc};
conditions([Condition | Rest], Acc0) ->
    {Shape, Acc1} = condition(Condition, Acc0),
    {Shapes, Acc2} = conditions(Rest, Acc1),
    {[Shape | Shapes], Acc2}.

-spec condition(term(), acc()) -> {term(), acc()}.
condition({Op, Conditions}, Acc0) when (Op =:= 'and' orelse Op =:= 'or'), is_list(Conditions) ->
    {Shapes, Acc1} = conditions(Conditions, Acc0),
    {{Op, Shapes}, Acc1};
condition({'not', Condition}, Acc0) ->
    {Shape, Acc1} = condition(Condition, Acc0),
    {{'not', Shape}, Acc1};
condition({fragment, SQL, Params}, Acc0) when is_list(Params) ->
    {Markers, Acc1} = values(Params, Acc0),
    {{fragment, SQL, Markers}, Acc1};
condition({_Field, Op} = Condition, Acc) when ?KURA_IS_UNARY_OP(Op) ->
    {Condition, Acc};
condition({Exists, {subquery, SubQ}}, Acc0) when Exists =:= exists; Exists =:= not_exists ->
    {Shape, Acc1} = subquery(SubQ, Acc0),
    {{Exists, Shape}, Acc1};
condition({Field, in, {subquery, SubQ}}, Acc0) ->
    {Shape, Acc1} = subquery(SubQ, Acc0),
    {{Field, in, Shape}, Acc1};
condition({Field, In, Values}, Acc0) when
    is_atom(Field), is_list(Values), (In =:= in orelse In =:= not_in)
->
    {Markers, Acc1} = values(Values, Acc0),
    {{Field, In, Markers}, Acc1};
condition({Cols, in, Rows} = Condition, Acc0) when is_list(Cols), is_list(Rows) ->
    Arity = length(Cols),
    case lists:all(fun(Row) -> is_tuple(Row) andalso tuple_size(Row) =:= Arity end, Rows) of
        true ->
            {Markers, Acc1} = rows(Rows, Acc0),
            {{Cols, in, Markers}, Acc1};
        false ->
            {Condition, Acc0}
    end;
condition({Field, between, {Low, High}}, Acc0) when is_atom(Field) ->
    {LowM, Acc1} = value(Low, Acc0),
    {HighM, Acc2} = value(High, Acc1),
    {{Field, between, {LowM, HighM}}, Acc2};
condition({Field, Op, Value}, Acc0) when ?IS_COMPARISON(Op), is_atom(Field) ->
    {Marker, Acc1} = value(Value, Acc0),
    {{Field, Op, Marker}, Acc1};
condition({Field, Value}, Acc0) when is_atom(Field) ->
    {Marker, Acc1} = value(Value, Acc0),
    {{Field, Marker}, Acc1};
condition(Condition, Acc) ->
    {Condition, Acc}.

-spec subquery(term(), acc()) -> {{subquery, term()}, acc()}.
subquery(#kura_query{} = SubQ, Acc0) ->
    {Shape, Acc1} = query(SubQ, Acc0),
    {{subquery, Shape}, Acc1};
subquery(SubQ, Acc) ->
    {{subquery, SubQ}, Acc}.

-spec rows([term()], acc()) -> {[tuple()], acc()}.
rows([], Acc) ->
    {[], Acc};
rows([Row | Rest], Acc0) when is_tuple(Row) ->
    {Markers, Acc1} = values(tuple_to_list(Row), Acc0),
    {Shapes, Acc2} = rows(Rest, Acc1),
    {[list_to_tuple(Markers) | Shapes], Acc2}.

-spec values([term()], acc()) -> {[term()], acc()}.
values([], Acc) ->
    {[], Acc};
values([Value | Rest], Acc0) ->
    {Marker, Acc1} = value(Value, Acc0),
    {Markers, Acc2} = values(Rest, Acc1),
    {[Marker | Markers], Acc2}.

%% No spec: a marker stands in for a value of any type, including limit and
%% offset, whose record fields are typed as integers.
value(Value, Acc) when Value =:= nil; Value =:= undefined ->
    {Value, Acc};
value(Value, {N, Values}) ->
    {{?MARKER, N}, {N + 1, [Value | Values]}}.

-spec is_marker(term()) -> boolean().
is_marker({?MARKER, N}) when is_integer(N) -> true;
is_marker(_) -> false.

-spec has_marker(term()) -> boolean().
has_marker({?MARKER, _}) -> true;
has_marker(T) when is_tuple(T) -> lists:any(fun has_marker/1, tuple_to_list(T));
has_marker([H | T]) -> has_marker(H) orelse has_marker(T);
has_marker(M) when is_map(M) -> has_marker(maps:to_list(M));
has_marker(_) -> false.
