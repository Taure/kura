-module(kura_encrypted_guard_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(ENC, kura_test_encrypted_schema).
-define(PLAIN, ~"123-45-6789").
-define(WHERE_SSN, {kura, {encrypted_field_in_where, ssn}}).

%% Every condition form that binds a value to the encrypted field `ssn`.
%% The first four are the bypasses the old first-element check let through.
refused() ->
    P = ?PLAIN,
    [
        {"not", {'not', {ssn, P}}},
        {"single-column row in", {[ssn], in, [{P}]}},
        {"multi-column row in", {[id, ssn], in, [{1, P}]}},
        {"multi-column row in, encrypted first", {[ssn, id], in, [{P, 1}]}},
        {"bare equality", {ssn, P}},
        {"in", {ssn, in, [P]}},
        {"not_in", {ssn, not_in, [P]}},
        {"between", {ssn, between, {P, P}}},
        {"in subquery", {ssn, in, {subquery, kura_query:from(?ENC)}}},
        {"double not", {'not', {'not', {ssn, P}}}},
        {"and", {'and', [{id, 1}, {ssn, P}]}},
        {"or", {'or', [{id, 1}, {ssn, P}]}},
        {"nested and/or/not", {'and', [{id, 1}, {'or', [{name, ~"x"}, {'not', {ssn, P}}]}]}},
        {"not over and", {'not', {'and', [{ssn, '!=', P}]}}}
    ] ++
        [
            {atom_to_list(Op), {ssn, Op, P}}
         || Op <- ['=', '!=', '<', '>', '<=', '>=', like, ilike, matches]
        ].

allowed() ->
    P = ?PLAIN,
    Sub = kura_query:from(?ENC),
    [
        {"is_nil", {ssn, is_nil}},
        {"is_not_nil", {ssn, is_not_nil}},
        {"not is_nil", {'not', {ssn, is_nil}}},
        {"plain field", {name, P}},
        {"plain row in", {[id, name], in, [{1, P}]}},
        {"fragment", {fragment, ~"1 = ?", [1]}},
        {"exists subquery", {exists, {subquery, Sub}}},
        {"not_exists subquery", {not_exists, {subquery, Sub}}},
        {"plain field in subquery", {id, in, {subquery, Sub}}}
    ].

where_refuses_test_() ->
    [
        {Name,
            ?_assertError(
                {kura, {encrypted_field_in_where, ssn}},
                kura_query:where(kura_query:from(?ENC), Cond)
            )}
     || {Name, Cond} <- refused()
    ].

having_refuses_test_() ->
    [
        {Name,
            ?_assertError(
                {kura, {encrypted_field_in_having, ssn}},
                kura_query:having(kura_query:from(?ENC), Cond)
            )}
     || {Name, Cond} <- refused()
    ].

where_allows_test_() ->
    [
        {Name, ?_assertMatch(#kura_query{}, kura_query:where(kura_query:from(?ENC), Cond))}
     || {Name, Cond} <- allowed()
    ].

having_allows_test_() ->
    [
        {Name, ?_assertMatch(#kura_query{}, kura_query:having(kura_query:from(?ENC), Cond))}
     || {Name, Cond} <- allowed()
    ].

%% The plaintext must not reach the compiled parameters, which go to the
%% server, the query log and telemetry. Building the query raises first; the
%% control shows the same forms would bind the plaintext on a schema where
%% the field is not encrypted, so the search below can find it.
plaintext_never_compiled_test_() ->
    [
        {Name, fun() ->
            ?assertEqual(refused, compiled_params(?ENC, Cond)),
            ?assert(contains_plaintext(compiled_params(kura_test_schema, rename(Cond))))
        end}
     || {Name, Cond} <- refused(), not is_subquery(Cond)
    ].

compiled_params(Schema, Cond) ->
    try kura_query:where(kura_query:from(Schema), Cond) of
        Q ->
            {_SQL, Params} = kura_dialect_pg:to_sql(Q),
            Params
    catch
        error:{kura, {encrypted_field_in_where, _}} -> refused
    end.

contains_plaintext(Params) ->
    binary:match(term_to_binary(Params), ?PLAIN) =/= nomatch.

is_subquery({ssn, in, {subquery, _}}) -> true;
is_subquery(_) -> false.

%% kura_test_schema has no ssn; `email` stands in for it in the control.
rename(ssn) -> email;
rename(T) when is_tuple(T) -> list_to_tuple([rename(E) || E <- tuple_to_list(T)]);
rename(L) when is_list(L) -> [rename(E) || E <- L];
rename(X) -> X.

unrecognised_condition_on_an_encrypted_schema_test_() ->
    Q = kura_query:from(?ENC),
    [
        ?_assertError({kura, unrecognised_condition}, kura_query:where(Q, Cond))
     || Cond <- [?PLAIN, {?PLAIN}, {ssn, '=', ?PLAIN, extra}, {~"ssn", ?PLAIN}]
    ].

unrecognised_condition_on_a_plain_schema_is_left_to_the_compiler_test() ->
    ?assertMatch(#kura_query{}, kura_query:where(kura_query:from(kura_test_schema), {1, 2, 3, 4})).

where_on_a_joined_encrypted_schema_test() ->
    Q = kura_query:join(kura_query:from(kura_test_schema), inner, ?ENC, {id, id}),
    ?assertError({kura, {encrypted_field_in_where, ssn}}, kura_query:where(Q, {ssn, ?PLAIN})).

where_before_joining_an_encrypted_schema_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {'not', {ssn, ?PLAIN}}),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}}, kura_query:join(Q, inner, ?ENC, {id, id})
    ).

having_before_joining_an_encrypted_schema_test() ->
    Q = kura_query:having(kura_query:from(kura_test_schema), {ssn, ?PLAIN}),
    ?assertError(
        {kura, {encrypted_field_in_having, ssn}}, kura_query:join(Q, left, ?ENC, {id, id})
    ).

join_to_a_bare_table_test() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {ssn, ?PLAIN}),
    ?assertMatch(#kura_query{}, kura_query:join(Q, inner, some_raw_table, {id, id})).

tenant_attribute_on_an_encrypted_field_test() ->
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_tenant:scope_query({attribute, {ssn, ?PLAIN}}, kura_query:from(?ENC))
    ).

tenant_attribute_on_an_encrypted_field_in_a_subquery_test() ->
    Sub = kura_query:from(?ENC),
    Q = kura_query:where(kura_query:from(kura_test_schema), {exists, {subquery, Sub}}),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_tenant:scope_query({attribute, {ssn, ?PLAIN}}, Q)
    ).

%%----------------------------------------------------------------------
%% Correlated subqueries: the dialect emits unqualified column names, so a
%% name the subquery's own sources lack resolves to the outer query's column.
%%----------------------------------------------------------------------

correlated(Cond) ->
    kura_query:where(kura_query:from(kura_test_schema), Cond).

correlated_subquery_refused_test_() ->
    P = ?PLAIN,
    Sub = correlated({ssn, P}),
    [
        {"exists", ?_assertError(?WHERE_SSN, where_enc({exists, {subquery, Sub}}))},
        {"not_exists", ?_assertError(?WHERE_SSN, where_enc({not_exists, {subquery, Sub}}))},
        {"in", ?_assertError(?WHERE_SSN, where_enc({id, in, {subquery, Sub}}))},
        {"under not and or",
            ?_assertError(
                ?WHERE_SSN, where_enc({'not', {'or', [{id, 1}, {exists, {subquery, Sub}}]}})
            )},
        {"in the subquery's having",
            ?_assertError(
                {kura, {encrypted_field_in_having, ssn}},
                where_enc(
                    {exists,
                        {subquery, kura_query:having(kura_query:from(kura_test_schema), {ssn, P})}}
                )
            )},
        {"two levels deep",
            ?_assertError(
                ?WHERE_SSN, where_enc({exists, {subquery, correlated({exists, {subquery, Sub}})}})
            )},
        {"in a union branch of the subquery",
            ?_assertError(
                ?WHERE_SSN,
                where_enc(
                    {exists, {subquery, kura_query:union(kura_query:from(kura_test_schema), Sub)}}
                )
            )},
        {"over a bare table",
            ?_assertError(
                ?WHERE_SSN,
                where_enc(
                    {exists, {subquery, kura_query:where(kura_query:from(some_table), {ssn, P})}}
                )
            )},
        {"in having",
            ?_assertError(
                ?WHERE_SSN, kura_query:having(kura_query:from(?ENC), {exists, {subquery, Sub}})
            )},
        {"via a joined encrypted schema",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    kura_query:join(kura_query:from(kura_test_schema), inner, ?ENC, {id, id}),
                    {exists, {subquery, Sub}}
                )
            )},
        {"added before joining an encrypted schema",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:join(correlated({exists, {subquery, Sub}}), inner, ?ENC, {id, id})
            )},
        {"through a virtual field of the subquery's schema",
            ?_assertError(
                {kura, {encrypted_field_in_where, secret}},
                kura_query:where(
                    kura_query:from(kura_test_enc_embedded),
                    {exists,
                        {subquery,
                            kura_query:where(
                                kura_query:from(kura_test_plain_ssn_schema), {secret, P}
                            )}}
                )
            )}
    ].

correlated_subquery_allowed_test_() ->
    P = ?PLAIN,
    [
        {"the subquery's own column shadows the outer one",
            ?_assertMatch(
                #kura_query{},
                where_enc(
                    {exists,
                        {subquery,
                            kura_query:where(kura_query:from(kura_test_plain_ssn_schema), {ssn, P})}}
                )
            )},
        {"a plain outer field",
            ?_assertMatch(
                #kura_query{}, where_enc({exists, {subquery, correlated({name, P})}})
            )},
        {"is_nil on the outer field",
            ?_assertMatch(
                #kura_query{}, where_enc({exists, {subquery, correlated({ssn, is_nil})}})
            )}
    ].

where_enc(Cond) ->
    kura_query:where(kura_query:from(?ENC), Cond).

%%----------------------------------------------------------------------
%% CTEs: a source named after a CTE reads the CTE's columns.
%%----------------------------------------------------------------------

enc_cte(Q) ->
    kura_query:with_cte(Q, ~"enc_cte", kura_query:from(?ENC)).

cte_refused_test_() ->
    P = ?PLAIN,
    Plain = kura_query:from(kura_test_schema),
    [
        {"where after with_cte",
            ?_assertError(
                ?WHERE_SSN, kura_query:where(enc_cte(kura_query:from(enc_cte)), {ssn, P})
            )},
        {"where before with_cte",
            ?_assertError(
                ?WHERE_SSN, enc_cte(kura_query:where(kura_query:from(enc_cte), {'not', {ssn, P}}))
            )},
        {"having before with_cte",
            ?_assertError(
                {kura, {encrypted_field_in_having, ssn}},
                enc_cte(kura_query:having(kura_query:from(enc_cte), {ssn, P}))
            )},
        {"join to a CTE, then where",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    kura_query:join(enc_cte(Plain), inner, enc_cte, {id, id}), {ssn, P}
                )
            )},
        {"where, join to a CTE, then with_cte",
            ?_assertError(
                ?WHERE_SSN,
                enc_cte(
                    kura_query:join(kura_query:where(Plain, {ssn, P}), inner, enc_cte, {id, id})
                )
            )},
        {"a schema whose table is named like the CTE",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    kura_query:with_cte(Plain, ~"users", kura_query:from(?ENC)), {ssn, P}
                )
            )},
        {"two CTEs of one name",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    kura_query:with_cte(
                        kura_query:with_cte(kura_query:from(c), ~"c", Plain),
                        ~"c",
                        kura_query:from(?ENC)
                    ),
                    {ssn, P}
                )
            )},
        {"an alias of an encrypted field",
            ?_assertError(
                {kura, {encrypted_field_in_where, alias}},
                kura_query:where(
                    kura_query:with_cte(
                        kura_query:from(c),
                        ~"c",
                        kura_query:select_expr(kura_query:from(?ENC), [{alias, ssn}])
                    ),
                    {alias, P}
                )
            )},
        {"max of an encrypted field",
            ?_assertError(
                {kura, {encrypted_field_in_aggregate, ssn}},
                kura_query:where(
                    kura_query:with_cte(
                        kura_query:from(c), ~"c", kura_query:max(kura_query:from(?ENC), ssn)
                    ),
                    {max, P}
                )
            )},
        {"a CTE reading another CTE",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    kura_query:with_cte(
                        enc_cte(kura_query:from(c2)), ~"c2", kura_query:from(enc_cte)
                    ),
                    {ssn, P}
                )
            )},
        {"a CTE body filtering an earlier CTE",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:with_cte(
                    enc_cte(Plain), ~"c2", kura_query:where(kura_query:from(enc_cte), {ssn, P})
                )
            )},
        {"a subquery reading the outer query's CTE",
            ?_assertError(
                ?WHERE_SSN,
                kura_query:where(
                    enc_cte(Plain),
                    {exists, {subquery, kura_query:where(kura_query:from(enc_cte), {ssn, P})}}
                )
            )}
    ].

cte_allowed_test_() ->
    P = ?PLAIN,
    Plain = kura_query:from(kura_test_schema),
    [
        {"a plain field of an encrypted CTE",
            ?_assertMatch(
                #kura_query{}, kura_query:where(enc_cte(kura_query:from(enc_cte)), {name, P})
            )},
        {"is_nil on an encrypted CTE field",
            ?_assertMatch(
                #kura_query{}, kura_query:where(enc_cte(kura_query:from(enc_cte)), {ssn, is_nil})
            )},
        {"a CTE over a plain schema",
            ?_assertMatch(
                #kura_query{},
                kura_query:where(kura_query:with_cte(kura_query:from(c), ~"c", Plain), {email, P})
            )},
        {"an alias of a plain field",
            ?_assertMatch(
                #kura_query{},
                kura_query:where(
                    kura_query:with_cte(
                        kura_query:from(c),
                        ~"c",
                        kura_query:select_expr(kura_query:from(?ENC), [{alias, name}])
                    ),
                    {alias, P}
                )
            )},
        {"a self-referencing CTE terminates",
            ?_assertMatch(
                #kura_query{},
                kura_query:where(
                    kura_query:with_cte(kura_query:from(r), ~"r", kura_query:from(r)), {id, 1}
                )
            )}
    ].

%%----------------------------------------------------------------------
%% Malformed conditions raise a reason that carries no value. A
%% function_clause would carry the plaintext in its arguments.
%%----------------------------------------------------------------------

malformed_condition_test_() ->
    P = ?PLAIN,
    [
        {Name, fun() -> assert_unrecognised_without_plaintext(Cond) end}
     || {Name, Cond} <- [
            {"improper and", {'and', [{id, 1} | ill_typed({ssn, P})]}},
            {"improper or", {'or', [{id, 1}, {name, P} | ill_typed({ssn, P})]}},
            {"improper nested and", {'not', {'and', [{'or', [{id, 1} | ill_typed(P)]}]}}},
            {"and over a tuple", {'and', {ssn, P}}},
            {"or over a binary", {'or', P}},
            {"improper row in", {[id | ill_typed(ssn)], in, [{1, P}]}}
        ]
    ].

%% An improper list, which the condition types do not allow.
-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.

assert_unrecognised_without_plaintext(Cond) ->
    try kura_query:where(kura_query:from(?ENC), Cond) of
        Q -> ?assertEqual(raised, Q)
    catch
        error:Reason:Stack ->
            ?assertEqual({kura, unrecognised_condition}, Reason),
            ?assertEqual(nomatch, binary:match(term_to_binary(Stack), ?PLAIN))
    end.

%%----------------------------------------------------------------------
%% kura_query_compiler re-runs the guard on every query it compiles, so a
%% record built or updated without where/2 cannot bind the plaintext.
%%----------------------------------------------------------------------

-define(REPO, kura_encrypted_guard_repo).

compiler_setup() ->
    application:set_env(kura, repos, #{?REPO => #{dialect => kura_dialect_pg}}),
    kura_query_cache:init().

compiler_teardown(_) ->
    kura_query_cache:flush(),
    application:unset_env(kura, repos).

compile_paths() ->
    [
        {"to_sql", fun(Q) -> kura_query_compiler:to_sql(?REPO, Q) end},
        {"to_sql_cached", fun(Q) -> kura_query_compiler:to_sql_cached(?REPO, Q) end},
        {"to_sql_from", fun(Q) -> kura_query_compiler:to_sql_from(?REPO, Q, 1) end},
        {"update_all", fun(Q) -> kura_query_compiler:update_all(?REPO, Q, #{name => ~"x"}) end},
        {"delete_all", fun(Q) -> kura_query_compiler:delete_all(?REPO, Q) end}
    ].

compiler_refuses_test_() ->
    {setup, fun compiler_setup/0, fun compiler_teardown/1, [
        {Name ++ " via " ++ Path, ?_assertError(Reason, Compile(Q))}
     || {Name, Reason, Q} <- compiler_refused(), {Path, Compile} <- compile_paths()
    ]}.

compiler_refused() ->
    P = ?PLAIN,
    Plain = kura_query:from(kura_test_schema),
    [
        {"raw record", ?WHERE_SSN, #kura_query{from = ?ENC, wheres = [{ssn, P}]}},
        {"record update after where", ?WHERE_SSN, (kura_query:where(Plain, {ssn, P}))#kura_query{
            from = ?ENC
        }},
        {"raw having", {kura, {encrypted_field_in_having, ssn}}, #kura_query{
            from = ?ENC, havings = [{'not', {ssn, P}}]
        }},
        {"raw subquery in a plain query", ?WHERE_SSN, Plain#kura_query{
            wheres = [{exists, {subquery, #kura_query{from = ?ENC, wheres = [{ssn, P}]}}}]
        }},
        {"raw CTE body", ?WHERE_SSN, Plain#kura_query{
            ctes = [{~"c", #kura_query{from = ?ENC, wheres = [{ssn, P}]}}]
        }},
        {"union branch over the outer query's CTE", ?WHERE_SSN,
            kura_query:union(enc_cte(Plain), kura_query:where(kura_query:from(enc_cte), {ssn, P}))},
        {"raw improper and", {kura, unrecognised_condition}, #kura_query{
            from = ?ENC, wheres = [{'and', [{id, 1} | ill_typed({ssn, P})]}]
        }}
    ].

compiler_allows_test_() ->
    Queries = [
        kura_query:where(kura_query:from(kura_test_schema), {email, ?PLAIN}),
        kura_query:where(kura_query:from(?ENC), {ssn, is_nil}),
        kura_query:where(kura_query:from(?ENC), {name, ?PLAIN})
    ],
    {setup, fun compiler_setup/0, fun compiler_teardown/1, [
        {Path, fun() -> [Compile(Q) || Q <- Queries] end}
     || {Path, Compile} <- compile_paths()
    ]}.

%%----------------------------------------------------------------------
%% to_sql_cached/2 runs the guard on a miss only. That is sound because the
%% guard's verdict is a function of the query's shape: it never reads a
%% bound value, which is all the shape replaces.
%%----------------------------------------------------------------------

verdict(Q) ->
    try kura_query:check_encrypted(Q) of
        ok -> ok
    catch
        error:Reason -> Reason
    end.

verdict_is_a_function_of_the_shape_test_() ->
    P = ?PLAIN,
    Sub = correlated({ssn, P}),
    Conds =
        [C || {_, C} <- refused() ++ allowed()] ++
            [
                {exists, {subquery, Sub}},
                {id, in, {subquery, Sub}},
                {name, {ssn, P}},
                {name, in, [{ssn, P}, {'and', [{ssn, P}]}]},
                {name, between, {{ssn, P}, {exists, {subquery, Sub}}}},
                {fragment, ~"x = ?", [{ssn, P}]},
                {'and', {ssn, P}},
                {exists, {ssn, P}},
                {exists, {subquery, not_a_query}},
                {id, '=', {subquery, Sub}},
                {id, {subquery, Sub}},
                {id, between, {subquery, Sub}},
                {[id], not_in, {subquery, Sub}},
                {fragment, ~"x = ?", [{subquery, Sub}]},
                {ssn, nil},
                {ssn, undefined},
                {[id, name], in, [{1, {ssn, P}}]},
                {[id, ssn], in, [{1}]}
            ],
    AliasConds = [
        {alias, P},
        {alias, in, [P]},
        {'not', {alias, '>', P}},
        {alias, is_nil},
        {name, {alias, P}}
    ],
    AliasCte = [{~"c", kura_query:select_expr(kura_query:from(?ENC), [{alias, ssn}])}],
    Queries =
        [#kura_query{from = ?ENC, wheres = [C]} || C <- Conds] ++
            [#kura_query{from = kura_test_schema, wheres = [C]} || C <- Conds] ++
            [#kura_query{from = ?ENC, havings = [C]} || C <- Conds] ++
            [
                kura_query:union(enc_cte(kura_query:from(kura_test_schema)), #kura_query{
                    from = enc_cte, wheres = [C]
                })
             || C <- Conds
            ] ++
            [
                #kura_query{
                    from = kura_test_schema,
                    joins = [{inner, ?ENC, {id, id}, undefined}],
                    wheres = [C]
                }
             || C <- Conds
            ] ++
            [
                #kura_query{
                    from = enc_cte, ctes = [{~"enc_cte", kura_query:from(?ENC)}], wheres = [C]
                }
             || C <- Conds
            ] ++
            [#kura_query{from = c, ctes = AliasCte, wheres = [C]} || C <- AliasConds] ++
            [Q || {_, _, Q} <- compiler_refused(), splits(Q)],
    [
        ?_assertEqual(verdict(Q), verdict(element(1, kura_query_shape:split(Q))))
     || Q <- Queries
    ].

%% A query the split cannot take apart has no shape; to_sql_cached/2 sends
%% it to to_sql/2, which checks it on every call.
splits(Q) ->
    try kura_query_shape:split(Q) of
        _ -> true
    catch
        error:_ -> false
    end.

cached_compile_test_() ->
    P = ?PLAIN,
    Raw = #kura_query{from = ?ENC, wheres = [{ssn, P}]},
    {foreach, fun compiler_setup/0, fun compiler_teardown/1, [
        {"an encrypted condition raises on the first and on every later call", fun() ->
            Before = ets:info(kura_query_cache, size),
            ?assertError(?WHERE_SSN, kura_query_compiler:to_sql_cached(?REPO, Raw)),
            ?assertError(?WHERE_SSN, kura_query_compiler:to_sql_cached(?REPO, Raw)),
            ?assertEqual(Before, ets:info(kura_query_cache, size))
        end},
        {"an uncacheable entry still checks", fun() ->
            {Shape, _} = kura_query_shape:split(Raw),
            kura_query_cache:put({?REPO, kura_tenant:get_tenant(), Shape}, uncacheable),
            ?assertError(?WHERE_SSN, kura_query_compiler:to_sql_cached(?REPO, Raw)),
            ?assertError(?WHERE_SSN, kura_query_compiler:to_sql_cached(?REPO, Raw))
        end},
        {"a query the shape split cannot take apart is checked, without the plaintext", fun() ->
            Q = #kura_query{from = ?ENC, wheres = [{'and', [{id, 1} | ill_typed({ssn, P})]}]},
            try kura_query_compiler:to_sql_cached(?REPO, Q) of
                R -> ?assertEqual(raised, R)
            catch
                error:Reason:Stack ->
                    ?assertEqual({kura, unrecognised_condition}, Reason),
                    ?assertEqual(nomatch, binary:match(term_to_binary(Stack), P))
            end
        end},
        {"a hit binds the new values", fun() ->
            Q = fun(V) -> kura_query:where(kura_query:from(?ENC), {name, V}) end,
            {SQL, [~"a"]} = kura_query_compiler:to_sql_cached(?REPO, Q(~"a")),
            Size = ets:info(kura_query_cache, size),
            ?assertMatch({SQL, [~"b"]}, kura_query_compiler:to_sql_cached(?REPO, Q(~"b"))),
            ?assertEqual(Size, ets:info(kura_query_cache, size))
        end}
    ]}.

%%----------------------------------------------------------------------
%% A subquery in a value position is walked too.
%%----------------------------------------------------------------------

subquery_value_refused_test_() ->
    Sub = correlated({ssn, ?PLAIN}),
    [
        {Name, ?_assertError(?WHERE_SSN, where_enc(Cond))}
     || {Name, Cond} <- [
            {"comparison", {id, '=', {subquery, Sub}}},
            {"equality", {id, {subquery, Sub}}},
            {"not_in", {id, not_in, {subquery, Sub}}},
            {"row in", {[id], in, {subquery, Sub}}},
            {"under not", {'not', {name, '>', {subquery, Sub}}}}
        ]
    ].

subquery_value_is_not_a_cache_hit_test_() ->
    Sub = correlated({ssn, ?PLAIN}),
    {setup, fun compiler_setup/0, fun compiler_teardown/1, fun() ->
        _ = kura_query_compiler:to_sql_cached(?REPO, where_enc({id, '=', 5})),
        Raw = #kura_query{from = ?ENC, wheres = [{id, '=', {subquery, Sub}}]},
        ?assertError(?WHERE_SSN, kura_query_compiler:to_sql_cached(?REPO, Raw))
    end}.

%%----------------------------------------------------------------------
%% Ordering, windowing and aggregating by an encrypted field.
%%----------------------------------------------------------------------

-define(ORDER_SSN, {kura, {encrypted_field_in_order_by, ssn}}).
-define(WINDOW_SSN, {kura, {encrypted_field_in_window, ssn}}).
-define(AGG_SSN, {kura, {encrypted_field_in_aggregate, ssn}}).

ordering_and_aggregates_refused_test_() ->
    Enc = kura_query:from(?ENC),
    Plain = kura_query:from(kura_test_schema),
    Joined = kura_query:join(Plain, inner, ?ENC, {id, id}),
    [
        {"order_by", ?_assertError(?ORDER_SSN, kura_query:order_by(Enc, [{ssn, asc}]))},
        {"order_by, second key",
            ?_assertError(?ORDER_SSN, kura_query:order_by(Enc, [{name, asc}, {ssn, desc}]))},
        {"order_by a joined schema",
            ?_assertError(?ORDER_SSN, kura_query:order_by(Joined, [{ssn, asc}]))},
        {"order_by, then join",
            ?_assertError(
                ?ORDER_SSN,
                kura_query:join(kura_query:order_by(Plain, [{ssn, asc}]), inner, ?ENC, {id, id})
            )},
        {"order_by over a CTE",
            ?_assertError(
                ?ORDER_SSN, kura_query:order_by(enc_cte(kura_query:from(enc_cte)), [{ssn, asc}])
            )}
    ] ++
        [
            {atom_to_list(Agg), ?_assertError(?AGG_SSN, kura_query:Agg(Enc, ssn))}
         || Agg <- [sum, avg, min, max]
        ] ++
        [
            {"aggregate in select",
                ?_assertError(?AGG_SSN, kura_query:select(Enc, [id, {max, ssn}]))},
            {"window aggregate",
                ?_assertError(
                    ?AGG_SSN,
                    kura_query:select_expr(Enc, [{m, kura_query:over({min, ssn}, #{})}])
                )},
            {"window partition_by",
                ?_assertError(
                    ?WINDOW_SSN,
                    kura_query:select_expr(Enc, [
                        {rn, kura_query:over(row_number, #{partition_by => [ssn]})}
                    ])
                )},
            {"window order_by",
                ?_assertError(
                    ?WINDOW_SSN,
                    kura_query:select_expr(Enc, [
                        {rn, kura_query:over(rank, #{order_by => [{ssn, desc}]})}
                    ])
                )}
        ].

ordering_and_aggregates_allowed_test_() ->
    Enc = kura_query:from(?ENC),
    [
        ?_assertMatch(#kura_query{}, kura_query:order_by(Enc, [{name, asc}])),
        ?_assertMatch(#kura_query{}, kura_query:count(Enc, ssn)),
        ?_assertMatch(#kura_query{}, kura_query:select(Enc, [id, ssn, {count, ssn}])),
        ?_assertMatch(#kura_query{}, kura_query:max(Enc, id)),
        ?_assertMatch(
            #kura_query{},
            kura_query:select_expr(Enc, [
                {s, ssn},
                {rn,
                    kura_query:over(row_number, #{partition_by => [name], order_by => [{id, asc}]})}
            ])
        )
    ].

ordering_and_aggregates_compiled_test_() ->
    Raw = [
        {?ORDER_SSN, #kura_query{from = ?ENC, order_bys = [{ssn, asc}]}},
        {?AGG_SSN, #kura_query{from = ?ENC, select = [{avg, ssn}]}},
        {?WINDOW_SSN, #kura_query{
            from = ?ENC,
            select = {exprs, [{rn, {over, row_number, #{partition_by => [ssn]}}}]}
        }},
        {?ORDER_SSN, #kura_query{
            from = kura_test_schema,
            wheres = [{exists, {subquery, #kura_query{from = ?ENC, order_bys = [{ssn, asc}]}}}]
        }}
    ],
    {setup, fun compiler_setup/0, fun compiler_teardown/1, [
        {Path, ?_assertError(Reason, Compile(Q))}
     || {Reason, Q} <- Raw, {Path, Compile} <- compile_paths()
    ]}.

%% update and delete add the attribute tenant to the key clauses directly,
%% not through where/2, and must refuse an encrypted tenant field the same way.
tenant_attribute_on_an_encrypted_field_in_writes_test_() ->
    Data = #{id => 1, name => ~"a", ssn => ~"x"},
    Tenant = {attribute, {ssn, ?PLAIN}},
    {setup, fun compiler_setup/0, fun compiler_teardown/1, [
        {"update",
            ?_assertError(
                ?WHERE_SSN,
                kura_tenant:with_tenant(Tenant, fun() ->
                    kura_repo_worker:update(
                        ?REPO, kura_changeset:cast(?ENC, Data, #{name => ~"b"}, [name])
                    )
                end)
            )},
        {"delete",
            ?_assertError(
                ?WHERE_SSN,
                kura_tenant:with_tenant(Tenant, fun() ->
                    kura_repo_worker:delete(?REPO, kura_changeset:cast(?ENC, Data, #{}, []))
                end)
            )}
    ]}.

%% An upsert's conflict guard compares the stored tenant ciphertext with the
%% new row's, which never match, so the upsert would silently never update.
tenant_attribute_on_an_encrypted_field_in_upserts_test_() ->
    Tenant = {attribute, {ssn, ?PLAIN}},
    Upsert = #{on_conflict => {[id], replace_all}},
    Row = #{id => 1, name => ~"a"},
    {setup,
        fun() ->
            compiler_setup(),
            application:set_env(kura, encryption, #{
                active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
            })
        end,
        fun(X) ->
            application:unset_env(kura, encryption),
            compiler_teardown(X)
        end,
        [
            {"insert",
                ?_assertError(
                    ?WHERE_SSN,
                    kura_tenant:with_tenant(Tenant, fun() ->
                        kura_repo_worker:insert(
                            ?REPO, kura_changeset:cast(?ENC, #{}, Row, [id, name]), Upsert
                        )
                    end)
                )},
            {"insert_all",
                ?_assertError(
                    ?WHERE_SSN,
                    kura_tenant:with_tenant(Tenant, fun() ->
                        kura_repo_worker:insert_all(?REPO, ?ENC, [Row], Upsert)
                    end)
                )}
        ]}.

%%----------------------------------------------------------------------
%% A CTE whose body is a union exports the ciphertext columns of every
%% branch, under the first query's column names.
%%----------------------------------------------------------------------

union_cte(Body) ->
    kura_query:with_cte(#kura_query{from = ~"u"}, ~"u", Body).

union_cte_test_() ->
    P = ?PLAIN,
    Enc = kura_query:from(?ENC),
    PlainSsn = kura_query:from(kura_test_plain_ssn_schema),
    Users = kura_query:from(kura_test_schema),
    Refused = [
        {"SELECT * union of different schemas", union_cte(kura_query:union(PlainSsn, Enc)),
            {ssn, P}},
        {"... fails closed on every column", union_cte(kura_query:union(PlainSsn, Enc)), {id, 1}},
        {"union_all", union_cte(kura_query:union_all(PlainSsn, Enc)), {ssn, P}},
        {"except", union_cte(kura_query:except(PlainSsn, Enc)), {ssn, P}},
        {"intersect", union_cte(kura_query:intersect(PlainSsn, Enc)), {ssn, P}},
        {"same name, by position",
            union_cte(
                kura_query:union(
                    kura_query:select(PlainSsn, [id, ssn]), kura_query:select(Enc, [id, ssn])
                )
            ),
            {ssn, P}},
        {"another name, by position",
            union_cte(
                kura_query:union(
                    kura_query:select(Users, [id, email]), kura_query:select(Enc, [id, ssn])
                )
            ),
            {email, P}},
        {"aliased in the branch",
            union_cte(
                kura_query:union(
                    kura_query:select(Users, [id, email]),
                    kura_query:select_expr(Enc, [{id, id}, {e, ssn}])
                )
            ),
            {email, P}},
        {"a union nested in a branch",
            union_cte(
                kura_query:union(
                    kura_query:select(Users, [id, email]),
                    kura_query:union(
                        kura_query:select(Users, [id, email]), kura_query:select(Enc, [id, ssn])
                    )
                )
            ),
            {email, P}},
        {"the same schema twice", union_cte(kura_query:union(Enc, Enc)), {ssn, P}},
        {"main SELECT * against a named branch",
            union_cte(kura_query:union(Users, kura_query:select(Enc, [id, ssn]))), {name, P}}
    ],
    Allowed = [
        {"another position",
            union_cte(
                kura_query:union(
                    kura_query:select(Users, [id, email]), kura_query:select(Enc, [id, ssn])
                )
            ),
            {id, 1}},
        {"a plain column of the same schema twice", union_cte(kura_query:union(Enc, Enc)),
            {name, P}},
        {"plain branches", union_cte(kura_query:union(Users, Users)), {email, P}}
    ],
    [
        {Name, fun() ->
            assert_refused({kura, {encrypted_field_in_where, element(1, Cond)}}, Q, Cond)
        end}
     || {Name, Q, Cond} <- Refused
    ] ++
        [
            {Name, ?_assertMatch(#kura_query{}, kura_query:where(Q, Cond))}
         || {Name, Q, Cond} <- Allowed
        ].

assert_refused(Reason, Q, Cond) ->
    ?assertError(Reason, kura_query:where(Q, Cond)).
