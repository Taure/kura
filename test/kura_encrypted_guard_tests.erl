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
                {kura, {encrypted_field_in_where, max}},
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
