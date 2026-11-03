-module(kura_encryption_lookup_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(DET, kura_test_det_schema).
-define(INVITE, kura_test_det_invite_schema).
-define(REPO, det_lookup_test_repo).
-define(P1, ~"plaintext-one-77").
-define(P2, ~"plaintext-two-88").

setup() ->
    forget(),
    application:set_env(kura, encryption, #{
        active => 1,
        deterministic_active => 1,
        deterministic_keys => [2],
        keys => [
            {1, base64:encode(crypto:strong_rand_bytes(32))},
            {2, base64:encode(crypto:strong_rand_bytes(32))}
        ]
    }),
    application:set_env(kura, repos, #{?REPO => #{dialect => kura_dialect_pg}}),
    kura_query_cache:init(),
    kura_query_cache:flush().

teardown(_) ->
    kura_query_cache:flush(),
    application:unset_env(kura, repos),
    application:unset_env(kura, encryption),
    forget().

forget() ->
    _ = [
        kura_schema:forget(M)
     || {M, _} <- code:all_loaded(), lists:prefix("kura_test_", atom_to_list(M))
    ],
    ok.

lookup_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [
            {Name, fun() -> walk(Field, Kind, Cond, Expect) end}
         || {Name, Field, Kind, Cond, Expect} <- table()
        ] ++
            [
                {"the lookup holds the ciphertexts", fun t_ciphertexts/0},
                {"the dialect compiles IN and NOT IN", fun t_sql/0},
                {"having looks up too", fun t_having/0},
                {"normalised and canonical values look up equally", fun t_canonical_lookup/0},
                {"an invalid value raises", fun t_invalid_value/0},
                {"no deterministic key raises", fun t_no_key/0},
                {"order, group, distinct, aggregates and windows raise", fun t_other_clauses/0},
                {"a field of two sources raises", fun t_ambiguous_join/0},
                {"a join after the lookup raises", fun t_join_after_lookup/0},
                {"a field of a joined schema is looked up", fun t_joined_field/0},
                {"a source read through a CTE raises", fun t_cte/0},
                {"a subquery looks up in its own scope", fun t_subquery/0},
                {"an outer deterministic field in a subquery raises", fun t_outer_field/0},
                {"the guard refuses a hand-built lookup", fun t_hand_built/0},
                {"the shape replaces frames only", fun t_shape/0},
                {"the cache cannot pass a hand-built plaintext lookup", fun t_cache_soundness/0},
                {"cached lookups share one entry", fun t_cache_shared/0},
                {"an attribute tenant on a deterministic field raises", fun t_tenant/0},
                {"the tenant scope checks distance thresholds", fun t_scope_thresholds/0},
                {"a raw table is not looked up", fun t_raw_table/0},
                {"a raw from joined to a deterministic schema looks up",
                    fun t_raw_from_joined_field/0},
                {"nil on a string field raises", fun t_nil_string/0},
                {"an empty in compiles as on a plain field", fun t_empty_in/0},
                {"a malformed keyring config raises without key material", fun t_malformed_config/0}
            ]}.

%% {Name, Field, Kind, Condition, Expect}: Expect is rewritten (the
%% plaintext is replaced by ciphertexts), unchanged, or {raises, Reason}.
table() ->
    Fields = [{name, plain}, {note, randomized}, {secret, randomized}, {ssn, deterministic}],
    Sub = kura_query:from(?DET),
    [
        {
            lists:flatten(io_lib:format("~s ~p ~s", [Form, F, Kind])),
            F,
            Kind,
            Cond,
            expect(Class, Kind, F)
        }
     || {F, Kind} <- Fields,
        {Form, Class, Cond} <-
            [
                {"bare", lookup, {F, ?P1}},
                {"=", lookup, {F, '=', ?P1}},
                {"!=", lookup, {F, '!=', ?P1}},
                {"in", lookup, {F, in, [?P1, ?P2]}},
                {"not_in", lookup, {F, not_in, [?P1]}},
                {"empty in", lookup, {F, in, []}},
                {"not", lookup, {'not', {F, ?P1}}},
                {"and", lookup, {'and', [{id, 1}, {F, ?P1}]}},
                {"or", lookup, {'or', [{id, 1}, {'not', {F, '!=', ?P1}}]}},
                {"is_nil", unary, {F, is_nil}},
                {"is_not_nil", unary, {F, is_not_nil}},
                {"fragment", unary, {fragment, ~"1 = ?", [1]}},
                {"between", refused, {F, between, {?P1, ?P2}}},
                {"single row in", refused, {[F], in, [{?P1}]}},
                {"row in", refused, {[id, F], in, [{1, ?P1}]}},
                {"in subquery", refused, {F, in, {subquery, Sub}}},
                {"= subquery", subquery_value, {F, '=', {subquery, Sub}}}
            ] ++
                [
                    {atom_to_list(Op), refused, {F, Op, ?P1}}
                 || Op <- ['<', '>', '<=', '>=', like, ilike, matches]
                ] ++
                [
                    {"reserved " ++ atom_to_list(Op), reserved, {F, Op, [?P1]}}
                 || Op <- [encrypted_in, encrypted_not_in]
                ]
    ].

expect(reserved, _Kind, _F) -> {raises, {kura, {reserved_operator, reserved}}};
expect(unary, _Kind, _F) -> unchanged;
expect(subquery_value, plain, _F) -> {raises, {kura, unrecognised_condition}};
expect(_Class, plain, _F) -> unchanged;
expect(lookup, deterministic, _F) -> rewritten;
expect(_Class, _Kind, F) -> {raises, {kura, {encrypted_field_in_where, F}}}.

walk(_Field, _Kind, Cond, {raises, {kura, {reserved_operator, reserved}}}) ->
    {_, Op, _} = Cond,
    ?assertError({kura, {reserved_operator, Op}}, kura_query:where(kura_query:from(?DET), Cond));
walk(_Field, _Kind, Cond, {raises, Reason}) ->
    ?assertError(Reason, kura_query:where(kura_query:from(?DET), Cond));
walk(_Field, _Kind, Cond, unchanged) ->
    Q = kura_query:where(kura_query:from(?DET), Cond),
    ?assertEqual([Cond], Q#kura_query.wheres),
    ?assertEqual(ok, kura_query:check_encrypted(Q));
walk(_Field, _Kind, Cond, rewritten) ->
    Q = kura_query:where(kura_query:from(?DET), Cond),
    ?assertNotEqual([Cond], Q#kura_query.wheres),
    ?assertEqual(ok, kura_query:check_encrypted(Q)),
    {SQL, Params} = kura_query_compiler:to_sql(?REPO, Q),
    {CachedSQL, CachedParams} = kura_query_compiler:to_sql_cached(?REPO, Q),
    ?assertEqual(iolist_to_binary(SQL), iolist_to_binary(CachedSQL)),
    ?assertEqual(Params, CachedParams),
    Shape = element(1, kura_query_shape:split(Q)),
    assert_absent([Q, Params, SQL, Shape, ets:tab2list(kura_query_cache)]),
    ?assert(lists:all(fun kura_crypto:is_deterministic_frame/1, [P || P <- Params, P =/= 1])).

assert_absent(Terms) ->
    Bin = term_to_binary(Terms),
    [?assertEqual(nomatch, binary:match(Bin, P)) || P <- [?P1, ?P2]].

frames(Field, Value) ->
    kura_encryption:lookup_params(?DET, Field, Value).

t_ciphertexts() ->
    Q = kura_query:where(kura_query:from(?DET), {ssn, ?P1}),
    ?assertEqual([{ssn, encrypted_in, frames(ssn, ?P1)}], Q#kura_query.wheres),
    ?assertEqual(2, length(frames(ssn, ?P1))),
    Q2 = kura_query:where(kura_query:from(?DET), {ssn, not_in, [?P1, ?P2]}),
    ?assertEqual(
        [{ssn, encrypted_not_in, interleave(frames(ssn, ?P1), frames(ssn, ?P2))}],
        Q2#kura_query.wheres
    ),
    {ok, Stored} = kura_types:dump(
        {encrypted, string, #{mode => deterministic}}, ?P1, {~"det_users", ~"ssn"}
    ),
    ?assertEqual(Stored, hd(frames(ssn, ?P1))).

%% Frames come key by key: every value under the first key, then the second.
interleave([A1, A2], [B1, B2]) -> [A1, B1, A2, B2].

t_empty_in() ->
    [
        begin
            Q = kura_query:where(kura_query:from(?DET), {ssn, Op, []}),
            ?assertEqual([{ssn, Lookup, []}], Q#kura_query.wheres),
            ?assertEqual(ok, kura_query:check_encrypted(Q)),
            {SQL, Params} = kura_query_compiler:to_sql(?REPO, Q),
            {PlainSQL, PlainParams} = kura_query_compiler:to_sql(
                ?REPO, kura_query:where(kura_query:from(?DET), {name, Op, []})
            ),
            ?assertEqual(
                binary:replace(iolist_to_binary(PlainSQL), ~"\"name\"", ~"\"ssn\""),
                iolist_to_binary(SQL)
            ),
            ?assertEqual({[], []}, {PlainParams, Params}),
            ?assertMatch({_, _}, binary:match(iolist_to_binary(SQL), Text))
        end
     || {Op, Lookup, Text} <- [
            {in, encrypted_in, ~"\"ssn\" IN ()"}, {not_in, encrypted_not_in, ~"\"ssn\" NOT IN ()"}
        ]
    ].

t_malformed_config() ->
    B64 = ~"S2V5T25lLi4uMzIgcmF3IGJ5dGVzLi4uYmFzZTY0ISE=",
    Raw = base64:decode(B64),
    Writes = [
        fun() -> kura_query:where(kura_query:from(?DET), {ssn, ?P1}) end,
        fun() ->
            kura_types:dump({encrypted, string, #{mode => deterministic}}, ?P1, {~"t", ~"c"})
        end,
        fun() -> kura_types:dump({encrypted, string, #{}}, ?P1, {~"t", ~"c"}) end,
        fun() -> kura_types:dump({encrypted, string}, ?P1) end
    ],
    Configs = [
        [{active, 1}, {deterministic_active, 1}, {keys, [{1, B64}]}],
        #{active => 1, deterministic_active => 1, keys => #{1 => B64}},
        #{active => 1, deterministic_active => 1, keys => improper({2, B64}, B64)}
    ],
    [
        begin
            application:set_env(kura, encryption, Config),
            try Write() of
                Result -> ?assertEqual(raised, Result)
            catch
                Class:Reason:Stack ->
                    ?assertEqual({kura_crypto, invalid_keyring_config}, Reason),
                    Bin = term_to_binary({Class, Reason, Stack}),
                    ?assertEqual(nomatch, binary:match(Bin, B64)),
                    ?assertEqual(nomatch, binary:match(Bin, Raw))
            end
        end
     || Config <- Configs, Write <- Writes
    ].

t_sql() ->
    {SQL, Params} = kura_query_compiler:to_sql(
        ?REPO,
        kura_query:where(
            kura_query:from(?DET), {'and', [{ssn, ?P1}, {email, '!=', ?P2}, {name, ~"n"}]}
        )
    ),
    ?assertMatch(
        {_, _},
        binary:match(
            iolist_to_binary(SQL),
            ~"(\"ssn\" IN ($1, $2) AND \"email\" NOT IN ($3, $4) AND \"name\" = $5)"
        )
    ),
    ?assertEqual(frames(ssn, ?P1) ++ frames(email, ?P2) ++ [~"n"], Params).

t_having() ->
    Q = kura_query:having(kura_query:from(?DET), {ssn, ?P1}),
    ?assertEqual([{ssn, encrypted_in, frames(ssn, ?P1)}], Q#kura_query.havings),
    ?assertError(
        {kura, {encrypted_field_in_having, note}},
        kura_query:having(kura_query:from(?DET), {note, ?P1})
    ),
    ?assertError(
        {kura, {encrypted_field_in_having, ssn}},
        kura_query:having(kura_query:from(?DET), {ssn, '>', ?P1})
    ).

t_canonical_lookup() ->
    ?assertEqual(frames(email, ~"ada@example.com"), frames(email, ~"ADA@Example.COM")),
    Lower = ~"0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
    ?assertEqual(frames(token, Lower), frames(token, string:uppercase(Lower))),
    ?assertEqual(frames(pin, 42), frames(pin, ~"42")),
    Q = kura_query:where(kura_query:from(?DET), {email, "ADA@example.com"}),
    ?assertEqual([{email, encrypted_in, frames(email, ~"ada@example.com")}], Q#kura_query.wheres).

t_invalid_value() ->
    [
        ?assertError(
            {kura, {invalid_lookup_value, pin}}, kura_query:where(kura_query:from(?DET), C)
        )
     || C <- [
            {pin, ~"not a number"},
            {pin, nil},
            {pin, undefined},
            {pin, in, [1, ~"x"]},
            {pin, '=', null}
        ]
    ],
    ?assertError(
        {kura, unrecognised_condition},
        kura_query:where(kura_query:from(?DET), {pin, in, improper(1, 2)})
    ),
    #{pin := Spec} = kura_schema:encryption_specs(?DET),
    ?assertError({kura, unrecognised_condition}, kura_encryption:lookup(pin, Spec, improper(1, 2))).

t_no_key() ->
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    ?assertError(
        {kura_crypto, no_deterministic_key}, kura_query:where(kura_query:from(?DET), {ssn, ?P1})
    ).

t_other_clauses() ->
    Q = kura_query:from(?DET),
    ?assertError({kura, {encrypted_field_in_order_by, ssn}}, kura_query:order_by(Q, [{ssn, asc}])),
    ?assertError({kura, {encrypted_field_in_group_by, ssn}}, kura_query:group_by(Q, [ssn])),
    ?assertError({kura, {encrypted_field_in_distinct, ssn}}, kura_query:distinct(Q, [ssn])),
    ?assertError({kura, {encrypted_field_in_aggregate, ssn}}, kura_query:max(Q, ssn)),
    ?assertError(
        {kura, {encrypted_field_in_window, ssn}},
        kura_query:select_expr(Q, [{r, kura_query:over(row_number, #{partition_by => [ssn]})}])
    ),
    ?assertError(
        {kura, {encrypted_field_in_join, email}},
        kura_query:join(Q, inner, ?INVITE, {email, email})
    ).

t_ambiguous_join() ->
    Q = kura_query:join(kura_query:from(?DET), inner, ?INVITE, {id, user_id}),
    ?assertError({kura, {encrypted_field_in_where, email}}, kura_query:where(Q, {email, ?P1})),
    ?assertMatch(#kura_query{}, kura_query:where(Q, {ssn, ?P1})).

t_join_after_lookup() ->
    Q = kura_query:where(kura_query:from(?DET), {email, ?P1}),
    ?assertError(
        {kura, {invalid_encrypted_lookup, email}},
        kura_query:join(Q, inner, ?INVITE, {id, user_id})
    ).

t_joined_field() ->
    Q = kura_query:join(kura_query:from(?DET), inner, ?INVITE, {id, user_id}),
    Q1 = kura_query:where(Q, {code, ?P1}),
    Frames = kura_encryption:lookup_params(?INVITE, code, ?P1),
    ?assertEqual([{code, encrypted_in, Frames}], Q1#kura_query.wheres),
    {ok, Stored} = kura_types:dump(
        {encrypted, string, #{mode => deterministic}}, ?P1, {~"shared", ~"code"}
    ),
    ?assertEqual(Stored, hd(Frames)),
    ?assertEqual(ok, kura_query:check_encrypted(Q1)).

t_cte() ->
    Cte = kura_query:from(?DET),
    Q = kura_query:with_cte(kura_query:from(?DET), ~"det_users", Cte),
    ?assertError({kura, {encrypted_field_in_where, ssn}}, kura_query:where(Q, {ssn, ?P1})),
    Q1 = kura_query:where(kura_query:from(?DET), {ssn, ?P1}),
    ?assertError(
        {kura, {invalid_encrypted_lookup, ssn}}, kura_query:with_cte(Q1, ~"det_users", Cte)
    ),
    Q2 = kura_query:with_cte(Q1, ~"other", Cte),
    ?assertEqual(ok, kura_query:check_encrypted(Q2)),
    Plain = kura_query:select(kura_query:from(kura_test_schema), [id]),
    Q3 = kura_query:with_cte(kura_query:from(?DET), ~"det_users", Plain),
    ?assertError({kura, {encrypted_field_in_where, ssn}}, kura_query:where(Q3, {ssn, ?P1})).

t_subquery() ->
    Sub = kura_query:select(kura_query:where(kura_query:from(?DET), {ssn, ?P1}), [id]),
    Q = kura_query:where(kura_query:from(kura_test_schema), {id, in, {subquery, Sub}}),
    ?assertEqual(ok, kura_query:check_encrypted(Q)),
    {_SQL, Params} = kura_query_compiler:to_sql_cached(?REPO, Q),
    ?assertEqual(frames(ssn, ?P1), Params),
    assert_absent([Q, Params]).

t_outer_field() ->
    Sub = kura_query:where(kura_query:from(kura_test_tag_schema), {ssn, ?P1}),
    ?assertError(
        {kura, {encrypted_field_in_where, ssn}},
        kura_query:where(kura_query:from(?DET), {exists, {subquery, Sub}})
    ),
    Inner = kura_query:where(kura_query:from(?DET), {ssn, ?P1}),
    Hand = Sub#kura_query{wheres = Inner#kura_query.wheres},
    ?assertError(
        {kura, {invalid_encrypted_lookup, ssn}},
        kura_query:where(kura_query:from(?DET), {exists, {subquery, Hand}})
    ).

t_hand_built() ->
    Frames = frames(ssn, ?P1),
    Ok = #kura_query{from = ?DET, wheres = [{ssn, encrypted_in, Frames}]},
    ?assertEqual(ok, kura_query:check_encrypted(Ok)),
    ?assertEqual(
        ok, kura_query:check_encrypted(Ok#kura_query{havings = [{ssn, encrypted_not_in, []}]})
    ),
    Refused = [
        {{ssn, encrypted_in, [?P1]}, ssn},
        {{ssn, encrypted_not_in, [hd(Frames), 42]}, ssn},
        {{ssn, encrypted_in, hd(Frames)}, ssn},
        {{ssn, encrypted_in, improper(hd(Frames), ?P1)}, ssn},
        {{note, encrypted_in, Frames}, note},
        {{secret, encrypted_in, Frames}, secret},
        {{name, encrypted_in, Frames}, name},
        {{missing, encrypted_in, Frames}, missing},
        {{'not', {email, encrypted_in, [?P1]}}, email},
        {{[ssn], encrypted_in, Frames}, not_a_column}
    ],
    [
        ?assertError(
            {kura, {invalid_encrypted_lookup, F}},
            kura_query:check_encrypted(#kura_query{from = ?DET, wheres = [C]})
        )
     || {C, F} <- Refused
    ],
    ?assertError(
        {kura, {invalid_encrypted_lookup, ssn}},
        kura_query:check_encrypted(#kura_query{
            from = ~"det_users", wheres = [{ssn, encrypted_in, Frames}]
        })
    ),
    ?assertError(
        {kura, {invalid_encrypted_lookup, ssn}},
        kura_query_compiler:to_sql_cached(?REPO, #kura_query{
            from = ?DET, wheres = [{ssn, encrypted_in, [?P1]}]
        })
    ).

t_shape() ->
    Frames = frames(ssn, ?P1),
    {Shape, Values} = kura_query_shape:split(#kura_query{
        from = ?DET, wheres = [{ssn, encrypted_in, Frames}]
    }),
    ?assertEqual(
        [{ssn, encrypted_in, [{'$kura_param', 1}, {'$kura_param', 2}]}], Shape#kura_query.wheres
    ),
    ?assertEqual(list_to_tuple(Frames), Values),
    Plain = #kura_query{from = ?DET, wheres = [{ssn, encrypted_in, [hd(Frames), ?P1]}]},
    {PlainShape, {}} = kura_query_shape:split(Plain),
    ?assertEqual(Plain, PlainShape),
    {OneShape, {Frame}} = kura_query_shape:split(Plain#kura_query{
        wheres = [{ssn, encrypted_in, [hd(Frames)]}]
    }),
    ?assertEqual(hd(Frames), Frame),
    ?assertEqual([{ssn, encrypted_in, [{'$kura_param', 1}]}], OneShape#kura_query.wheres),
    Improper = Plain#kura_query{wheres = [{ssn, encrypted_in, improper(hd(Frames), hd(Frames))}]},
    ?assertEqual({Improper, {}}, kura_query_shape:split(Improper)).

%% A query that passed the guard puts its shape in the cache. A hand-built
%% lookup with plaintext in the same position must not hit that entry.
t_cache_soundness() ->
    [Frame | _] = frames(pin, 1),
    Good = #kura_query{from = ?DET, wheres = [{pin, encrypted_in, [Frame]}]},
    {_, [Frame]} = kura_query_compiler:to_sql_cached(?REPO, Good),
    Bad = Good#kura_query{wheres = [{pin, encrypted_in, [?P1]}]},
    ?assertError(
        {kura, {invalid_encrypted_lookup, pin}}, kura_query_compiler:to_sql_cached(?REPO, Bad)
    ),
    Forged = <<2, 1, 0:16, 0:96, 0:128, (~"forged-plaintext")/binary>>,
    ?assert(kura_crypto:is_deterministic_frame(Forged)),
    {_, [Forged]} = kura_query_compiler:to_sql_cached(
        ?REPO, Good#kura_query{wheres = [{pin, encrypted_in, [Forged]}]}
    ).

t_cache_shared() ->
    kura_query_cache:flush(),
    [
        kura_query_compiler:to_sql_cached(?REPO, kura_query:where(kura_query:from(?DET), {ssn, V}))
     || V <- [?P1, ?P2, ~"three"]
    ],
    ?assertEqual(1, kura_query_cache:entry_count()).

t_scope_thresholds() ->
    Q = kura_query:from(kura_test_vector_schema),
    Bad = {{distance, embedding, l2, ~"[1.0,2.0,3.0]"}, '<', ~"x"},
    ?assertError({kura, {invalid_distance_threshold, embedding}}, kura_query:where(Q, Bad)),
    ?assertError(
        {kura, {invalid_distance_threshold, embedding}}, kura_query:where_unrewritten(Q, Bad)
    ).

t_tenant() ->
    kura_tenant:put_tenant({attribute, {ssn, ?P1}}),
    try
        ?assertError(
            {kura, {encrypted_field_in_where, ssn}},
            kura_query_compiler:to_sql(?REPO, kura_query:from(?DET))
        )
    after
        kura_tenant:clear_tenant()
    end.

t_raw_from_joined_field() ->
    Q = kura_query:join(kura_query:from(ill_typed(~"users")), inner, ?DET, {id, id}),
    Q1 = kura_query:where(Q, {ssn, ?P1}),
    ?assertEqual([{ssn, encrypted_in, frames(ssn, ?P1)}], Q1#kura_query.wheres),
    ?assertEqual(ok, kura_query:check_encrypted(Q1)).

t_nil_string() ->
    Q = kura_query:from(?DET),
    [
        ?assertError({kura, {invalid_lookup_value, F}}, kura_query:where(Q, C))
     || {F, C} <- [
            {ssn, {ssn, nil}},
            {ssn, {ssn, '=', null}},
            {ssn, {ssn, '!=', nil}},
            {ssn, {ssn, undefined}},
            {email, {email, in, [~"a@example.com", nil]}},
            {email, {email, not_in, [null]}}
        ]
    ],
    ?assertError(
        {kura, {invalid_lookup_value, ssn}}, kura_encryption:lookup_params(?DET, ssn, nil)
    ).

t_raw_table() ->
    Q = kura_query:where(kura_query:from(ill_typed(~"det_users")), {ssn, ?P1}),
    ?assertEqual([{ssn, ?P1}], Q#kura_query.wheres).

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) ->
    Term.

-spec improper(term(), term()) -> dynamic().
improper(Head, Tail) ->
    [Head | ill_typed(Tail)].
