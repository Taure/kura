-module(kura_preloader_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% get_field
%%----------------------------------------------------------------------

get_field_test() ->
    ?assertEqual(42, kura_preloader:get_field(age, #{name => <<"Alice">>, age => 42})).

%%----------------------------------------------------------------------
%% get_field_default
%%----------------------------------------------------------------------

get_field_default_present_test() ->
    ?assertEqual(42, kura_preloader:get_field_default(age, #{age => 42}, 0)).

get_field_default_missing_test() ->
    ?assertEqual(0, kura_preloader:get_field_default(age, #{}, 0)).

get_field_default_nil_test() ->
    ?assertEqual(nil, kura_preloader:get_field_default(missing, #{}, nil)).

%%----------------------------------------------------------------------
%% set_field
%%----------------------------------------------------------------------

set_field_new_test() ->
    ?assertEqual(#{a => 1}, kura_preloader:set_field(a, 1, #{})).

set_field_overwrite_test() ->
    ?assertEqual(#{a => 2}, kura_preloader:set_field(a, 2, #{a => 1})).

%%----------------------------------------------------------------------
%% group_by_tuple
%%----------------------------------------------------------------------

group_by_tuple_empty_test() ->
    ?assertEqual(#{}, kura_preloader:group_by_tuple([], [fk], #{})).

group_by_tuple_single_test() ->
    Records = [#{fk => 1, name => <<"a">>}],
    Result = kura_preloader:group_by_tuple(Records, [fk], #{}),
    ?assertEqual(#{{1} => [#{fk => 1, name => <<"a">>}]}, Result).

group_by_tuple_multiple_same_key_test() ->
    Records = [
        #{fk => 1, name => <<"a">>},
        #{fk => 1, name => <<"b">>},
        #{fk => 2, name => <<"c">>}
    ],
    Result = kura_preloader:group_by_tuple(Records, [fk], #{}),
    ?assertEqual(2, length(maps:get({1}, Result))),
    ?assertEqual(1, length(maps:get({2}, Result))).

group_by_tuple_composite_key_test() ->
    Records = [
        #{a => 1, b => 2, n => ~"x"},
        #{a => 1, b => 2, n => ~"y"},
        #{a => 1, b => 3, n => ~"z"}
    ],
    Result = kura_preloader:group_by_tuple(Records, [a, b], #{}),
    ?assertEqual(2, length(maps:get({1, 2}, Result))),
    ?assertEqual(1, length(maps:get({1, 3}, Result))).

%%----------------------------------------------------------------------
%% group_m2m_join
%%----------------------------------------------------------------------

group_m2m_join_empty_test() ->
    ?assertEqual(#{}, kura_preloader:group_m2m_join([], [owner_id], [related_id], #{}, #{})).

group_m2m_join_with_lookup_test() ->
    JoinRows = [
        #{owner_id => 1, related_id => 10},
        #{owner_id => 1, related_id => 20},
        #{owner_id => 2, related_id => 10}
    ],
    RelLookup = #{
        {10} => #{id => 10, name => <<"tag1">>},
        {20} => #{id => 20, name => <<"tag2">>}
    },
    Result = kura_preloader:group_m2m_join(JoinRows, [owner_id], [related_id], RelLookup, #{}),
    ?assertEqual(2, length(maps:get({1}, Result))),
    ?assertEqual(1, length(maps:get({2}, Result))).

group_m2m_join_missing_in_lookup_test() ->
    JoinRows = [#{owner_id => 1, related_id => 99}],
    RelLookup = #{{10} => #{id => 10, name => <<"tag1">>}},
    Result = kura_preloader:group_m2m_join(JoinRows, [owner_id], [related_id], RelLookup, #{}),
    ?assertEqual(#{}, Result).

%%----------------------------------------------------------------------
%% reverse_maps
%%----------------------------------------------------------------------

reverse_maps_empty_test() ->
    ?assertEqual([], kura_preloader:reverse_maps([], [])).

reverse_maps_single_test() ->
    ?assertEqual([#{a => 1}], kura_preloader:reverse_maps([#{a => 1}], [])).

reverse_maps_multiple_test() ->
    ?assertEqual(
        [#{a => 3}, #{a => 2}, #{a => 1}],
        kura_preloader:reverse_maps([#{a => 1}, #{a => 2}, #{a => 3}], [])
    ).

reverse_maps_with_acc_test() ->
    ?assertEqual(
        [#{a => 2}, #{a => 1}, #{a => 0}],
        kura_preloader:reverse_maps([#{a => 1}, #{a => 2}], [#{a => 0}])
    ).

%%----------------------------------------------------------------------
%% join_rows_sql: the many_to_many join-table read, through the dialect
%%----------------------------------------------------------------------

join_rows_sql_reads_a_single_key_test() ->
    ?assertEqual(
        {~"SELECT \"post_id\", \"tag_id\" FROM \"posts_tags\" WHERE \"post_id\" IN ($1, $2)", [1, 2]},
        join_rows(kura_dialect_pg, ~"posts_tags", {[post_id], [tag_id]}, [{1}, {2}])
    ).

join_rows_sql_reads_a_composite_key_as_row_values_test() ->
    ?assertEqual(
        {
            ~"SELECT \"org_id\", \"post_id\", \"tag_id\" FROM \"posts_tags\" WHERE (\"org_id\", \"post_id\") IN (($1, $2), ($3, $4))",
            [o, 1, o, 2]
        },
        join_rows(kura_dialect_pg, ~"posts_tags", {[org_id, post_id], [tag_id]}, [{o, 1}, {o, 2}])
    ).

join_rows_sql_uses_the_dialect_placeholders_test() ->
    ?assertEqual(
        {
            ~"SELECT \"org_id\", \"post_id\", \"tag_id\" FROM \"posts_tags\" WHERE (\"org_id\", \"post_id\") IN ((?1, ?2), (?3, ?4))",
            [o, 1, o, 2]
        },
        join_rows(
            kura_test_sqlite_dialect, ~"posts_tags", {[org_id, post_id], [tag_id]}, [{o, 1}, {o, 2}]
        )
    ).

join_rows_sql_quotes_the_join_table_test() ->
    {SQL, _} = join_rows(kura_dialect_pg, ~"posts\" -- ", {[post_id], [tag_id]}, [{1}]),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"FROM \"posts\"\" -- \" WHERE")).

join_rows_sql_names_a_join_schema_by_its_table_test() ->
    {SQL, _} = join_rows(kura_dialect_pg, kura_test_tag_schema, {[post_id], [tag_id]}, [{1}]),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"FROM \"tags\" WHERE")).

join_rows_sql_is_bare_without_a_prefix_tenant_test() ->
    Expected = join_rows(kura_dialect_pg, ~"posts_tags", {[post_id], [tag_id]}, [{1}]),
    ?assertEqual(
        Expected,
        kura_tenant:with_tenant({attribute, {org_id, 1}}, fun() ->
            join_rows(kura_dialect_pg, ~"posts_tags", {[post_id], [tag_id]}, [{1}])
        end)
    ).

join_rows_sql_qualifies_with_the_prefix_tenant_test() ->
    {SQL, _} = kura_tenant:with_tenant({prefix, ~"tenant_a"}, fun() ->
        join_rows(kura_dialect_pg, ~"posts_tags", {[post_id], [tag_id]}, [{1}])
    end),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"FROM \"tenant_a\".\"posts_tags\" WHERE")).

join_rows(Dialect, JoinThrough, Keys, OwnerTuples) ->
    {SQL, Params} = kura_preloader:join_rows_sql(Dialect, JoinThrough, Keys, OwnerTuples),
    {iolist_to_binary(SQL), Params}.

%%----------------------------------------------------------------------
%% quote_table_bin/1, deprecated
%%----------------------------------------------------------------------

quote_table_bin_doubles_embedded_quotes_test() ->
    ?assertEqual(~"\"posts\"\" -- \"", kura_preloader:quote_table_bin(~"posts\" -- ")).

quote_table_bin_is_bare_without_a_prefix_tenant_test() ->
    ?assertEqual(~"\"posts_tags\"", kura_preloader:quote_table_bin(~"posts_tags")),
    ?assertEqual(
        ~"\"posts_tags\"",
        kura_tenant:with_tenant({attribute, {org_id, 1}}, fun() ->
            kura_preloader:quote_table_bin(~"posts_tags")
        end)
    ).

quote_table_bin_qualifies_with_the_prefix_tenant_test() ->
    ?assertEqual(
        ~"\"tenant_a\".\"posts_tags\"",
        kura_tenant:with_tenant({prefix, ~"tenant_a"}, fun() ->
            kura_preloader:quote_table_bin(~"posts_tags")
        end)
    ).
