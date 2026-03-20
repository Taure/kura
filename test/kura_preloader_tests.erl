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
%% group_by_key
%%----------------------------------------------------------------------

group_by_key_empty_test() ->
    ?assertEqual(#{}, kura_preloader:group_by_key([], fk, #{})).

group_by_key_single_test() ->
    Records = [#{fk => 1, name => <<"a">>}],
    Result = kura_preloader:group_by_key(Records, fk, #{}),
    ?assertEqual(#{1 => [#{fk => 1, name => <<"a">>}]}, Result).

group_by_key_multiple_same_key_test() ->
    Records = [
        #{fk => 1, name => <<"a">>},
        #{fk => 1, name => <<"b">>},
        #{fk => 2, name => <<"c">>}
    ],
    Result = kura_preloader:group_by_key(Records, fk, #{}),
    ?assertEqual(2, length(maps:get(1, Result))),
    ?assertEqual(1, length(maps:get(2, Result))).

%%----------------------------------------------------------------------
%% group_m2m_join
%%----------------------------------------------------------------------

group_m2m_join_empty_test() ->
    ?assertEqual(#{}, kura_preloader:group_m2m_join([], owner_id, related_id, #{}, #{})).

group_m2m_join_with_lookup_test() ->
    JoinRows = [
        #{owner_id => 1, related_id => 10},
        #{owner_id => 1, related_id => 20},
        #{owner_id => 2, related_id => 10}
    ],
    RelLookup = #{
        10 => #{id => 10, name => <<"tag1">>},
        20 => #{id => 20, name => <<"tag2">>}
    },
    Result = kura_preloader:group_m2m_join(JoinRows, owner_id, related_id, RelLookup, #{}),
    ?assertEqual(2, length(maps:get(1, Result))),
    ?assertEqual(1, length(maps:get(2, Result))).

group_m2m_join_missing_in_lookup_test() ->
    JoinRows = [#{owner_id => 1, related_id => 99}],
    RelLookup = #{10 => #{id => 10, name => <<"tag1">>}},
    Result = kura_preloader:group_m2m_join(JoinRows, owner_id, related_id, RelLookup, #{}),
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
%% join_bins
%%----------------------------------------------------------------------

join_bins_empty_test() ->
    ?assertEqual(<<>>, kura_preloader:join_bins([], <<", ">>)).

join_bins_single_test() ->
    ?assertEqual(<<"hello">>, kura_preloader:join_bins([<<"hello">>], <<", ">>)).

join_bins_multiple_test() ->
    ?assertEqual(
        <<"a, b, c">>,
        kura_preloader:join_bins([<<"a">>, <<"b">>, <<"c">>], <<", ">>)
    ).
