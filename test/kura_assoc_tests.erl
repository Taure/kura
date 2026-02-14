-module(kura_assoc_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Schema helpers — associations/1, association/2
%%----------------------------------------------------------------------

associations_returns_empty_for_no_callback_test() ->
    ?assertEqual([], kura_schema:associations(kura_test_schema)).

associations_returns_list_test() ->
    Assocs = kura_schema:associations(kura_test_post),
    ?assertEqual(3, length(Assocs)).

association_found_test() ->
    {ok, Assoc} = kura_schema:association(kura_test_post, author),
    ?assertEqual(author, Assoc#kura_assoc.name),
    ?assertEqual(belongs_to, Assoc#kura_assoc.type),
    ?assertEqual(kura_test_schema, Assoc#kura_assoc.schema),
    ?assertEqual(author_id, Assoc#kura_assoc.foreign_key).

association_not_found_test() ->
    ?assertEqual({error, not_found}, kura_schema:association(kura_test_post, nonexistent)).

has_many_association_test() ->
    {ok, Assoc} = kura_schema:association(kura_test_post, comments),
    ?assertEqual(comments, Assoc#kura_assoc.name),
    ?assertEqual(has_many, Assoc#kura_assoc.type),
    ?assertEqual(kura_test_comment, Assoc#kura_assoc.schema),
    ?assertEqual(post_id, Assoc#kura_assoc.foreign_key).

%%----------------------------------------------------------------------
%% Query preload
%%----------------------------------------------------------------------

query_preload_test() ->
    Q = kura_query:preload(kura_query:from(kura_test_post), [author, comments]),
    ?assertEqual([author, comments], Q#kura_query.preloads).

query_preload_nested_test() ->
    Q = kura_query:preload(kura_query:from(kura_test_post), [{comments, [author]}]),
    ?assertEqual([{comments, [author]}], Q#kura_query.preloads).

query_preload_stacks_test() ->
    Q0 = kura_query:from(kura_test_post),
    Q1 = kura_query:preload(Q0, [author]),
    Q2 = kura_query:preload(Q1, [comments]),
    ?assertEqual([author, comments], Q2#kura_query.preloads).
