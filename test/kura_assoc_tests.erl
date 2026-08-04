-module(kura_assoc_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Schema helpers - associations/1, association/2
%%----------------------------------------------------------------------

associations_returns_empty_for_no_callback_test() ->
    ?assertEqual([], kura_schema:associations(kura_test_post_simple_schema)).

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

%%----------------------------------------------------------------------
%% assoc_on_delete/1
%%----------------------------------------------------------------------

belongs_to() ->
    #kura_assoc{
        name = author, type = belongs_to, schema = kura_test_schema, foreign_key = author_id
    }.

on_delete_defaults_to_no_action_test() ->
    ?assertEqual(no_action, kura_schema:assoc_on_delete(belongs_to())).

on_delete_cascade_test() ->
    ?assertEqual(
        cascade, kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = cascade})
    ).

on_delete_restrict_test() ->
    ?assertEqual(
        restrict, kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = restrict})
    ).

on_delete_set_null_test() ->
    ?assertEqual(
        set_null, kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = set_null})
    ).

on_delete_no_action_test() ->
    ?assertEqual(
        no_action, kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = no_action})
    ).

on_delete_rejects_set_default_test() ->
    ?assertError(
        {invalid_on_delete, author, set_default},
        kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = set_default})
    ).

on_delete_rejects_typo_test() ->
    ?assertError(
        {invalid_on_delete, author, cascde},
        kura_schema:assoc_on_delete((belongs_to())#kura_assoc{on_delete = cascde})
    ).

on_delete_undefined_on_has_many_test() ->
    Assoc = #kura_assoc{name = comments, type = has_many, schema = kura_test_comment},
    ?assertEqual(undefined, kura_schema:assoc_on_delete(Assoc)).

on_delete_rejected_on_has_many_test() ->
    Assoc = #kura_assoc{
        name = comments, type = has_many, schema = kura_test_comment, on_delete = cascade
    },
    ?assertError({on_delete_not_owned, comments, has_many}, kura_schema:assoc_on_delete(Assoc)).

on_delete_rejected_on_many_to_many_test() ->
    Assoc = #kura_assoc{
        name = tags, type = many_to_many, schema = kura_test_tag, on_delete = cascade
    },
    ?assertError({on_delete_not_owned, tags, many_to_many}, kura_schema:assoc_on_delete(Assoc)).
