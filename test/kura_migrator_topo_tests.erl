-module(kura_migrator_topo_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Topological sort tests for create_table operations
%%----------------------------------------------------------------------

%% No create_table ops — pass through unchanged.
no_creates_test() ->
    Ops = [{drop_table, <<"foo">>}, {create_index, <<"idx">>, <<"foo">>, [name]}],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% Single table, no deps.
single_table_test() ->
    Ops = [
        {create_table, <<"users">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]}
    ],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% Two tables: B references A. If B comes first, sort should fix it.
simple_dependency_test() ->
    A =
        {create_table, <<"players">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    B =
        {create_table, <<"wallets">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    %% B before A in input — should be reordered to A, B
    Result = kura_migrator:topo_sort_ops([B, A]),
    ?assertEqual([A, B], Result).

%% Already correct order stays the same.
already_sorted_test() ->
    A =
        {create_table, <<"players">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    B =
        {create_table, <<"wallets">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Result = kura_migrator:topo_sort_ops([A, B]),
    ?assertEqual([A, B], Result).

%% Chain: C -> B -> A
chain_dependency_test() ->
    A =
        {create_table, <<"players">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    B =
        {create_table, <<"wallets">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    C =
        {create_table, <<"transactions">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = wallet_id, type = uuid, references = {<<"wallets">>, id}}
        ]},
    %% Reversed input
    Result = kura_migrator:topo_sort_ops([C, B, A]),
    Names = [element(2, Op) || Op <- Result],
    ?assertEqual([<<"players">>, <<"wallets">>, <<"transactions">>], Names).

%% Non-create ops are preserved in their slots.
mixed_ops_test() ->
    A =
        {create_table, <<"players">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    B =
        {create_table, <<"wallets">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Idx = {create_index, <<"idx_wallets">>, <<"wallets">>, [player_id]},
    %% B before A, with an index op in between
    Input = [B, Idx, A],
    Result = kura_migrator:topo_sort_ops(Input),
    %% create_table slots get sorted creates; index stays in its slot
    ?assertEqual([A, Idx, B], Result).

%% Self-referencing table (e.g. parent_id -> same table) should not cause issues.
self_reference_test() ->
    A =
        {create_table, <<"categories">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = parent_id, type = uuid, references = {<<"categories">>, id}}
        ]},
    ?assertEqual([A], kura_migrator:topo_sort_ops([A])).

%% Real-world test based on the asobi schema: alphabetically ordered tables
%% with FK references should be reordered so players comes before everything
%% that references it.
asobi_schema_test() ->
    ChatMessages =
        {create_table, <<"chat_messages">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = sender_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Friendships =
        {create_table, <<"friendships">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}},
            #kura_column{name = friend_id, type = uuid, references = {<<"players">>, id}}
        ]},
    GroupMembers =
        {create_table, <<"group_members">>, [
            #kura_column{name = group_id, type = uuid, references = {<<"groups">>, id}},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Groups =
        {create_table, <<"groups">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = creator_id, type = uuid, references = {<<"players">>, id}}
        ]},
    ItemDefs =
        {create_table, <<"item_defs">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    PlayerItems =
        {create_table, <<"player_items">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = item_def_id, type = uuid, references = {<<"item_defs">>, id}},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    PlayerStats =
        {create_table, <<"player_stats">>, [
            #kura_column{
                name = player_id, type = uuid, primary_key = true, references = {<<"players">>, id}
            }
        ]},
    PlayerTokens =
        {create_table, <<"player_tokens">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = user_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Players =
        {create_table, <<"players">>, [
            #kura_column{name = id, type = uuid, primary_key = true}
        ]},
    Wallets =
        {create_table, <<"wallets">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = player_id, type = uuid, references = {<<"players">>, id}}
        ]},
    Transactions =
        {create_table, <<"transactions">>, [
            #kura_column{name = id, type = uuid, primary_key = true},
            #kura_column{name = wallet_id, type = uuid, references = {<<"wallets">>, id}}
        ]},

    %% Alphabetical order (wrong for FK deps)
    Input = [
        ChatMessages,
        Friendships,
        GroupMembers,
        Groups,
        ItemDefs,
        PlayerItems,
        PlayerStats,
        PlayerTokens,
        Players,
        Wallets,
        Transactions
    ],

    Result = kura_migrator:topo_sort_ops(Input),
    Names = [element(2, Op) || Op <- Result],

    %% Verify: players must come before everything that references it
    PlayersIdx = index_of(<<"players">>, Names),
    GroupsIdx = index_of(<<"groups">>, Names),
    ItemDefsIdx = index_of(<<"item_defs">>, Names),
    WalletsIdx = index_of(<<"wallets">>, Names),

    ?assert(PlayersIdx < index_of(<<"chat_messages">>, Names)),
    ?assert(PlayersIdx < index_of(<<"friendships">>, Names)),
    ?assert(PlayersIdx < index_of(<<"player_stats">>, Names)),
    ?assert(PlayersIdx < index_of(<<"player_tokens">>, Names)),
    ?assert(PlayersIdx < index_of(<<"wallets">>, Names)),
    ?assert(PlayersIdx < GroupsIdx),
    ?assert(GroupsIdx < index_of(<<"group_members">>, Names)),
    ?assert(ItemDefsIdx < index_of(<<"player_items">>, Names)),
    ?assert(WalletsIdx < index_of(<<"transactions">>, Names)).

%% Helper
index_of(Elem, List) ->
    index_of(Elem, List, 1).

index_of(_Elem, [], _N) ->
    error(not_found);
index_of(Elem, [Elem | _], N) ->
    N;
index_of(Elem, [_ | Rest], N) ->
    index_of(Elem, Rest, N + 1).
