-module(kura_migrator_topo_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Topological sort tests for create_table operations
%%----------------------------------------------------------------------

%% No create_table ops - pass through unchanged.
no_creates_test() ->
    Ops = [{drop_table, <<"foo">>}, {create_index, <<"idx">>, <<"foo">>, [name], []}],
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
    %% B before A in input - should be reordered to A, B
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

%% A table referenced before it is created moves up to just before the table
%% that references it; nothing else moves, so an index still follows its table.
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
    Idx = {create_index, <<"idx_wallets">>, <<"wallets">>, [player_id], []},
    %% B before A, with an index op in between
    Input = [B, Idx, A],
    Result = kura_migrator:topo_sort_ops(Input),
    ?assertEqual([A, B, Idx], Result).

%% Two tables that do not reference each other stay as written, whatever
%% their names sort as, so an index between them finds its table.
independent_tables_keep_the_written_order_test() ->
    Users =
        {create_table, ~"users", [
            #kura_column{name = id, type = id, primary_key = true}
        ]},
    UsersIdx = {create_index, ~"users_email_index", ~"users", [email], [unique]},
    Tokens =
        {create_table, ~"user_tokens", [
            #kura_column{name = id, type = id, primary_key = true},
            #kura_column{name = user_id, type = integer}
        ]},
    TokensIdx = {create_index, ~"user_tokens_user_id_index", ~"user_tokens", [user_id], []},
    Ops = [Users, UsersIdx, Tokens, TokensIdx],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% Only the referenced table moves; tables written in between stay put.
only_the_referenced_table_moves_test() ->
    Zebra = {create_table, ~"zebras", [#kura_column{name = id, type = id}]},
    Wallets =
        {create_table, ~"wallets", [
            #kura_column{name = player_id, type = uuid, references = {~"players", id}}
        ]},
    Apples = {create_table, ~"apples", [#kura_column{name = id, type = id}]},
    Players = {create_table, ~"players", [#kura_column{name = id, type = uuid}]},
    ?assertEqual(
        [Zebra, Players, Wallets, Apples],
        kura_migrator:topo_sort_ops([Zebra, Wallets, Apples, Players])
    ).

%% Two tables that reference each other cannot be ordered by creation. Both
%% are kept, once each, the one the first refers to in front.
tables_that_reference_each_other_are_all_kept_test() ->
    A =
        {create_table, ~"a", [
            #kura_column{name = b_id, type = id, references = {~"b", id}}
        ]},
    B =
        {create_table, ~"b", [
            #kura_column{name = a_id, type = id, references = {~"a", id}}
        ]},
    C = {create_table, ~"c", [#kura_column{name = id, type = id}]},
    ?assertEqual([B, A, C], kura_migrator:topo_sort_ops([A, B, C])),
    ?assertEqual([C, A, B], kura_migrator:topo_sort_ops([C, B, A])).

%% A table dropped and created again in one migration is created both times.
a_table_created_twice_is_created_twice_test() ->
    First = {create_table, ~"scratch", [#kura_column{name = id, type = id}]},
    Second = {create_table, ~"scratch", [#kura_column{name = id, type = uuid}]},
    Ops = [First, {drop_table, ~"scratch"}, Second],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% An index written before the table it is on gets the table first.
an_index_written_before_its_table_gets_the_table_first_test() ->
    Users = {create_table, ~"users", [#kura_column{name = id, type = id}]},
    Idx = {create_index, ~"accounts", [email], #{unique => true}},
    NamedIdx = {create_index, ~"accounts_name_index", ~"accounts", [name], []},
    Accounts = {create_table, ~"accounts", [#kura_column{name = id, type = id}]},
    ?assertEqual(
        [Users, Accounts, Idx, NamedIdx],
        kura_migrator:topo_sort_ops([Users, Idx, NamedIdx, Accounts])
    ).

%% So does an alter_table, for its own table and for one a new column refers to.
an_alter_written_before_a_table_it_needs_gets_the_table_first_test() ->
    Alter =
        {alter_table, ~"posts", [
            {add_column, #kura_column{name = author_id, type = id, references = {~"authors", id}}}
        ]},
    Posts = {create_table, ~"posts", [#kura_column{name = id, type = id}]},
    Authors = {create_table, ~"authors", [#kura_column{name = id, type = id}]},
    ?assertEqual([Posts, Authors, Alter], kura_migrator:topo_sort_ops([Alter, Posts, Authors])).

%% The four-element create_table moves like the three-element one.
a_table_with_constraints_moves_too_test() ->
    Members =
        {create_table, ~"members",
            [#kura_column{name = org_id, type = id, references = {~"orgs", id}}], [
                {primary_key, [org_id]}
            ]},
    Idx = {create_index, ~"members", [org_id], #{}},
    Orgs = {create_table, ~"orgs", [#kura_column{name = id, type = id}], [{unique, [id]}]},
    ?assertEqual([Orgs, Members, Idx], kura_migrator:topo_sort_ops([Members, Idx, Orgs])).

%% A table created twice has no single place to move to, so nothing that
%% names it is reordered.
a_table_created_twice_is_never_moved_test() ->
    Ref =
        {create_table, ~"x", [
            #kura_column{name = s_id, type = id, references = {~"scratch", id}}
        ]},
    First = {create_table, ~"scratch", [#kura_column{name = id, type = id}]},
    Second = {create_table, ~"scratch", [#kura_column{name = id, type = uuid}], []},
    Ops = [Ref, First, {drop_table, ~"scratch"}, Second],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% A table that is altered, dropped and created again already exists when
%% the migration starts. Its create stays where it is written.
a_table_dropped_and_created_again_is_never_moved_test() ->
    Ops = [
        {alter_table, ~"legacy", [{drop_column, old}]},
        {create_index, ~"legacy", [name], #{}},
        {drop_table, ~"legacy"},
        {create_table, ~"legacy", [#kura_column{name = id, type = id}]}
    ],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% Raw SQL is not read, so it never moves a table.
raw_sql_moves_nothing_test() ->
    Ops = [
        {execute, ~"INSERT INTO b DEFAULT VALUES"},
        {create_table, ~"b", [#kura_column{name = id, type = id}]},
        {create_table, ~"a", [#kura_column{name = id, type = id}]}
    ],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

%% A reference to a table this migration does not create changes nothing.
a_reference_to_an_existing_table_changes_nothing_test() ->
    Ops = [
        {create_table, ~"b", [
            #kura_column{name = a_id, type = id, references = {~"already_there", id}}
        ]},
        {create_table, ~"a", [#kura_column{name = id, type = id}]}
    ],
    ?assertEqual(Ops, kura_migrator:topo_sort_ops(Ops)).

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
