-module(kura_fragment_scope_tests).
-moduledoc """
The conditions kura adds to a query (the tenant's, the soft-delete filter,
the keys of a conditional preload) apply to the rows a fragment matches,
whatever operators the fragment's text holds.
""".

-include_lib("eunit/include/eunit.hrl").

-define(REPO, kura_fragment_scope_test_repo).
-define(ITEMS, kura_test_edge_schema).
-define(SOFT, kura_test_soft_delete_schema).
-define(TENANT, {attribute, {qty, 7}}).
-define(EITHER, {fragment, ~"name ILIKE ? OR note ILIKE ?", [~"%", ~"%"]}).

scope_test_() ->
    {foreach,
        fun() ->
            kura_test_recording_driver:configure(?REPO, #{require_tenant => false}),
            kura_query_cache:init(),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_tenant:clear_tenant(),
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush()
        end,
        [
            {"a select keeps the tenant condition outside the fragment", fun() ->
                Q = kura_query:where(kura_query:from(?ITEMS), ?EITHER),
                {ok, []} = under_tenant({kura_repo_worker, all, [?REPO, Q]}),
                {ok, []} = under_tenant({kura_repo_worker, all, [?REPO, Q]}),
                Statement = {
                    <<
                        "SELECT * FROM \"edge_items\" WHERE (name ILIKE $1 OR note ILIKE $2\n)"
                        " AND \"qty\" = $3"
                    >>,
                    [~"%", ~"%", 7]
                },
                ?assertEqual([Statement, Statement], kura_test_recording_driver:statements())
            end},
            {"update_all does", fun() ->
                Q = kura_query:where(kura_query:from(?ITEMS), ?EITHER),
                {ok, 0} = under_tenant({kura_repo_worker, update_all, [?REPO, Q, #{note => ~"x"}]}),
                ?assertEqual(
                    [
                        {
                            <<
                                "UPDATE \"edge_items\" SET \"note\" = $1"
                                " WHERE (name ILIKE $2 OR note ILIKE $3\n) AND \"qty\" = $4"
                            >>,
                            [~"x", ~"%", ~"%", 7]
                        }
                    ],
                    kura_test_recording_driver:statements()
                )
            end},
            {"delete_all does", fun() ->
                Q = kura_query:where(kura_query:from(?ITEMS), ?EITHER),
                {ok, 0} = under_tenant({kura_repo_worker, delete_all, [?REPO, Q]}),
                ?assertEqual(
                    [
                        {
                            <<
                                "DELETE FROM \"edge_items\""
                                " WHERE (name ILIKE $1 OR note ILIKE $2\n) AND \"qty\" = $3"
                            >>,
                            [~"%", ~"%", 7]
                        }
                    ],
                    kura_test_recording_driver:statements()
                )
            end},
            {"a fragment that ends in a line comment keeps what follows it", fun() ->
                Q = kura_query:where(
                    kura_query:from(?ITEMS), {fragment, ~"name = ? -- by name", [~"a"]}
                ),
                {ok, []} = under_tenant({kura_repo_worker, all, [?REPO, Q]}),
                ?assertEqual(
                    [
                        {
                            ~"SELECT * FROM \"edge_items\" WHERE (name = $1 -- by name\n) AND \"qty\" = $2",
                            [~"a", 7]
                        }
                    ],
                    kura_test_recording_driver:statements()
                )
            end},
            {"the soft-delete filter stays outside the fragment", fun() ->
                Q = kura_query:where(
                    kura_query:from(?SOFT), {fragment, ~"name = ? OR status = ?", [~"a", ~"b"]}
                ),
                {ok, []} = kura_repo_worker:all(?REPO, Q),
                {ok, 0} = kura_repo_worker:update_all(?REPO, Q, #{status => ~"c"}),
                ?assertEqual(
                    [
                        {
                            <<
                                "SELECT * FROM \"soft_items\" WHERE (name = $1 OR status = $2\n)"
                                " AND \"deleted_at\" IS NULL"
                            >>,
                            [~"a", ~"b"]
                        },
                        {
                            <<
                                "UPDATE \"soft_items\" SET \"status\" = $1"
                                " WHERE (name = $2 OR status = $3\n) AND \"deleted_at\" IS NULL"
                            >>,
                            [~"c", ~"a", ~"b"]
                        }
                    ],
                    kura_test_recording_driver:statements()
                )
            end},
            {"a conditional preload reads the owners' rows only", fun() ->
                Opts = #{where => {fragment, ~"published = ? OR title = ?", [true, ~"a"]}},
                [#{posts := []}] = kura_repo_worker:preload(
                    ?REPO, kura_test_cond_user, [#{id => 3}], [{posts, Opts}]
                ),
                ?assertEqual(
                    [
                        {
                            <<
                                "SELECT * FROM \"cond_posts\" WHERE \"user_id\" IN ($1)"
                                " AND (published = $2 OR title = $3\n)"
                            >>,
                            [3, true, ~"a"]
                        }
                    ],
                    kura_test_recording_driver:statements()
                )
            end}
        ]}.

under_tenant(Run) ->
    kura_tenant:with_tenant(?TENANT, Run).
