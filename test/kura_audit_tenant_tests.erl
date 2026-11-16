-module(kura_audit_tenant_tests).
-moduledoc "The audit row of a write under a tenant.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_audit_tenant_test_repo).
-define(ITEMS, kura_test_audited_tenant_schema).
-define(TENANT, {attribute, {org_id, 7}}).
-define(ROW, #{id => 1, org_id => 7, name => ~"a"}).

with_required(Tests) ->
    {foreach,
        fun() ->
            application:set_env(kura, require_tenant, true),
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:init(),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_tenant:clear_tenant(),
            kura_audit:clear_actor(),
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush(),
            application:unset_env(kura, require_tenant)
        end,
        Tests}.

changeset(Data, Params) ->
    kura_changeset:cast(?ITEMS, Data, Params, [name]).

writes() ->
    [
        {"insert", insert, fun() ->
            kura_repo_worker:insert(?REPO, changeset(#{}, #{name => ~"a"}))
        end},
        {"update", update, fun() ->
            kura_repo_worker:update(
                ?REPO, changeset(#{id => 1, org_id => 7, name => ~"b"}, #{name => ~"a"})
            )
        end},
        {"delete", delete, fun() -> kura_repo_worker:delete(?REPO, changeset(?ROW, #{})) end}
    ].

%% The row the write returns, then the audit row.
replies(Command) ->
    [
        #{command => Command, rows => [?ROW], num_rows => 1},
        #{command => insert, rows => [#{id => 9}], num_rows => 1}
    ].

audited(Tenant, Command, Run) ->
    kura_test_recording_driver:reply_with(replies(Command)),
    Result = kura_tenant:with_tenant(Tenant, fun() ->
        Written = Run(),
        ?assertEqual(Tenant, kura_tenant:get_tenant()),
        Written
    end),
    ?assertEqual({ok, ?ROW}, Result),
    kura_test_recording_driver:statements().

an_audited_write_under_a_required_attribute_tenant_test_() ->
    with_required([
        {Name, fun() ->
            [{Write, _}, {Audit, Params}] = audited(?TENANT, Command, Run),
            ?assertMatch({_, _}, binary:match(Write, ~"\"org_id\"")),
            ?assertMatch(<<"INSERT INTO \"audit_log\" ", _/binary>>, Audit),
            ?assertEqual(nomatch, binary:match(Audit, ~"\"org_id\"")),
            ?assert(lists:member(atom_to_binary(Command), Params))
        end}
     || {Name, Command, Run} <- writes()
    ]).

an_audited_write_under_a_prefix_tenant_test_() ->
    with_required([
        {Name, fun() ->
            [{Write, _}, {Audit, _}] = audited({prefix, ~"t1"}, Command, Run),
            ?assertMatch({_, _}, binary:match(Write, ~"\"t1\".\"audited_tenant_items\"")),
            ?assertMatch(<<"INSERT INTO \"t1\".\"audit_log\" ", _/binary>>, Audit)
        end}
     || {Name, Command, Run} <- writes()
    ]).

the_audit_table_is_read_outside_the_tenant_test_() ->
    Q = kura_query:from(kura_audit_log),
    with_required([
        {"a read under the tenant is refused", fun() ->
            ?assertError(
                {invalid_tenant, ?TENANT},
                kura_tenant:with_tenant(?TENANT, {kura_repo_worker, all, [?REPO, Q]})
            ),
            ?assertEqual([], kura_test_recording_driver:statements())
        end},
        {"a read inside without_tenant/1 runs", fun() ->
            {ok, []} = kura_tenant:with_tenant(
                ?TENANT, {kura_tenant, without_tenant, [{kura_repo_worker, all, [?REPO, Q]}]}
            ),
            ?assertMatch(
                [{<<"SELECT ", _/binary>>, []}], kura_test_recording_driver:statements()
            )
        end}
    ]).
