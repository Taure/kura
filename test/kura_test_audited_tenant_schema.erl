-module(kura_test_audited_tenant_schema).
-moduledoc "An audited schema that declares a tenant's field.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).
-export([before_update/1, after_insert/1, after_update/1, after_delete/1]).

-define(REPO, kura_audit_tenant_test_repo).

table() -> ~"audited_tenant_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = org_id, type = integer},
        #kura_field{name = name, type = string}
    ].

before_update(CS) ->
    kura_audit:stash(CS),
    {ok, CS}.

after_insert(Record) ->
    kura_audit:log(?REPO, ?MODULE, insert, Record),
    {ok, Record}.

after_update(Record) ->
    kura_audit:log(?REPO, ?MODULE, update, Record),
    {ok, Record}.

after_delete(Record) ->
    kura_audit:log(?REPO, ?MODULE, delete, Record),
    ok.
