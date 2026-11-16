-module(kura_test_tenant_shared_link_schema).
-moduledoc "`tenant_item_links`, read as a table every tenant shares.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, tenant_scoped/0]).

table() -> ~"tenant_item_links".

fields() ->
    [
        #kura_field{name = item_id, type = id, primary_key = true},
        #kura_field{name = linked_id, type = id}
    ].

tenant_scoped() -> false.
