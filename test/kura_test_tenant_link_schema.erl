-module(kura_test_tenant_link_schema).
-moduledoc "`tenant_item_links`, a table with no tenant column.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"tenant_item_links".

fields() ->
    [
        #kura_field{name = item_id, type = id, primary_key = true},
        #kura_field{name = linked_id, type = id}
    ].
