-module(kura_test_tenant_open_item_schema).
-moduledoc "`tenant_items` with its tenant column, by a schema that says every tenant shares it.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, tenant_scoped/0]).

table() -> ~"tenant_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = org_id, type = integer},
        #kura_field{name = name, type = string},
        #kura_field{name = score, type = integer}
    ].

tenant_scoped() -> false.
