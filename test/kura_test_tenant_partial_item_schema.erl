-module(kura_test_tenant_partial_item_schema).
-moduledoc "`tenant_items` without the tenant column the table has.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"tenant_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = score, type = integer}
    ].
