-module(kura_test_tenant_plan_schema).
-moduledoc "`tenant_plans`, a table with no tenant column that every tenant shares.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, tenant_scoped/0]).

table() -> ~"tenant_plans".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string},
        #kura_field{name = price, type = integer}
    ].

tenant_scoped() -> false.
