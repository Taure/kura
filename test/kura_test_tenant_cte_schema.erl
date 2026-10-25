-module(kura_test_tenant_cte_schema).
-behaviour(kura_schema).

-export([table/0, fields/0]).

table() -> ~"tenant_cte".

fields() -> kura_test_tenant_item_schema:fields().
