-module(cte_admins).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

%% CTE alias — mirrors kura_test_schema fields for type loading
table() -> ~"cte_admins".

fields() -> kura_test_schema:fields().
