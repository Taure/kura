-module(kura_test_shared_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, tenant_scoped/0]).

table() -> ~"shared_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

tenant_scoped() -> false.
