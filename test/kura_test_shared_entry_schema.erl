-module(kura_test_shared_entry_schema).
-moduledoc "The records of `kura_test_shared_list_schema`, shared by every tenant too.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, tenant_scoped/0]).

table() -> ~"shared_entries".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = list_id, type = integer},
        #kura_field{name = name, type = string}
    ].

tenant_scoped() -> false.
