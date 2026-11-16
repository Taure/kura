-module(kura_test_shared_list_schema).
-moduledoc "A schema every tenant shares, with records of another shared schema.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0, tenant_scoped/0]).

table() -> ~"shared_lists".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = entries,
            type = has_many,
            schema = kura_test_shared_entry_schema,
            foreign_key = list_id
        }
    ].

tenant_scoped() -> false.
