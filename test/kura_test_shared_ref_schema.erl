-module(kura_test_shared_ref_schema).
-moduledoc "A tenant's rows that each point at a row of a schema every tenant shares.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0, tenant_scoped/0]).

table() -> ~"shared_refs".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = shared_id, type = integer},
        #kura_field{name = qty, type = integer}
    ].

associations() ->
    [
        #kura_assoc{
            name = shared,
            type = belongs_to,
            schema = kura_test_shared_schema,
            foreign_key = shared_id
        }
    ].

tenant_scoped() -> true.
