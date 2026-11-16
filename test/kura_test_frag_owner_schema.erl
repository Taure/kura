-module(kura_test_frag_owner_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"frag_owners".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = org_id, type = integer, nullable = false},
        #kura_field{name = name, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = items,
            type = has_many,
            schema = kura_test_frag_item_schema,
            foreign_key = owner_id
        }
    ].
