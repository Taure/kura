-module(kura_test_tenant_item_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"tenant_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = org_id, type = integer},
        #kura_field{name = name, type = string},
        #kura_field{name = score, type = integer},
        #kura_field{name = lock_version, type = integer}
    ].

associations() ->
    [
        #kura_assoc{
            name = notes,
            type = has_many,
            schema = kura_test_tenant_note_schema,
            foreign_key = item_id
        },
        #kura_assoc{
            name = links,
            type = many_to_many,
            schema = kura_test_tenant_item_schema,
            join_through = ~"tenant_item_links",
            join_keys = {item_id, linked_id}
        }
    ].
