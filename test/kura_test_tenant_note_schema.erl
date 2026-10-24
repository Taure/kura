-module(kura_test_tenant_note_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"tenant_notes".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = org_id, type = integer},
        #kura_field{name = item_id, type = integer},
        #kura_field{name = body, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = item,
            type = belongs_to,
            schema = kura_test_tenant_item_schema,
            foreign_key = item_id
        },
        #kura_assoc{
            name = parent,
            type = belongs_to,
            schema = kura_test_tenant_item_schema,
            foreign_key = item_id
        }
    ].
