-module(kura_test_tenant_note_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"tenant_notes".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = org_id, type = integer},
        #kura_field{name = item_id, type = integer},
        #kura_field{name = body, type = string}
    ].
