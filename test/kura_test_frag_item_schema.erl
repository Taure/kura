-module(kura_test_frag_item_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"frag_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = org_id, type = integer, nullable = false},
        #kura_field{name = owner_id, type = integer},
        #kura_field{name = name, type = string},
        #kura_field{name = note, type = string},
        #kura_field{name = deleted_at, type = utc_datetime}
    ].
