-module(kura_test_renamed_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"renamed_items".

fields() ->
    [
        #kura_field{
            name = id, type = id, primary_key = true, nullable = false, column = ~"item_id"
        },
        #kura_field{name = title, type = string, nullable = false, column = ~"item_title"},
        #kura_field{name = qty, type = integer, column = ~"quantity"},
        #kura_field{name = note, type = string, column = ~"note"},
        #kura_field{name = draft, type = string, virtual = true, column = ~"never_stored"}
    ].
