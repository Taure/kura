-module(kura_test_index_method_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"kura_index_method_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = string},
        #kura_field{name = name, type = string},
        #kura_field{name = code, type = string}
    ].

indexes() ->
    [
        {[email], #{using => hash}},
        {[name], #{}},
        {[code], #{using => btree}}
    ].
