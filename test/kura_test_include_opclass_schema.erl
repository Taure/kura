-module(kura_test_include_opclass_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"kura_index_opclass_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = embedding, type = {vector, 3}},
        #kura_field{name = code, type = string}
    ].

indexes() ->
    [
        {[id], #{unique => true, opclass => int8_ops}},
        {[code], #{opclass => text_pattern_ops}}
    ].
