-module(kura_test_bad_opclass_schema).
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
    [{[embedding], #{opclass => ~"vector_cosine_ops"}}].
