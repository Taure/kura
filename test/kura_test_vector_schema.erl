-module(kura_test_vector_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"kura_vector_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = content, type = text},
        #kura_field{name = embedding, type = {vector, 3}},
        #kura_field{name = any_embedding, type = vector}
    ].
