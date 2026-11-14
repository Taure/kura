-module(kura_test_join_review_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"join_reviews".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = book_id, type = integer, nullable = false},
        #kura_field{name = rating, type = integer, column = ~"stars"}
    ].
