-module(kura_test_join_book_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"join_books".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = author_id, type = integer, nullable = false, column = ~"written_by"},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = rating, type = integer, column = ~"book_rating"},
        #kura_field{name = note, type = string}
    ].
