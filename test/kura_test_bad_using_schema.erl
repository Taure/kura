-module(kura_test_bad_using_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"kura_index_method_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = string}
    ].

indexes() ->
    [{[email], #{using => ill_typed(~"hash")}}].

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) -> Term.
