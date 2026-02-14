-module(kura_test_address).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, primary_key/0]).

table() -> <<"_embedded">>.

primary_key() -> undefined.

fields() ->
    [
        #kura_field{name = street, type = string},
        #kura_field{name = city, type = string},
        #kura_field{name = zip, type = string}
    ].
