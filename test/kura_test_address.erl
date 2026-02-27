-module(kura_test_address).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"_embedded">>.

fields() ->
    [
        #kura_field{name = street, type = string},
        #kura_field{name = city, type = string},
        #kura_field{name = zip, type = string}
    ].
