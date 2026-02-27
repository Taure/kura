-module(kura_test_label).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"_embedded">>.

fields() ->
    [
        #kura_field{name = label, type = string},
        #kura_field{name = weight, type = integer}
    ].
