-module(kura_test_decimal_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"decimal_prices".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = price, type = decimal}
    ].
