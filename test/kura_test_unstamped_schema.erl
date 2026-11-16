-module(kura_test_unstamped_schema).
-moduledoc "No timestamp fields, and a `timestamps/0` that says so.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, timestamps/0]).

table() -> ~"plain_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

timestamps() -> [].
