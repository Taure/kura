-module(kura_test_encrypted_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"secrets">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string},
        #kura_field{name = ssn, type = {encrypted, string}, nullable = false}
    ].
