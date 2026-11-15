-module(kura_test_bad_redact_schema).

-include("kura.hrl").

-export([table/0, fields/0, redact/0]).

table() -> ~"bad_redacts".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = token, type = string}
    ].

redact() -> [~"token"].
