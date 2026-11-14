-module(kura_test_secret_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, redact/0]).

table() -> ~"accounts".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = email, type = string},
        #kura_field{name = password_hash, type = string},
        #kura_field{name = token, type = string}
    ].

redact() -> [password_hash, token].
