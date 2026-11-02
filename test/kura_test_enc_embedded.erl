-module(kura_test_enc_embedded).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"_embedded".

fields() ->
    [
        #kura_field{name = label, type = string},
        #kura_field{name = secret, type = {encrypted, string}}
    ].
