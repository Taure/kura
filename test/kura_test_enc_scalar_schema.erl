-module(kura_test_enc_scalar_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_scalars".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = owner_id, type = {encrypted, integer}},
        #kura_field{name = flag, type = {encrypted, boolean}}
    ].
