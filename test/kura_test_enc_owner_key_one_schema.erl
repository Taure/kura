-module(kura_test_enc_owner_key_one_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"enc_owner_key_one".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = {encrypted, string}}
    ].

associations() ->
    [
        #kura_assoc{
            name = kid,
            type = has_one,
            schema = kura_test_schema,
            ref = #kura_ref{fields = [email], target = kura_test_schema, target_key = [email]}
        }
    ].
