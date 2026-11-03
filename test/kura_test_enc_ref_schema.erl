-module(kura_test_enc_ref_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"enc_ref".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = owner_ssn, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = owner,
            type = belongs_to,
            schema = kura_test_encrypted_schema,
            ref = #kura_ref{
                fields = [owner_ssn], target = kura_test_encrypted_schema, target_key = [ssn]
            }
        }
    ].
