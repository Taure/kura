-module(kura_test_enc_fk_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"enc_fk".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = owner_id, type = {encrypted, integer}}
    ].

associations() ->
    [
        #kura_assoc{
            name = owner, type = belongs_to, schema = kura_test_schema, foreign_key = owner_id
        }
    ].
