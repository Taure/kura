-module(kura_test_enc_has_many_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"enc_has_many".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true}].

associations() ->
    [
        #kura_assoc{
            name = children,
            type = has_many,
            schema = kura_test_enc_fk_schema,
            foreign_key = owner_id
        }
    ].
