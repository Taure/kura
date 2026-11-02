-module(kura_test_enc_m2m_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"enc_m2m".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true}].

associations() ->
    [
        #kura_assoc{
            name = links,
            type = many_to_many,
            schema = kura_test_schema,
            join_through = kura_test_enc_fk_schema,
            join_keys = {id, owner_id}
        }
    ].
