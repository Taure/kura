-module(kura_test_secret_owner_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"owners".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = accounts,
            type = has_many,
            schema = kura_test_secret_schema,
            foreign_key = owner_id
        },
        #kura_assoc{
            name = account,
            type = has_one,
            schema = kura_test_secret_schema,
            foreign_key = owner_id
        }
    ].
