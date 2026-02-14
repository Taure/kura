-module(kura_test_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, primary_key/0]).

table() -> <<"users">>.

primary_key() -> id.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = email, type = string, nullable = false},
        #kura_field{name = age, type = integer},
        #kura_field{name = active, type = boolean, default = true},
        #kura_field{name = role, type = string, default = <<"user">>},
        #kura_field{name = score, type = float},
        #kura_field{name = metadata, type = jsonb},
        #kura_field{name = status, type = {enum, [active, inactive, banned]}},
        #kura_field{name = tags, type = {array, string}},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime},
        #kura_field{name = full_name, type = string, virtual = true}
    ].
