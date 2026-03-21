-module(kura_test_type_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"type_items".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = bio, type = text},
        #kura_field{name = birth_date, type = date},
        #kura_field{name = status, type = {enum, [active, inactive, banned]}},
        #kura_field{name = score, type = float},
        #kura_field{name = count, type = integer},
        #kura_field{name = active, type = boolean, default = true},
        #kura_field{name = tags, type = {array, string}},
        #kura_field{name = scores, type = {array, integer}},
        #kura_field{name = metadata, type = jsonb},
        #kura_field{name = ref_id, type = uuid},
        #kura_field{name = happened_at, type = utc_datetime},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].
