-module(kura_test_uuid_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"uuid_items".

fields() ->
    [
        #kura_field{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = external_id, type = uuid},
        #kura_field{name = correlation_id, type = uuid},
        #kura_field{name = status, type = string, default = ~"active"},
        #kura_field{name = tags, type = {array, string}},
        #kura_field{name = metadata, type = jsonb},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = children,
            type = has_many,
            schema = kura_test_uuid_child_schema,
            foreign_key = parent_id
        }
    ].
