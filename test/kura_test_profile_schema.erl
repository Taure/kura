-module(kura_test_profile_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"profiles">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = bio, type = string},
        #kura_field{name = user_id, type = integer, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = user,
            type = belongs_to,
            schema = kura_test_schema,
            foreign_key = user_id
        }
    ].
