-module(kura_test_participant_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, constraints/0]).

table() -> <<"participants">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = chat_id, type = uuid, nullable = false},
        #kura_field{name = user_id, type = uuid, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

constraints() ->
    [{unique, [chat_id, user_id]}].
