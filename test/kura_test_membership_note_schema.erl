-module(kura_test_membership_note_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"membership_notes".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = org_id, type = uuid, nullable = false},
        #kura_field{name = user_id, type = uuid, nullable = false},
        #kura_field{name = body, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = membership,
            type = belongs_to,
            ref = #kura_ref{fields = [org_id, user_id], target = kura_test_composite_schema}
        }
    ].
