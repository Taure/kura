-module(kura_test_composite_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, key/0, fields/0, associations/0]).

table() -> ~"memberships".

key() -> [org_id, user_id].

fields() ->
    [
        #kura_field{name = org_id, type = uuid, nullable = false},
        #kura_field{name = user_id, type = uuid, nullable = false},
        #kura_field{name = role, type = string, default = ~"member"}
    ].

associations() ->
    [
        #kura_assoc{
            name = notes,
            type = has_many,
            ref = #kura_ref{fields = [org_id, user_id], target = kura_test_membership_note_schema}
        },
        #kura_assoc{
            name = latest_note,
            type = has_one,
            ref = #kura_ref{fields = [org_id, user_id], target = kura_test_membership_note_schema}
        }
    ].
