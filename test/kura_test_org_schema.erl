-module(kura_test_org_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"orgs".

fields() ->
    [
        #kura_field{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_field{name = name, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = members,
            type = has_many,
            ref = #kura_ref{fields = [org_id], target = kura_test_composite_schema}
        }
    ].
