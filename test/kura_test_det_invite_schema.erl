-module(kura_test_det_invite_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"det_invites".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = user_id, type = integer},
        #kura_field{name = email, type = {encrypted, string, #{mode => deterministic}}},
        #kura_field{
            name = code,
            type = {encrypted, string, #{mode => deterministic, context => {~"shared", ~"code"}}}
        }
    ].
