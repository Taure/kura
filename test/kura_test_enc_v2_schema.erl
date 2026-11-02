-module(kura_test_enc_v2_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, v2_type/0]).

table() -> ~"enc_v2".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = v2_type()}
    ].

%% The options form planned for kura#137. Not an accepted type yet, so it is
%% built at run time rather than written where the type checker sees it.
v2_type() ->
    list_to_tuple([encrypted, string, #{mode => deterministic}]).
