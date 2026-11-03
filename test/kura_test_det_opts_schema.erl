-module(kura_test_det_opts_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"det_opts".

fields() ->
    [
        #kura_field{name = id, type = id_type(), primary_key = true},
        #kura_field{name = value, type = value_type()}
    ].

-spec id_type() -> dynamic().
id_type() ->
    application:get_env(kura, test_det_id_type, id).

-spec value_type() -> dynamic().
value_type() ->
    application:get_env(kura, test_det_type, {encrypted, string, #{mode => deterministic}}).
