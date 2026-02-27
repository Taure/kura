-module(kura_test_profile).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> <<"profiles">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = bio, type = text}
    ].

embeds() ->
    [
        #kura_embed{name = address, type = embeds_one, schema = kura_test_address},
        #kura_embed{name = labels, type = embeds_many, schema = kura_test_label}
    ].
