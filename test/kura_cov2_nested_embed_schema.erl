-module(kura_cov2_nested_embed_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> <<"_embedded">>.

fields() ->
    [
        #kura_field{name = label, type = string},
        #kura_field{name = count, type = integer}
    ].

embeds() ->
    [
        #kura_embed{name = address, type = embeds_one, schema = kura_test_address},
        #kura_embed{name = labels, type = embeds_many, schema = kura_test_label}
    ].
