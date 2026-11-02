-module(kura_test_enc_embed_middle).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> ~"_embedded".

fields() ->
    [#kura_field{name = note, type = string}].

embeds() ->
    [#kura_embed{name = inner, type = embeds_one, schema = kura_test_enc_embedded}].
