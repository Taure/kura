-module(kura_test_enc_embed_parent).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> ~"enc_embed_parent".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true}].

embeds() ->
    [#kura_embed{name = middle, type = embeds_many, schema = kura_test_enc_embed_middle}].
