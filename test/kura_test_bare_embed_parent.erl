-module(kura_test_bare_embed_parent).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> ~"bare_embed_parents".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = title, type = string}
    ].

embeds() ->
    [#kura_embed{name = meta, type = embeds_one, schema = kura_test_bare_embed}].
