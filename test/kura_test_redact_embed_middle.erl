-module(kura_test_redact_embed_middle).
-moduledoc "An embedded schema that redacts nothing itself, embeds one that does, and embeds itself.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> ~"_embedded".

fields() ->
    [#kura_field{name = note, type = string}].

embeds() ->
    [
        #kura_embed{name = replies, type = embeds_many, schema = ?MODULE},
        #kura_embed{name = creds, type = embeds_one, schema = kura_test_redact_embed_creds}
    ].
