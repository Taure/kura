-module(kura_test_redact_embed_parent).
-moduledoc "A schema that redacts nothing itself and embeds schemas that do.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, embeds/0]).

table() -> ~"redact_embed_parents".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = name, type = string}
    ].

embeds() ->
    [
        #kura_embed{name = creds, type = embeds_one, schema = kura_test_redact_embed_creds},
        #kura_embed{name = thread, type = embeds_many, schema = kura_test_redact_embed_middle},
        #kura_embed{name = address, type = embeds_one, schema = kura_test_address}
    ].
