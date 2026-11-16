-module(kura_test_redact_embed_creds).
-moduledoc "An embedded schema with a field of its own to keep out of logs.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, redact/0]).

table() -> ~"_embedded".

fields() ->
    [
        #kura_field{name = api_key, type = string},
        #kura_field{name = api_secret, type = string}
    ].

redact() -> [api_secret].
