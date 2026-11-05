-module(kura_test_bare_embed).

-include("kura.hrl").

-export([fields/0]).

fields() ->
    [
        #kura_field{name = cover_url, type = string},
        #kura_field{name = minutes, type = integer}
    ].
