-module(kura_test_secret_team_schema).
-moduledoc "A schema with a redacted field and an association.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0, redact/0]).

table() -> ~"teams".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string},
        #kura_field{name = api_key, type = string}
    ].

associations() ->
    [
        #kura_assoc{
            name = members,
            type = has_many,
            schema = kura_test_secret_schema,
            foreign_key = team_id
        }
    ].

redact() -> [api_key].
