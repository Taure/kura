-module(kura_audit_log).
-moduledoc "Schema for the audit_log table used by `kura_audit`.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> <<"audit_log">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = table_name, type = string, nullable = false},
        #kura_field{name = record_id, type = string, nullable = false},
        #kura_field{name = action, type = string, nullable = false},
        #kura_field{name = old_data, type = jsonb},
        #kura_field{name = new_data, type = jsonb},
        #kura_field{name = changes, type = jsonb},
        #kura_field{name = actor, type = string},
        #kura_field{name = metadata, type = jsonb},
        #kura_field{name = inserted_at, type = utc_datetime}
    ].

indexes() ->
    [
        {[table_name, record_id], #{}},
        {[action], #{}},
        {[actor], #{}},
        {[inserted_at], #{}}
    ].
