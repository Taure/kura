-module(kura_test_stray_assoc_schema).
-moduledoc "A schema whose association names a module that is no schema.".
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"stray_assocs".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true}].

associations() ->
    [#kura_assoc{name = plain, type = has_one, schema = lists, foreign_key = stray_id}].
