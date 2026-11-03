-module(kura_test_hook_parent_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"hook_parents".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false}
    ].

associations() ->
    [
        #kura_assoc{
            name = children,
            type = has_many,
            schema = kura_test_hook_child_schema,
            foreign_key = parent_id
        }
    ].
