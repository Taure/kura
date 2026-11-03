-module(kura_test_hook_child_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, after_insert/1]).

table() -> ~"hook_children".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = parent_id, type = integer, nullable = false}
    ].

after_insert(#{name := ~"fail"}) -> {error, child_hook_failed};
after_insert(Record) -> {ok, Record}.
