-module(kura_bench_schema_post_tag).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"bench_post_tags">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = post_id, type = integer, nullable = false},
        #kura_field{name = tag_id, type = integer, nullable = false}
    ].
