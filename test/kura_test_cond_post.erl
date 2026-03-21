-module(kura_test_cond_post).
-behaviour(kura_schema).
-include("kura.hrl").
-export([table/0, fields/0]).

table() -> ~"cond_posts".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = user_id, type = integer, nullable = false},
        #kura_field{name = title, type = string, nullable = false},
        #kura_field{name = published, type = boolean, default = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].
