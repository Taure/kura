-module(kura_bench_schema_post).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"bench_posts">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = title, type = string, nullable = false},
        #kura_field{name = body, type = text},
        #kura_field{name = published, type = boolean, default = false},
        #kura_field{name = view_count, type = integer, default = 0},
        #kura_field{name = rating, type = float, default = 0.0},
        #kura_field{name = user_id, type = integer, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = user,
            type = belongs_to,
            schema = kura_bench_schema_user,
            foreign_key = user_id
        },
        #kura_assoc{
            name = comments,
            type = has_many,
            schema = kura_bench_schema_comment,
            foreign_key = post_id
        }
    ].
