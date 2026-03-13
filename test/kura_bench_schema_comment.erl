-module(kura_bench_schema_comment).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"bench_comments">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = body, type = text, nullable = false},
        #kura_field{name = post_id, type = integer, nullable = false},
        #kura_field{name = user_id, type = integer, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = post,
            type = belongs_to,
            schema = kura_bench_schema_post,
            foreign_key = post_id
        },
        #kura_assoc{
            name = user,
            type = belongs_to,
            schema = kura_bench_schema_user,
            foreign_key = user_id
        }
    ].
