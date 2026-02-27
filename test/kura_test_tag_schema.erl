-module(kura_test_tag_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"tags">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = posts,
            type = many_to_many,
            schema = kura_test_post_schema,
            join_through = <<"posts_tags">>,
            join_keys = {tag_id, post_id}
        }
    ].
