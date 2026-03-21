-module(kura_test_through_post).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"through_posts".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = user_id, type = integer, nullable = false},
        #kura_field{name = title, type = string, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = comments,
            type = has_many,
            schema = kura_test_through_comment,
            foreign_key = post_id
        }
    ].
