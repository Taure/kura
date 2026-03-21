-module(kura_test_through_user).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"through_users".

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
            type = has_many,
            schema = kura_test_through_post,
            foreign_key = user_id
        },
        #kura_assoc{
            name = comments,
            type = has_many,
            schema = kura_test_through_comment,
            through = [posts, comments]
        }
    ].
