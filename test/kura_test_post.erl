-module(kura_test_post).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, primary_key/0, associations/0]).

table() -> <<"posts">>.

primary_key() -> id.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = title, type = string, nullable = false},
        #kura_field{name = body, type = text},
        #kura_field{name = author_id, type = integer, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = author, type = belongs_to, schema = kura_test_schema, foreign_key = author_id
        },
        #kura_assoc{
            name = comments, type = has_many, schema = kura_test_comment, foreign_key = post_id
        }
    ].
