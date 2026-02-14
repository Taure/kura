-module(kura_test_comment).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, primary_key/0, associations/0]).

table() -> <<"comments">>.

primary_key() -> id.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = body, type = text, nullable = false},
        #kura_field{name = post_id, type = integer, nullable = false},
        #kura_field{name = author_id, type = integer, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{name = post, type = belongs_to, schema = kura_test_post, foreign_key = post_id},
        #kura_assoc{
            name = author, type = belongs_to, schema = kura_test_schema, foreign_key = author_id
        }
    ].
