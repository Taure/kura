-module(kura_bench_schema_user).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> <<"bench_users">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = email, type = string, nullable = false},
        #kura_field{name = age, type = integer},
        #kura_field{name = active, type = boolean, default = true},
        #kura_field{name = score, type = float, default = 0.0},
        #kura_field{name = role, type = {enum, [admin, user, moderator]}, default = user},
        #kura_field{name = metadata, type = jsonb},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

associations() ->
    [
        #kura_assoc{
            name = posts,
            type = has_many,
            schema = kura_bench_schema_post,
            foreign_key = user_id
        }
    ].
