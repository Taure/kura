-module(kura_test_audited_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).
-export([before_update/1, after_insert/1, after_update/1, after_delete/1]).

table() -> <<"audited_items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = value, type = integer},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

before_update(CS) ->
    kura_audit:stash(CS),
    {ok, CS}.

after_insert(Record) ->
    kura_audit:log(kura_test_repo, ?MODULE, insert, Record),
    {ok, Record}.

after_update(Record) ->
    kura_audit:log(kura_test_repo, ?MODULE, update, Record),
    {ok, Record}.

after_delete(Record) ->
    kura_audit:log(kura_test_repo, ?MODULE, delete, Record),
    ok.
