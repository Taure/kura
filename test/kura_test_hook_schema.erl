-module(kura_test_hook_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).
-export([before_insert/1, after_insert/1]).
-export([before_update/1, after_update/1]).
-export([before_delete/1, after_delete/1]).

table() -> <<"hook_items">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = status, type = string},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

before_insert(CS) ->
    notify(before_insert, CS),
    case kura_changeset:get_change(CS, name) of
        <<"reject_insert">> ->
            {error, kura_changeset:add_error(CS, name, <<"insert rejected by hook">>)};
        _ ->
            {ok, kura_changeset:put_change(CS, status, <<"hook_inserted">>)}
    end.

after_insert(Record) ->
    notify(after_insert, Record),
    case maps:get(name, Record) of
        <<"fail_after_insert">> -> {error, after_insert_failed};
        _ -> {ok, Record}
    end.

before_update(CS) ->
    notify(before_update, CS),
    case kura_changeset:get_change(CS, name) of
        <<"reject_update">> ->
            {error, kura_changeset:add_error(CS, name, <<"update rejected by hook">>)};
        _ ->
            {ok, CS}
    end.

after_update(Record) ->
    notify(after_update, Record),
    case maps:get(name, Record) of
        <<"fail_after_update">> -> {error, after_update_failed};
        _ -> {ok, Record}
    end.

before_delete(Record) ->
    notify(before_delete, Record),
    case maps:get(name, Record) of
        <<"reject_delete">> -> {error, delete_rejected};
        _ -> ok
    end.

after_delete(Record) ->
    notify(after_delete, Record),
    case maps:get(name, Record) of
        <<"fail_after_delete">> -> {error, after_delete_failed};
        _ -> ok
    end.

notify(Hook, Data) ->
    case erlang:get(hook_test_pid) of
        Pid when is_pid(Pid) -> Pid ! {hook, Hook, Data};
        _ -> ok
    end.
