-module(kura_audit).
-moduledoc """
Audit trail for Kura schema operations.

Tracks inserts, updates, and deletes in an `audit_log` table. Uses the
process dictionary for actor context (who made the change) and for stashing
old data during updates.

## Setup

Create the audit_log table via migration:

```erlang
up() -> kura_audit:migration_up().
down() -> kura_audit:migration_down().
```

## Usage in schema hooks

```erlang
-export([before_update/1, after_insert/1, after_update/1, after_delete/1]).

before_update(CS) ->
    kura_audit:stash(CS),
    {ok, CS}.

after_insert(Record) ->
    kura_audit:log(my_repo, ?MODULE, insert, Record),
    {ok, Record}.

after_update(Record) ->
    kura_audit:log(my_repo, ?MODULE, update, Record),
    {ok, Record}.

after_delete(Record) ->
    kura_audit:log(my_repo, ?MODULE, delete, Record),
    ok.
```

## Actor context

```erlang
kura_audit:set_actor(<<"user-123">>).
kura_audit:set_actor(<<"user-123">>, #{ip => <<"1.2.3.4">>}).
kura_audit:with_actor(<<"admin">>, fun() -> my_repo:delete(CS) end).
```
""".

-include("kura.hrl").

-export([
    set_actor/1,
    set_actor/2,
    get_actor/0,
    clear_actor/0,
    with_actor/2,
    with_actor/3,
    stash/1,
    log/4,
    migration_up/0,
    migration_down/0
]).

-ifdef(TEST).
-export([
    build_audit_data/3,
    compute_diff/2,
    filter_virtual/2,
    sanitize/1,
    sanitize_value/1,
    format_id/1,
    restore/2
]).
-endif.

%%----------------------------------------------------------------------
%% Actor context
%%----------------------------------------------------------------------

-doc "Set the audit actor for the current process.".
-spec set_actor(binary()) -> ok.
set_actor(ActorId) when is_binary(ActorId) ->
    erlang:put(kura_audit_actor, ActorId),
    erlang:put(kura_audit_metadata, undefined),
    ok.

-doc "Set the audit actor and metadata for the current process.".
-spec set_actor(binary(), map()) -> ok.
set_actor(ActorId, Metadata) when is_binary(ActorId), is_map(Metadata) ->
    erlang:put(kura_audit_actor, ActorId),
    erlang:put(kura_audit_metadata, Metadata),
    ok.

-doc "Get the current audit actor, or `undefined` if not set.".
-spec get_actor() -> binary() | undefined.
get_actor() ->
    case erlang:get(kura_audit_actor) of
        V when is_binary(V) -> V;
        _ -> undefined
    end.

-doc "Clear the audit actor from the current process.".
-spec clear_actor() -> ok.
clear_actor() ->
    erlang:erase(kura_audit_actor),
    erlang:erase(kura_audit_metadata),
    ok.

-doc "Execute a function with a temporary actor, restoring the previous actor after.".
-spec with_actor(binary(), fun(() -> T)) -> T when T :: term().
with_actor(ActorId, Fun) ->
    with_actor(ActorId, undefined, Fun).

-doc "Execute a function with a temporary actor and metadata.".
-spec with_actor(binary(), map() | undefined, fun(() -> T)) -> T when T :: term().
with_actor(ActorId, Metadata, Fun) ->
    OldActor = erlang:get(kura_audit_actor),
    OldMeta = erlang:get(kura_audit_metadata),
    erlang:put(kura_audit_actor, ActorId),
    erlang:put(kura_audit_metadata, Metadata),
    try
        Fun()
    after
        restore(kura_audit_actor, OldActor),
        restore(kura_audit_metadata, OldMeta)
    end.

%%----------------------------------------------------------------------
%% Stash (for capturing old data in before_update hooks)
%%----------------------------------------------------------------------

-doc """
Stash the changeset's current data for later diff computation.
Call this in `before_update/1` so that `log/4` can compute the changes.
""".
-spec stash(#kura_changeset{}) -> ok.
stash(#kura_changeset{schema = SchemaMod, data = Data}) ->
    erlang:put({kura_audit_stash, SchemaMod}, Data),
    ok.

%%----------------------------------------------------------------------
%% Logging
%%----------------------------------------------------------------------

-doc """
Write an audit log entry. Call from `after_insert/1`, `after_update/1`,
or `after_delete/1` hooks.

For updates, call `kura_audit:stash/1` in the corresponding `before_update/1`
hook to capture the old data for diff computation.
""".
-spec log(module(), module(), insert | update | delete, map()) -> ok.
log(Repo, SchemaMod, Action, Record) ->
    PK = kura_schema:primary_key(SchemaMod),
    RecordId = format_id(maps:get(PK, Record, undefined)),
    TableName = SchemaMod:table(),
    {OldData, NewData, Changes} = build_audit_data(SchemaMod, Action, Record),
    Actor = erlang:get(kura_audit_actor),
    Metadata = erlang:get(kura_audit_metadata),
    Entry = #{
        table_name => TableName,
        record_id => RecordId,
        action => atom_to_binary(Action, utf8),
        old_data => OldData,
        new_data => NewData,
        changes => Changes,
        actor => Actor,
        metadata => Metadata
    },
    CS = kura_changeset:cast(kura_audit_log, #{}, Entry, [
        table_name, record_id, action, old_data, new_data, changes, actor, metadata
    ]),
    CS1 = kura_changeset:validate_required(CS, [table_name, record_id, action]),
    {ok, _} = kura_repo_worker:insert(Repo, CS1),
    ok.

%%----------------------------------------------------------------------
%% Migration helpers
%%----------------------------------------------------------------------

-doc "Migration operations to create the audit_log table.".
-spec migration_up() -> [kura_migration:operation()].
migration_up() ->
    [
        {create_table, ~"audit_log", [
            #kura_column{name = id, type = id, primary_key = true},
            #kura_column{name = table_name, type = string, nullable = false},
            #kura_column{name = record_id, type = string, nullable = false},
            #kura_column{name = action, type = string, nullable = false},
            #kura_column{name = old_data, type = jsonb},
            #kura_column{name = new_data, type = jsonb},
            #kura_column{name = changes, type = jsonb},
            #kura_column{name = actor, type = string},
            #kura_column{name = metadata, type = jsonb},
            #kura_column{name = inserted_at, type = utc_datetime}
        ]},
        {create_index, ~"audit_log", [table_name, record_id], #{}},
        {create_index, ~"audit_log", [action], #{}},
        {create_index, ~"audit_log", [actor], #{}},
        {create_index, ~"audit_log", [inserted_at], #{}}
    ].

-doc "Migration operations to drop the audit_log table.".
-spec migration_down() -> [kura_migration:operation()].
migration_down() ->
    [{drop_table, ~"audit_log"}].

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

build_audit_data(SchemaMod, insert, Record) ->
    Filtered = sanitize(filter_virtual(SchemaMod, Record)),
    {undefined, Filtered, undefined};
build_audit_data(SchemaMod, delete, Record) ->
    Filtered = sanitize(filter_virtual(SchemaMod, Record)),
    {Filtered, undefined, undefined};
build_audit_data(SchemaMod, update, NewRecord) ->
    case erlang:erase({kura_audit_stash, SchemaMod}) of
        undefined ->
            Filtered = sanitize(filter_virtual(SchemaMod, NewRecord)),
            {undefined, Filtered, undefined};
        OldData ->
            OldFiltered = sanitize(filter_virtual(SchemaMod, OldData)),
            NewFiltered = sanitize(filter_virtual(SchemaMod, NewRecord)),
            Changes = compute_diff(OldFiltered, NewFiltered),
            {OldFiltered, NewFiltered, Changes}
    end.

compute_diff(Old, New) ->
    maps:fold(
        fun(K, NewVal, Acc) ->
            case maps:get(K, Old, undefined) of
                NewVal -> Acc;
                OldVal -> Acc#{K => #{old => OldVal, new => NewVal}}
            end
        end,
        #{},
        New
    ).

filter_virtual(SchemaMod, Record) ->
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    maps:with(NonVirtual, Record).

sanitize(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> sanitize_value(V) end, Map).

sanitize_value({{Y, M, D}, {H, Mi, S}}) when
    is_integer(Y),
    is_integer(M),
    is_integer(D),
    is_integer(H),
    is_integer(Mi),
    is_integer(S)
->
    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ", [Y, M, D, H, Mi, S])
    );
sanitize_value({Y, M, D}) when is_integer(Y), is_integer(M), is_integer(D) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]));
sanitize_value(V) when is_atom(V), V =/= true, V =/= false, V =/= null, V =/= undefined ->
    atom_to_binary(V, utf8);
sanitize_value(undefined) ->
    null;
sanitize_value(M) when is_map(M) ->
    sanitize(M);
sanitize_value(L) when is_list(L) ->
    [sanitize_value(V) || V <- L];
sanitize_value(V) ->
    V.

format_id(undefined) -> ~"unknown";
format_id(Id) when is_integer(Id) -> integer_to_binary(Id);
format_id(Id) when is_binary(Id) -> Id;
format_id(Id) -> iolist_to_binary(io_lib:format("~p", [Id])).

restore(Key, undefined) -> erlang:erase(Key);
restore(Key, Value) -> erlang:put(Key, Value).
