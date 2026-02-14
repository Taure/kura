-module(kura_repo_worker).
-moduledoc """
CRUD operations, preloading, transactions, and raw queries.

All functions take a repo module as the first argument and execute against
its connection pool.

```erlang
kura_repo_worker:start(MyRepo),
{ok, Users} = kura_repo_worker:all(MyRepo, kura_query:from(my_user)),
{ok, User} = kura_repo_worker:get(MyRepo, my_user, 1),
{ok, Inserted} = kura_repo_worker:insert(MyRepo, Changeset).
```
""".

-include("kura.hrl").

-export([
    start/1,
    all/2,
    get/3,
    get_by/3,
    one/2,
    insert/2,
    insert/3,
    update/2,
    delete/2,
    update_all/3,
    delete_all/2,
    insert_all/3,
    transaction/2,
    multi/2,
    preload/4,
    query/3,
    build_log_event/5,
    default_logger/0
]).

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

%%----------------------------------------------------------------------
%% Pool management
%%----------------------------------------------------------------------

-doc "Start the connection pool for the given repo module.".
-spec start(module()) -> ok.
start(RepoMod) ->
    Config = RepoMod:config(),
    Pool = maps:get(pool, Config, RepoMod),
    PgoConfig = #{
        host => binary_to_list(maps:get(hostname, Config, <<"localhost">>)),
        port => maps:get(port, Config, 5432),
        database => binary_to_list(maps:get(database, Config)),
        user => binary_to_list(maps:get(username, Config, <<"postgres">>)),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => maps:get(pool_size, Config, 10),
        decode_opts => ?DECODE_OPTS
    },
    case pgo:start_pool(Pool, PgoConfig) of
        {ok, _Pid} -> ok
    end.

-doc "Execute a query and return all matching rows.".
-spec all(module(), #kura_query{}) -> {ok, [map()]} | {error, term()}.
all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:to_sql(Query),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := select, rows := Rows} ->
            Schema = Query#kura_query.from,
            Loaded = [load_row(Schema, Row) || Row <- Rows],
            case Query#kura_query.preloads of
                [] -> {ok, Loaded};
                Preloads -> {ok, do_preload(RepoMod, Loaded, Schema, Preloads)}
            end;
        {error, _} = Err ->
            Err
    end.

-doc "Fetch a single record by primary key.".
-spec get(module(), module(), term()) -> {ok, map()} | {error, not_found} | {error, term()}.
get(RepoMod, SchemaMod, Id) ->
    PK = SchemaMod:primary_key(),
    Q = kura_query:where(kura_query:from(SchemaMod), {PK, Id}),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {error, _} = Err -> Err
    end.

-doc "Fetch a single record matching all key-value clauses.".
-spec get_by(module(), module(), [{atom(), term()}]) ->
    {ok, map()} | {error, not_found} | {error, term()}.
get_by(RepoMod, SchemaMod, Clauses) ->
    Q = lists:foldl(
        fun({Field, Value}, Acc) ->
            kura_query:where(Acc, {Field, Value})
        end,
        kura_query:from(SchemaMod),
        Clauses
    ),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {ok, [_ | _]} -> {error, multiple_results};
        {error, _} = Err -> Err
    end.

-doc "Execute a query and return exactly one result (adds LIMIT 1).".
-spec one(module(), #kura_query{}) -> {ok, map()} | {error, not_found} | {error, term()}.
one(RepoMod, Query) ->
    Q = kura_query:limit(Query, 1),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {error, _} = Err -> Err
    end.

-doc "Insert a record from a changeset. Returns `{error, Changeset}` with errors on failure.".
-spec insert(module(), #kura_changeset{}) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{assoc_changes = AC}) when map_size(AC) > 0 ->
    transaction(RepoMod, fun() ->
        case insert_record(RepoMod, CS) of
            {ok, ParentRow} ->
                persist_assoc_changes(RepoMod, CS#kura_changeset.schema, ParentRow, AC);
            {error, _} = Err ->
                Err
        end
    end);
insert(RepoMod, CS) ->
    insert_record(RepoMod, CS).

-spec insert(module(), #kura_changeset{}, map()) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}, _Opts) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}, Opts) ->
    Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
    Fields = maps:keys(Changes1),
    DumpedChanges = dump_changes(SchemaMod, Changes1),
    {SQL, Params} = kura_query_compiler:insert(SchemaMod, Fields, DumpedChanges, Opts),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        #{command := insert, rows := []} ->
            {ok, kura_changeset:apply_changes(CS)};
        {error, PgError} ->
            {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
    end.

-doc "Update a record from a changeset. No-op if there are no changes.".
-spec update(module(), #kura_changeset{}) -> {ok, map()} | {error, #kura_changeset{}}.
update(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = update}};
update(RepoMod, CS = #kura_changeset{assoc_changes = AC}) when map_size(AC) > 0 ->
    transaction(RepoMod, fun() ->
        case update_record(RepoMod, CS) of
            {ok, ParentRow} ->
                persist_assoc_changes(RepoMod, CS#kura_changeset.schema, ParentRow, AC);
            {error, _} = Err ->
                Err
        end
    end);
update(RepoMod, CS) ->
    update_record(RepoMod, CS).

-doc "Delete the record referenced by the changeset's data.".
-spec delete(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
delete(RepoMod, #kura_changeset{schema = SchemaMod, data = Data}) ->
    delete_record(RepoMod, SchemaMod, Data).

-doc "Bulk update all rows matching the query, returning the count of affected rows.".
-spec update_all(module(), #kura_query{}, map()) -> {ok, non_neg_integer()} | {error, term()}.
update_all(RepoMod, Query, Updates) ->
    {SQL, Params} = kura_query_compiler:update_all(Query, Updates),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := update, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Bulk delete all rows matching the query, returning the count of deleted rows.".
-spec delete_all(module(), #kura_query{}) -> {ok, non_neg_integer()} | {error, term()}.
delete_all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:delete_all(Query),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := delete, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Bulk insert a list of maps, returning the count of inserted rows.".
-spec insert_all(module(), module(), [map()]) -> {ok, non_neg_integer()} | {error, term()}.
insert_all(RepoMod, SchemaMod, Entries) ->
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    Rows = [
        begin
            WithTS = maybe_add_timestamps(SchemaMod, Entry, insert),
            Filtered = maps:with(NonVirtual, WithTS),
            dump_changes(SchemaMod, Filtered)
        end
     || Entry <- Entries
    ],
    Fields = maps:keys(hd(Rows)),
    {SQL, Params} = kura_query_compiler:insert_all(SchemaMod, Fields, Rows),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Execute a function inside a database transaction.".
-spec transaction(module(), fun(() -> any())) -> any() | {error, any()}.
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    pgo:transaction(Fun, #{pool => Pool}).

-doc "Execute a `kura_multi` pipeline inside a transaction.".
-spec multi(module(), term()) ->
    {ok, map()} | {error, atom(), term(), map()}.
multi(RepoMod, Multi) ->
    Ops = kura_multi:to_list(Multi),
    try
        transaction(RepoMod, fun() ->
            execute_multi_ops(RepoMod, Ops, #{})
        end)
    catch
        throw:{multi_error, Name, Value, Completed} ->
            {error, Name, Value, Completed}
    end.

-doc "Execute raw SQL with parameters.".
-spec query(module(), iodata(), [term()]) -> {ok, list()} | {error, term()}.
query(RepoMod, SQL, Params) ->
    case pgo_query(RepoMod, SQL, Params) of
        #{rows := Rows} -> {ok, Rows};
        {error, _} = Err -> Err
    end.

%%----------------------------------------------------------------------
%% Preloading
%%----------------------------------------------------------------------

-doc "Preload associations on one or more records (standalone, outside of queries).".
-spec preload(module(), module(), map() | [map()], [atom() | {atom(), list()}]) ->
    map() | [map()].
preload(RepoMod, Schema, Record, Assocs) when is_map(Record) ->
    [Result] = preload(RepoMod, Schema, [Record], Assocs),
    Result;
preload(_RepoMod, _Schema, [], _Assocs) ->
    [];
preload(RepoMod, Schema, Records, Assocs) when is_list(Records) ->
    do_preload(RepoMod, Records, Schema, Assocs).

do_preload(_RepoMod, Records, _Schema, []) ->
    Records;
do_preload(RepoMod, Records, Schema, [Assoc | Rest]) ->
    Updated = preload_assoc(RepoMod, Records, Schema, Assoc),
    do_preload(RepoMod, Updated, Schema, Rest).

preload_assoc(RepoMod, Records, Schema, {AssocName, Nested}) ->
    Records1 = preload_assoc(RepoMod, Records, Schema, AssocName),
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    RelatedSchema = Assoc#kura_assoc.schema,
    lists:map(
        fun(R) ->
            case maps:get(AssocName, R, undefined) of
                undefined ->
                    R;
                nil ->
                    R;
                Related when is_list(Related) ->
                    R#{AssocName => do_preload(RepoMod, Related, RelatedSchema, Nested)};
                Related when is_map(Related) ->
                    [Updated] = do_preload(RepoMod, [Related], RelatedSchema, Nested),
                    R#{AssocName => Updated}
            end
        end,
        Records1
    );
preload_assoc(RepoMod, Records, Schema, AssocName) when is_atom(AssocName) ->
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    case Assoc#kura_assoc.type of
        belongs_to -> preload_belongs_to(RepoMod, Records, Assoc);
        has_many -> preload_has_many(RepoMod, Records, Schema, Assoc);
        has_one -> preload_has_one(RepoMod, Records, Schema, Assoc)
    end.

preload_belongs_to(RepoMod, Records, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    FKValues = lists:usort([
        maps:get(FK, R)
     || R <- Records, maps:get(FK, R, undefined) =/= undefined
    ]),
    case FKValues of
        [] ->
            [R#{Name => nil} || R <- Records];
        _ ->
            RelPK = RelSchema:primary_key(),
            Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, FKValues}),
            {ok, Related} = all(RepoMod, Q),
            Lookup = maps:from_list([{maps:get(RelPK, Rel), Rel} || Rel <- Related]),
            [R#{Name => maps:get(maps:get(FK, R, undefined), Lookup, nil)} || R <- Records]
    end.

preload_has_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = all(RepoMod, Q),
    Grouped = lists:foldl(
        fun(Rel, Acc) ->
            Key = maps:get(FK, Rel),
            maps:update_with(Key, fun(L) -> L ++ [Rel] end, [Rel], Acc)
        end,
        #{},
        Related
    ),
    [R#{Name => maps:get(maps:get(PK, R), Grouped, [])} || R <- Records].

preload_has_one(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = all(RepoMod, Q),
    Lookup = maps:from_list([{maps:get(FK, Rel), Rel} || Rel <- Related]),
    [R#{Name => maps:get(maps:get(PK, R), Lookup, nil)} || R <- Records].

%%----------------------------------------------------------------------
%% Internal: multi execution
%%----------------------------------------------------------------------

execute_multi_ops(_RepoMod, [], Acc) ->
    {ok, Acc};
execute_multi_ops(RepoMod, [{Name, Op} | Rest], Acc) ->
    case execute_multi_op(RepoMod, Op, Acc) of
        {ok, Result} ->
            execute_multi_ops(RepoMod, Rest, Acc#{Name => Result});
        {error, Value} ->
            throw({multi_error, Name, Value, Acc})
    end.

execute_multi_op(RepoMod, {insert, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {insert, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {insert, CS}, _Acc) ->
    insert(RepoMod, CS);
execute_multi_op(RepoMod, {update, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {update, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {update, CS}, _Acc) ->
    update(RepoMod, CS);
execute_multi_op(RepoMod, {delete, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {delete, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {delete, CS}, _Acc) ->
    delete(RepoMod, CS);
execute_multi_op(_RepoMod, {run, Fun}, Acc) ->
    Fun(Acc).

%%----------------------------------------------------------------------
%% Internal: pgo bridge
%%----------------------------------------------------------------------

pgo_query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    T0 = erlang:monotonic_time(microsecond),
    Result = pgo:query(SQL, Params, #{pool => Pool, decode_opts => ?DECODE_OPTS}),
    T1 = erlang:monotonic_time(microsecond),
    emit_log(RepoMod, SQL, Params, Result, T1 - T0),
    Result.

emit_log(RepoMod, SQL, Params, Result, DurationUs) ->
    try
        case application:get_env(kura, log) of
            {ok, true} ->
                Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
                (default_logger())(Event);
            {ok, LogFun} when is_function(LogFun, 1) ->
                Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
                LogFun(Event);
            {ok, {M, F}} when is_atom(M), is_atom(F) ->
                Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
                M:F(Event);
            _ ->
                ok
        end
    catch
        _:_ -> ok
    end.

get_pool(RepoMod) ->
    Config = RepoMod:config(),
    maps:get(pool, Config, RepoMod).

insert_record(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}) ->
    Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
    Fields = maps:keys(Changes1),
    DumpedChanges = dump_changes(SchemaMod, Changes1),
    {SQL, Params} = kura_query_compiler:insert(SchemaMod, Fields, DumpedChanges),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        {error, PgError} ->
            {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
    end.

update_record(RepoMod, CS = #kura_changeset{schema = SchemaMod, data = Data, changes = Changes}) ->
    case maps:size(Changes) of
        0 ->
            {ok, Data};
        _ ->
            PK = SchemaMod:primary_key(),
            PKValue = maps:get(PK, Data),
            Changes1 = maybe_add_timestamps(SchemaMod, Changes, update),
            Fields = maps:keys(Changes1),
            DumpedChanges = dump_changes(SchemaMod, Changes1),
            {SQL, Params} = kura_query_compiler:update(
                SchemaMod, Fields, DumpedChanges, {PK, PKValue}
            ),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, load_row(SchemaMod, Row)};
                #{command := update, rows := []} ->
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, base, <<"record not found">>
                        )};
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = update}, PgError)}
            end
    end.

persist_assoc_changes(RepoMod, SchemaMod, ParentRow, AssocChanges) ->
    maps:fold(
        fun(AssocName, ChildCSs, {ok, AccRow}) ->
            {ok, Assoc} = kura_schema:association(SchemaMod, AssocName),
            FK = Assoc#kura_assoc.foreign_key,
            PK = SchemaMod:primary_key(),
            PKValue = maps:get(PK, AccRow),
            case is_list(ChildCSs) of
                true ->
                    Children = lists:map(
                        fun(ChildCS) ->
                            ChildCS1 = kura_changeset:put_change(ChildCS, FK, PKValue),
                            case ChildCS1#kura_changeset.action of
                                insert ->
                                    {ok, Child} = insert_record(RepoMod, ChildCS1),
                                    Child;
                                update ->
                                    {ok, Child} = update_record(RepoMod, ChildCS1),
                                    Child
                            end
                        end,
                        ChildCSs
                    ),
                    {ok, AccRow#{AssocName => Children}};
                false ->
                    ChildCS1 = kura_changeset:put_change(ChildCSs, FK, PKValue),
                    case ChildCSs#kura_changeset.action of
                        insert ->
                            {ok, Child} = insert_record(RepoMod, ChildCS1),
                            {ok, AccRow#{AssocName => Child}};
                        update ->
                            {ok, Child} = update_record(RepoMod, ChildCS1),
                            {ok, AccRow#{AssocName => Child}}
                    end
            end
        end,
        {ok, ParentRow},
        AssocChanges
    ).

delete_record(RepoMod, SchemaMod, Data) ->
    PK = SchemaMod:primary_key(),
    PKValue = maps:get(PK, Data),
    {SQL, Params} = kura_query_compiler:delete(SchemaMod, PK, PKValue),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := delete, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        #{command := delete, rows := []} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

load_row(SchemaMod, Row) when is_atom(SchemaMod) ->
    case code:ensure_loaded(SchemaMod) of
        {module, SchemaMod} ->
            case erlang:function_exported(SchemaMod, fields, 0) of
                true ->
                    Types = kura_schema:field_types(SchemaMod),
                    maps:fold(
                        fun(K, V, Acc) ->
                            case maps:find(K, Types) of
                                {ok, Type} ->
                                    case kura_types:load(Type, V) of
                                        {ok, Loaded} -> Acc#{K => Loaded};
                                        {error, _} -> Acc#{K => V}
                                    end;
                                error ->
                                    Acc#{K => V}
                            end
                        end,
                        #{},
                        Row
                    );
                false ->
                    Row
            end;
        _ ->
            Row
    end.

dump_changes(SchemaMod, Changes) ->
    Types = kura_schema:field_types(SchemaMod),
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    maps:fold(
        fun(K, V, Acc) ->
            case lists:member(K, NonVirtual) of
                false ->
                    Acc;
                true ->
                    case maps:find(K, Types) of
                        {ok, Type} ->
                            case kura_types:dump(Type, V) of
                                {ok, Dumped} -> Acc#{K => Dumped};
                                {error, _} -> Acc#{K => V}
                            end;
                        error ->
                            Acc#{K => V}
                    end
            end
        end,
        #{},
        Changes
    ).

maybe_add_timestamps(SchemaMod, Changes, Action) ->
    Types = kura_schema:field_types(SchemaMod),
    Now = calendar:universal_time(),
    Changes1 =
        case Action of
            insert ->
                case maps:is_key(inserted_at, Types) of
                    true -> Changes#{inserted_at => maps:get(inserted_at, Changes, Now)};
                    false -> Changes
                end;
            update ->
                Changes
        end,
    case maps:is_key(updated_at, Types) of
        true -> Changes1#{updated_at => maps:get(updated_at, Changes1, Now)};
        false -> Changes1
    end.

handle_pg_error(CS, #{code := Code, constraint := Constraint}) when
    Code =:= <<"23505">>; Code =:= <<"23503">>; Code =:= <<"23514">>
->
    DefaultType =
        case Code of
            <<"23505">> -> unique;
            <<"23503">> -> foreign_key;
            <<"23514">> -> check
        end,
    DefaultMsg =
        case DefaultType of
            unique -> <<"has already been taken">>;
            foreign_key -> <<"does not exist">>;
            check -> <<"is invalid">>
        end,
    case find_constraint(CS#kura_changeset.constraints, Constraint) of
        {ok, #kura_constraint{field = Field, message = Msg}} ->
            kura_changeset:add_error(CS, Field, Msg);
        error ->
            Field = constraint_to_field(Constraint),
            kura_changeset:add_error(CS, Field, DefaultMsg)
    end;
handle_pg_error(CS, #{code := <<"23502">>, column := Column}) ->
    Field = binary_to_atom(Column, utf8),
    kura_changeset:add_error(CS, Field, <<"can't be blank">>);
handle_pg_error(CS, Reason) ->
    kura_changeset:add_error(CS, base, format_error(Reason)).

find_constraint([], _Name) ->
    error;
find_constraint([C = #kura_constraint{constraint = Name} | _], Name) ->
    {ok, C};
find_constraint([_ | Rest], Name) ->
    find_constraint(Rest, Name).

constraint_to_field(Constraint) when is_binary(Constraint) ->
    case binary:split(Constraint, <<"_">>, [global]) of
        [_Table, Field | _Rest] ->
            try
                binary_to_existing_atom(Field, utf8)
            catch
                error:badarg -> binary_to_atom(Field, utf8)
            end;
        _ ->
            base
    end;
constraint_to_field(_) ->
    base.

format_error(#{message := Msg}) when is_binary(Msg) -> Msg;
format_error(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

-doc "Build a log event map from query execution data.".
-spec build_log_event(module(), iodata(), [term()], term(), integer()) -> map().
build_log_event(Repo, SQL, Params, Result, DurationUs) ->
    ResultStatus =
        case Result of
            {error, _} -> error;
            _ -> ok
        end,
    NumRows =
        case Result of
            #{num_rows := N} -> N;
            #{rows := Rows} -> length(Rows);
            _ -> 0
        end,
    #{
        query => iolist_to_binary([SQL]),
        params => Params,
        result => ResultStatus,
        num_rows => NumRows,
        duration_us => DurationUs,
        repo => Repo
    }.

-doc "Return a default logger function that logs queries via logger:info.".
-spec default_logger() -> fun((map()) -> ok).
default_logger() ->
    fun(Event) ->
        logger:info("Kura ~s ~pus", [
            maps:get(query, Event), maps:get(duration_us, Event)
        ])
    end.
