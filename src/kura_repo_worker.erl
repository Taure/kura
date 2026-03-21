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
    insert_all/4,
    exists/2,
    reload/3,
    aggregate/3,
    aggregate/4,
    count/2,
    transaction/2,
    multi/2,
    preload/4,
    query/3,
    pgo_query/3,
    build_log_event/5,
    build_telemetry_metadata/4,
    extract_source/1,
    default_logger/0
]).

-ifdef(TEST).
-export([
    narrow_ok_error/1,
    extract_result_status/1,
    extract_num_rows/1
]).
-endif.

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

%% eqWAlizer: pgo:transaction/2 returns any() — no override module available
-eqwalizer({nowarn_function, insert/2}).
-eqwalizer({nowarn_function, update/2}).
-eqwalizer({nowarn_function, delete/2}).
-eqwalizer({nowarn_function, multi/2}).
-eqwalizer({nowarn_function, persist_many_to_many/6}).

%%----------------------------------------------------------------------
%% Pool management
%%----------------------------------------------------------------------

-doc "Start the connection pool for the given repo module.".
-spec start(module()) -> ok.
start(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    Pool = maps:get(pool, Config, RepoMod),
    PgoConfig = #{
        host => binary_to_list(maps:get(hostname, Config, ~"localhost")),
        port => maps:get(port, Config, 5432),
        database => binary_to_list(maps:get(database, Config)),
        user => binary_to_list(maps:get(username, Config, ~"postgres")),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => maps:get(pool_size, Config, 10),
        decode_opts => ?DECODE_OPTS
    },
    %% pgo spec says {ok, pid()} but supervisor:start_child underneath
    %% can return {error, {already_started, Pid}} when pool exists.
    case pgo_sup:start_child(Pool, PgoConfig) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

-doc "Execute a query and return all matching rows.".
-spec all(module(), #kura_query{}) -> {ok, [map()]} | {error, term()}.
all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:to_sql(maybe_apply_tenant_query(Query)),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := select, rows := Rows} ->
            Schema = Query#kura_query.from,
            Loaded = [load_row(Schema, Row) || Row <- Rows],
            case Query#kura_query.preloads of
                [] -> {ok, Loaded};
                Preloads -> {ok, kura_preloader:do_preload(RepoMod, Loaded, Schema, Preloads)}
            end;
        {error, _} = Err ->
            Err
    end.

-doc "Fetch a single record by primary key.".
-spec get(module(), module(), term()) -> {ok, map()} | {error, not_found} | {error, term()}.
get(RepoMod, SchemaMod, Id) ->
    PK = kura_schema:primary_key(SchemaMod),
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
    Q = apply_clauses(kura_query:from(SchemaMod), Clauses),
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

-doc "Check if any record matches the query.".
-spec exists(module(), #kura_query{}) -> {ok, boolean()} | {error, term()}.
exists(RepoMod, Query) ->
    Q = kura_query:limit(Query, 1),
    case all(RepoMod, Q) of
        {ok, []} -> {ok, false};
        {ok, [_ | _]} -> {ok, true};
        {error, _} = Err -> Err
    end.

-doc "Re-fetch a record from the database by its primary key.".
-spec reload(module(), module(), map()) -> {ok, map()} | {error, term()}.
reload(RepoMod, SchemaMod, Record) ->
    PK = kura_schema:primary_key(SchemaMod),
    case Record of
        #{PK := Id} -> get(RepoMod, SchemaMod, Id);
        #{} -> {error, no_primary_key}
    end.

-doc """
Run an aggregate function on a query. Supported: `count`, `sum`, `avg`, `min`, `max`.

```erlang
{ok, 42} = kura_repo_worker:aggregate(MyRepo, Q, count).
{ok, 150.5} = kura_repo_worker:aggregate(MyRepo, Q, {sum, score}).
```
""".
-spec aggregate(module(), #kura_query{}, count | {count | sum | avg | min | max, atom()}) ->
    {ok, term()} | {error, term()}.
aggregate(RepoMod, Query, count) ->
    aggregate(RepoMod, Query, {count, '*'});
aggregate(RepoMod, Query, {Agg, Field}) ->
    AggQ = Query#kura_query{
        select = [{Agg, Field}],
        order_bys = [],
        limit = undefined,
        offset = undefined,
        preloads = [],
        distinct = false
    },
    {SQL, Params} = kura_query_compiler:to_sql(AggQ),
    case pgo_query(RepoMod, SQL, Params) of
        #{rows := [Row]} -> {ok, maps:get(Agg, Row)};
        #{rows := []} -> {ok, nil};
        {error, _} = Err -> Err
    end.

-doc """
Run an aggregate with a default value when the result is nil/null.

```erlang
{ok, 0} = kura_repo_worker:aggregate(MyRepo, Q, {sum, score}, 0).
```
""".
-spec aggregate(module(), #kura_query{}, count | {count | sum | avg | min | max, atom()}, term()) ->
    {ok, term()} | {error, term()}.
aggregate(RepoMod, Query, Agg, Default) ->
    case aggregate(RepoMod, Query, Agg) of
        {ok, nil} -> {ok, Default};
        {ok, null} -> {ok, Default};
        {ok, undefined} -> {ok, Default};
        Other -> Other
    end.

-doc """
Count all rows matching a query.

```erlang
{ok, 42} = kura_repo_worker:count(MyRepo, kura_query:from(my_schema)).
```
""".
-spec count(module(), #kura_query{}) -> {ok, non_neg_integer()} | {error, term()}.
count(RepoMod, Query) ->
    case aggregate(RepoMod, Query, count) of
        {ok, N} when is_integer(N) -> {ok, N};
        {error, _} = Err -> Err
    end.

-doc "Insert a record from a changeset. Returns `{error, Changeset}` with errors on failure.".
-spec insert(module(), #kura_changeset{}) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, assoc_changes = AC}) when
    map_size(AC) > 0
->
    narrow_ok_error(
        transaction(RepoMod, fun() ->
            case insert_record(RepoMod, CS) of
                {ok, ParentRow} ->
                    persist_assoc_changes(RepoMod, SchemaMod, ParentRow, AC);
                {error, _} = Err ->
                    Err
            end
        end)
    );
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case needs_transaction(SchemaMod, insert) of
        true ->
            try
                narrow_ok_error(
                    transaction(RepoMod, fun() ->
                        case insert_record(RepoMod, CS) of
                            {ok, _} = Ok -> Ok;
                            {error, Reason} -> throw({kura_rollback, Reason})
                        end
                    end)
                )
            catch
                throw:{kura_rollback, Reason} -> {error, Reason}
            end;
        false ->
            insert_record(RepoMod, CS)
    end.

-spec insert(module(), #kura_changeset{}, map()) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}, _Opts) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}, Opts) ->
    Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
    DumpedChanges = dump_changes(SchemaMod, Changes1),
    Fields = maps:keys(DumpedChanges),
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
update(RepoMod, CS = #kura_changeset{schema = SchemaMod, assoc_changes = AC}) when
    map_size(AC) > 0
->
    narrow_ok_error(
        transaction(RepoMod, fun() ->
            case update_record(RepoMod, CS) of
                {ok, ParentRow} ->
                    persist_assoc_changes(RepoMod, SchemaMod, ParentRow, AC);
                {error, _} = Err ->
                    Err
            end
        end)
    );
update(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case needs_transaction(SchemaMod, update) of
        true ->
            try
                narrow_ok_error(
                    transaction(RepoMod, fun() ->
                        case update_record(RepoMod, CS) of
                            {ok, _} = Ok -> Ok;
                            {error, Reason} -> throw({kura_rollback, Reason})
                        end
                    end)
                )
            catch
                throw:{kura_rollback, Reason} -> {error, Reason}
            end;
        false ->
            update_record(RepoMod, CS)
    end.

-doc "Delete the record referenced by the changeset's data.".
-spec delete(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
delete(RepoMod, #kura_changeset{schema = SchemaMod, data = Data}) ->
    case needs_transaction(SchemaMod, delete) of
        true ->
            try
                narrow_ok_error(
                    transaction(RepoMod, fun() ->
                        case delete_record(RepoMod, SchemaMod, Data) of
                            {ok, _} = Ok -> Ok;
                            {error, Reason} -> throw({kura_rollback, Reason})
                        end
                    end)
                )
            catch
                throw:{kura_rollback, Reason} -> {error, Reason}
            end;
        false ->
            delete_record(RepoMod, SchemaMod, Data)
    end.

-doc "Bulk update all rows matching the query, returning the count of affected rows.".
-spec update_all(module(), #kura_query{}, map()) -> {ok, non_neg_integer()} | {error, term()}.
update_all(RepoMod, Query, Updates) ->
    {SQL, Params} = kura_query_compiler:update_all(maybe_apply_tenant_query(Query), Updates),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := update, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Bulk delete all rows matching the query, returning the count of deleted rows.".
-spec delete_all(module(), #kura_query{}) -> {ok, non_neg_integer()} | {error, term()}.
delete_all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:delete_all(maybe_apply_tenant_query(Query)),

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
    [First | _] = Rows,
    Fields = maps:keys(First),
    {SQL, Params} = kura_query_compiler:insert_all(SchemaMod, Fields, Rows),

    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Bulk insert with options. `#{returning => true | [atom()]}` returns inserted rows.".
-spec insert_all(module(), module(), [map()], map()) ->
    {ok, non_neg_integer()} | {ok, non_neg_integer(), [map()]} | {error, term()}.
insert_all(RepoMod, SchemaMod, Entries, Opts) ->
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    Rows = [
        begin
            WithTS = maybe_add_timestamps(SchemaMod, Entry, insert),
            Filtered = maps:with(NonVirtual, WithTS),
            dump_changes(SchemaMod, Filtered)
        end
     || Entry <- Entries
    ],
    [First | _] = Rows,
    Fields = maps:keys(First),
    {SQL, Params} = kura_query_compiler:insert_all(SchemaMod, Fields, Rows, Opts),

    case Opts of
        #{returning := RetOpt} when RetOpt =:= true; is_list(RetOpt) ->
            case pgo_query(RepoMod, SQL, Params) of
                #{command := insert, num_rows := Count, rows := RetRows} ->
                    Loaded = [load_row(SchemaMod, R) || R <- RetRows],
                    {ok, Count, Loaded};
                {error, _} = Err ->
                    Err
            end;
        #{} ->
            case pgo_query(RepoMod, SQL, Params) of
                #{command := insert, num_rows := Count} -> {ok, Count};
                {error, _} = Err -> Err
            end
    end.

-doc "Execute a function inside a database transaction.".
-spec transaction(module(), fun(() -> term())) -> term().
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    case kura_sandbox:get_conn(Pool) of
        {ok, _Conn} ->
            Fun();
        not_found ->
            pgo:transaction(Fun, #{pool => Pool})
    end.

-doc "Execute a `kura_multi` pipeline inside a transaction.".
-spec multi(module(), term()) ->
    {ok, map()} | {error, atom(), term(), map()}.
multi(RepoMod, Multi) ->
    Ops = kura_multi:to_list(Multi),
    try
        narrow_ok_error(
            transaction(RepoMod, fun() ->
                execute_multi_ops(RepoMod, Ops, #{})
            end)
        )
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
%% Preloading (delegated to kura_preloader)
%%----------------------------------------------------------------------

-doc "Preload associations on one or more records (standalone, outside of queries).".
-spec preload(module(), module(), map() | [map()], [atom() | {atom(), list()}]) ->
    map() | [map()].
preload(RepoMod, Schema, Records, Assocs) ->
    kura_preloader:preload(RepoMod, Schema, Records, Assocs).

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
    T0 = erlang:monotonic_time(),
    Result =
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                pgo:query(SQL, Params, #{decode_opts => ?DECODE_OPTS}, Conn);
            not_found ->
                pgo:query(SQL, Params, #{pool => Pool, decode_opts => ?DECODE_OPTS})
        end,
    T1 = erlang:monotonic_time(),
    DurationNative = T1 - T0,
    DurationUs = erlang:convert_time_unit(DurationNative, native, microsecond),
    emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs),
    Result.

emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs) ->
    try
        Measurements = #{
            duration => DurationNative,
            duration_us => DurationUs
        },
        Metadata = build_telemetry_metadata(RepoMod, SQL, Params, Result),
        telemetry:execute([kura, repo, query], Measurements, Metadata),
        emit_legacy_log(RepoMod, SQL, Params, Result, DurationUs)
    catch
        _:_ -> ok
    end.

emit_legacy_log(RepoMod, SQL, Params, Result, DurationUs) ->
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
    end.

build_telemetry_metadata(RepoMod, SQL, Params, Result) ->
    SQLBin = iolist_to_binary(SQL),
    {ResultStatus, NumRows} =
        case Result of
            #{rows := Rows} -> {ok, length(Rows)};
            #{num_rows := N} -> {ok, N};
            {error, _} -> {error, 0};
            _ -> {ok, 0}
        end,
    Source = extract_source(SQLBin),
    #{
        query => SQLBin,
        params => Params,
        repo => RepoMod,
        result => ResultStatus,
        num_rows => NumRows,
        source => Source
    }.

extract_source(SQL) ->
    case
        re:run(SQL, ~"(?:FROM|INTO|UPDATE|TABLE)\\s+\"?(\\w+)\"?", [
            {capture, [1], binary}, caseless
        ])
    of
        {match, [Table]} -> Table;
        nomatch -> undefined
    end.

get_pool(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    maps:get(pool, Config, RepoMod).

insert_record(RepoMod, CS0 = #kura_changeset{schema = SchemaMod}) ->
    CS1 = run_prepare(CS0),
    case kura_schema:run_before_insert(SchemaMod, CS1) of
        {error, ErrCS} ->
            {error, ErrCS#kura_changeset{action = insert}};
        {ok, CS} ->
            Changes = maybe_apply_tenant_changes(CS#kura_changeset.changes),
            Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
            DumpedChanges = dump_changes(SchemaMod, Changes1),
            Fields = maps:keys(DumpedChanges),
            {SQL, Params} = kura_query_compiler:insert(SchemaMod, Fields, DumpedChanges),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := insert, rows := [Row]} ->
                    Row1 = load_row(SchemaMod, Row),
                    kura_schema:run_after_insert(SchemaMod, Row1);
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
            end
    end.

update_record(RepoMod, CS0 = #kura_changeset{schema = SchemaMod, data = Data}) ->
    CS1 = run_prepare(CS0),
    case kura_schema:run_before_update(SchemaMod, CS1) of
        {error, ErrCS} ->
            {error, ErrCS#kura_changeset{action = update}};
        {ok, CS} ->
            Changes = CS#kura_changeset.changes,
            case maps:size(Changes) of
                0 ->
                    {ok, Data};
                _ ->
                    PK = kura_schema:primary_key(SchemaMod),
                    PKValue = maps:get(PK, Data),
                    Changes1 = maybe_add_timestamps(SchemaMod, Changes, update),
                    DumpedChanges = dump_changes(SchemaMod, Changes1),
                    Fields = maps:keys(DumpedChanges),
                    case
                        do_update_query(RepoMod, CS, SchemaMod, Fields, DumpedChanges, PK, PKValue)
                    of
                        {ok, Row} ->
                            kura_schema:run_after_update(SchemaMod, Row);
                        {error, _} = Err ->
                            Err
                    end
            end
    end.

do_update_query(RepoMod, CS, SchemaMod, Fields, DumpedChanges, PK, PKValue) ->
    case CS#kura_changeset.optimistic_lock of
        undefined ->
            {SQL, Params} = kura_query_compiler:update(
                SchemaMod, Fields, DumpedChanges, {PK, PKValue}
            ),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, load_row(SchemaMod, Row)};
                #{command := update, rows := []} ->
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update},
                            base,
                            ~"record not found"
                        )};
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = update}, PgError)}
            end;
        LockField ->
            Data = CS#kura_changeset.data,
            LockValue = maps:get(LockField, Data, 0),
            {SQL, Params} = compile_update_with_lock(
                SchemaMod,
                Fields,
                DumpedChanges,
                {PK, PKValue},
                {LockField, LockValue}
            ),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, load_row(SchemaMod, Row)};
                #{command := update, rows := []} ->
                    {error, stale};
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = update}, PgError)}
            end
    end.

run_prepare(CS = #kura_changeset{prepare = []}) ->
    CS;
run_prepare(CS = #kura_changeset{prepare = Funs}) ->
    apply_prepare_funs(CS, Funs).

-spec apply_prepare_funs(#kura_changeset{}, [fun((#kura_changeset{}) -> #kura_changeset{})]) ->
    #kura_changeset{}.
apply_prepare_funs(CS, []) ->
    CS;
apply_prepare_funs(CS, [Fun | Rest]) ->
    apply_prepare_funs(Fun(CS), Rest).

compile_update_with_lock(
    SchemaOrTable, Fields, Changes, {PKField, PKValue}, {LockField, LockValue}
) ->
    Table = resolve_table_name(SchemaOrTable),
    {Sets, Params, Counter} = build_lock_set_parts(Fields, Changes, 1),
    PKPlaceholder = [~"$", integer_to_binary(Counter)],
    LockPlaceholder = [~"$", integer_to_binary(Counter + 1)],
    SQL = iolist_to_binary([
        ~"UPDATE ",
        quote_ident_bin(Table),
        ~" SET ",
        join_comma_bin(Sets),
        ~" WHERE ",
        quote_ident_bin(atom_to_binary(PKField, utf8)),
        ~" = ",
        PKPlaceholder,
        ~" AND ",
        quote_ident_bin(atom_to_binary(LockField, utf8)),
        ~" = ",
        LockPlaceholder,
        ~" RETURNING *"
    ]),
    {SQL, Params ++ [PKValue, LockValue]}.

resolve_table_name(Mod) when is_atom(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            case erlang:function_exported(Mod, table, 0) of
                true -> Mod:table();
                false -> atom_to_binary(Mod, utf8)
            end;
        _ ->
            atom_to_binary(Mod, utf8)
    end.

quote_ident_bin(Name) when is_binary(Name) ->
    <<$", Name/binary, $">>.

-spec apply_clauses(#kura_query{}, [{atom(), term()}]) -> #kura_query{}.
apply_clauses(Q, []) ->
    Q;
apply_clauses(Q, [{Field, Value} | Rest]) ->
    apply_clauses(kura_query:where(Q, {Field, Value}), Rest).

-spec build_lock_set_parts([atom()], map(), pos_integer()) ->
    {[iodata()], [term()], pos_integer()}.
build_lock_set_parts([], _Changes, N) ->
    {[], [], N};
build_lock_set_parts([Field | Rest], Changes, N) ->
    Value = maps:get(Field, Changes),
    Set = [quote_ident_bin(atom_to_binary(Field, utf8)), ~" = $", integer_to_binary(N)],
    {Sets, Params, N2} = build_lock_set_parts(Rest, Changes, N + 1),
    {[Set | Sets], [Value | Params], N2}.

join_comma_bin(Parts) ->
    lists:join(~", ", Parts).

persist_assoc_changes(RepoMod, SchemaMod, ParentRow, AssocChanges) ->
    maps:fold(
        fun(AssocName, ChildCSs, {ok, AccRow}) ->
            {ok, Assoc} = kura_schema:association(SchemaMod, AssocName),
            case Assoc#kura_assoc.type of
                many_to_many ->
                    persist_many_to_many(RepoMod, SchemaMod, AccRow, AssocName, Assoc, ChildCSs);
                _ ->
                    persist_owned_assoc(RepoMod, SchemaMod, AccRow, AssocName, Assoc, ChildCSs)
            end
        end,
        {ok, ParentRow},
        AssocChanges
    ).

-spec persist_owned_assoc(
    module(),
    module(),
    map(),
    atom(),
    #kura_assoc{},
    #kura_changeset{} | [#kura_changeset{}]
) -> {ok, map()}.
persist_owned_assoc(RepoMod, SchemaMod, AccRow, AssocName, Assoc, ChildCSs) when
    is_list(ChildCSs)
->
    FK = Assoc#kura_assoc.foreign_key,
    PK = kura_schema:primary_key(SchemaMod),
    PKValue = maps:get(PK, AccRow),
    Children = [
        persist_child(RepoMod, kura_changeset:put_change(ChildCS, FK, PKValue))
     || ChildCS <- ChildCSs
    ],
    {ok, AccRow#{AssocName => Children}};
persist_owned_assoc(RepoMod, SchemaMod, AccRow, AssocName, Assoc, ChildCS) ->
    FK = Assoc#kura_assoc.foreign_key,
    PK = kura_schema:primary_key(SchemaMod),
    PKValue = maps:get(PK, AccRow),
    Child = persist_child(RepoMod, kura_changeset:put_change(ChildCS, FK, PKValue)),
    {ok, AccRow#{AssocName => Child}}.

persist_child(RepoMod, CS = #kura_changeset{action = insert}) ->
    {ok, Child} = insert_record(RepoMod, CS),
    Child;
persist_child(RepoMod, CS = #kura_changeset{action = update}) ->
    {ok, Child} = update_record(RepoMod, CS),
    Child.

persist_m2m_child(RepoMod, CS = #kura_changeset{action = insert}) ->
    {ok, Child} = insert_record(RepoMod, CS),
    Child;
persist_m2m_child(RepoMod, CS = #kura_changeset{action = update}) ->
    {ok, Child} = update_record(RepoMod, CS),
    Child;
persist_m2m_child(_RepoMod, CS = #kura_changeset{action = undefined}) ->
    kura_changeset:apply_changes(CS).

-spec persist_many_to_many(
    module(),
    module(),
    map(),
    atom(),
    #kura_assoc{},
    [#kura_changeset{}]
) -> {ok, map()}.
persist_many_to_many(RepoMod, SchemaMod, AccRow, AssocName, Assoc, ChildCSs) ->
    #kura_assoc{
        schema = RelSchema,
        join_through = JoinThrough,
        join_keys = {OwnerKey, RelatedKey}
    } = Assoc,
    PK = kura_schema:primary_key(SchemaMod),
    PKValue = maps:get(PK, AccRow),
    RelPK = kura_schema:primary_key(RelSchema),
    JoinTable =
        case JoinThrough of
            B when is_binary(B) -> B;
            M when is_atom(M) -> M:table()
        end,
    OwnerCol = atom_to_binary(OwnerKey, utf8),
    RelatedCol = atom_to_binary(RelatedKey, utf8),
    Children = [persist_m2m_child(RepoMod, ChildCS) || ChildCS <- ChildCSs],
    DeleteSQL = iolist_to_binary([
        ~"DELETE FROM ",
        JoinTable,
        ~" WHERE ",
        OwnerCol,
        ~" = $1"
    ]),
    _ = pgo_query(RepoMod, DeleteSQL, [PKValue]),
    lists:foreach(
        fun(Child) ->
            RelPKValue = maps:get(RelPK, Child),
            InsertSQL = iolist_to_binary([
                ~"INSERT INTO ",
                JoinTable,
                ~" (",
                OwnerCol,
                ~", ",
                RelatedCol,
                ~") VALUES ($1, $2)"
            ]),
            pgo_query(RepoMod, InsertSQL, [PKValue, RelPKValue])
        end,
        Children
    ),
    {ok, AccRow#{AssocName => Children}}.

delete_record(RepoMod, SchemaMod, Data) ->
    case kura_schema:run_before_delete(SchemaMod, Data) of
        {error, _} = Err ->
            Err;
        ok ->
            PK = kura_schema:primary_key(SchemaMod),
            PKValue = maps:get(PK, Data),
            {SQL, Params} = kura_query_compiler:delete(SchemaMod, PK, PKValue),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := delete, rows := [Row]} ->
                    Row1 = load_row(SchemaMod, Row),
                    case kura_schema:run_after_delete(SchemaMod, Row1) of
                        ok -> {ok, Row1};
                        {error, _} = AfterErr -> AfterErr
                    end;
                #{command := delete, rows := []} ->
                    {error, not_found};
                {error, _} = Err ->
                    Err
            end
    end.

load_row(SchemaMod, Row) when is_atom(SchemaMod) ->
    case code:ensure_loaded(SchemaMod) of
        {module, SchemaMod} ->
            case erlang:function_exported(SchemaMod, fields, 0) of
                true ->
                    Types = kura_schema:field_types(SchemaMod),
                    maps:fold(
                        fun(K, V, Acc) ->
                            case Types of
                                #{K := Type} ->
                                    case kura_types:load(Type, V) of
                                        {ok, Loaded} -> Acc#{K => Loaded};
                                        {error, _} -> Acc#{K => V}
                                    end;
                                #{} ->
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
    dump_change_fields(maps:to_list(Changes), Types, NonVirtual).

dump_change_fields([], _Types, _NonVirtual) ->
    #{};
dump_change_fields([{K, V} | Rest], Types, NonVirtual) ->
    Acc = dump_change_fields(Rest, Types, NonVirtual),
    case {lists:member(K, NonVirtual), Types} of
        {false, _} ->
            Acc;
        {true, #{K := Type}} ->
            case kura_types:dump(Type, V) of
                {ok, Dumped} -> Acc#{K => Dumped};
                {error, _} -> Acc#{K => V}
            end;
        {true, #{}} ->
            Acc#{K => V}
    end.

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

handle_pg_error(CS, {pgsql_error, Fields}) when is_map(Fields) ->
    handle_pg_error(CS, Fields);
handle_pg_error(CS, {pgsql_error, Fields}) when is_list(Fields) ->
    handle_pg_error(CS, maps:from_list(Fields));
handle_pg_error(CS, #{code := Code, constraint := Constraint}) when
    Code =:= ~"23505"; Code =:= ~"23503"; Code =:= ~"23514"
->
    DefaultType =
        case Code of
            ~"23505" -> unique;
            ~"23503" -> foreign_key;
            ~"23514" -> check
        end,
    DefaultMsg =
        case DefaultType of
            unique -> ~"has already been taken";
            foreign_key -> ~"does not exist";
            check -> ~"is invalid"
        end,
    case find_constraint(CS#kura_changeset.constraints, Constraint) of
        {ok, #kura_constraint{field = Field, message = Msg}} ->
            kura_changeset:add_error(CS, Field, Msg);
        error ->
            Field = constraint_to_field(Constraint),
            kura_changeset:add_error(CS, Field, DefaultMsg)
    end;
handle_pg_error(CS, #{code := ~"23502", column := Column}) ->
    Field =
        try
            binary_to_existing_atom(Column, utf8)
        catch
            error:badarg -> base
        end,
    kura_changeset:add_error(CS, Field, ~"can't be blank");
handle_pg_error(CS, Reason) ->
    kura_changeset:add_error(CS, base, format_error(Reason)).

find_constraint([], _Name) ->
    error;
find_constraint([C = #kura_constraint{constraint = Name} | _], Name) ->
    {ok, C};
find_constraint([_ | Rest], Name) ->
    find_constraint(Rest, Name).

constraint_to_field(Constraint) when is_binary(Constraint) ->
    case binary:split(Constraint, ~"_", [global]) of
        [_Table, Field | _Rest] ->
            try
                binary_to_existing_atom(Field, utf8)
            catch
                error:badarg -> base
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
    ResultStatus = extract_result_status(Result),
    NumRows = extract_num_rows(Result),
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

%%----------------------------------------------------------------------
%% Internal: multitenancy
%%----------------------------------------------------------------------

needs_transaction(undefined, _Action) ->
    false;
needs_transaction(SchemaMod, Action) ->
    kura_schema:has_after_hook(SchemaMod, Action).

maybe_apply_tenant_query(Query) ->
    case kura_tenant:get_tenant() of
        {prefix, Prefix} ->
            case Query#kura_query.prefix of
                undefined -> Query#kura_query{prefix = Prefix};
                _ -> Query
            end;
        {attribute, {Field, Value}} ->
            kura_query:where(Query, {Field, Value});
        undefined ->
            Query
    end.

maybe_apply_tenant_changes(Changes) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, Value}} ->
            Changes#{Field => Value};
        _ ->
            Changes
    end.

%%----------------------------------------------------------------------
%% Internal: type narrowing helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec narrow_ok_error(term()) -> {ok, term()} | {error, term()}.
narrow_ok_error({ok, _} = Ok) -> Ok;
narrow_ok_error({error, _} = Err) -> Err.

-spec extract_result_status(term()) -> ok | error.
extract_result_status({error, _}) -> error;
extract_result_status(_) -> ok.

-spec extract_num_rows(term()) -> non_neg_integer().
extract_num_rows(#{num_rows := N}) when is_integer(N) -> N;
extract_num_rows(#{rows := Rows}) when is_list(Rows) -> length(Rows);
extract_num_rows(_) -> 0.
