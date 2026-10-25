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
    soft_delete/2,
    restore/2,
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
    generate_uuid/1,
    uuid_version/1,
    key_clauses/2,
    key_conds/2,
    quote_ident_bin/1,
    dump_field/2,
    join_error/3
]).
-endif.

-eqwalizer({nowarn_function, multi/2}).
-eqwalizer({nowarn_function, join_many_to_many/5}).

-type write_error() :: {error, read_only}.
-type assoc_change() :: #kura_changeset{} | [#kura_changeset{}].
-export_type([write_error/0]).

%%----------------------------------------------------------------------
%% Pool management
%%----------------------------------------------------------------------

-doc "Start the connection pool for the given repo module.".
-spec start(module()) -> ok.
start(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    Pool = maps:get(pool, Config, RepoMod),
    PoolMod = kura_db:get_pool_module(RepoMod),
    PgoConfig = #{
        host => binary_to_list(maps:get(hostname, Config, ~"localhost")),
        port => maps:get(port, Config, 5432),
        database => binary_to_list(maps:get(database, Config)),
        user => binary_to_list(maps:get(username, Config, ~"postgres")),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => maps:get(pool_size, Config, 10),
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    },
    case PoolMod:start_pool(Pool, PgoConfig) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, _} -> ok
    end.

-doc "Execute a query and return all matching rows.".
-spec all(module(), #kura_query{}) -> {ok, [map()]} | {error, term()}.
all(RepoMod, Query) ->
    Q1 = maybe_apply_soft_delete(maybe_apply_tenant_prefix(Query)),
    {SQL, Params} = kura_query_compiler:to_sql_cached(RepoMod, Q1),

    case kura_db:query(RepoMod, SQL, Params) of
        #{command := select, rows := Rows} ->
            Schema = Query#kura_query.from,
            Loaded = [kura_db:load_row(Schema, Row) || Row <- Rows],
            case Query#kura_query.preloads of
                [] -> {ok, Loaded};
                Preloads -> {ok, kura_preloader:do_preload(RepoMod, Loaded, Schema, Preloads)}
            end;
        {error, _} = Err ->
            Err
    end.

-doc """
Fetch a single record by primary key.

For a single-key schema pass the bare key value; for a composite-key
schema pass a `#{field => value}` map covering every key column. A
map-valued single key must be wrapped as `#{KeyField => Map}`, since a
bare map is read as a composite key spec.
""".
-spec get(module(), module(), term()) -> {ok, map()} | {error, not_found} | {error, term()}.
get(RepoMod, SchemaMod, Spec) ->
    Q = apply_clauses(kura_query:from(SchemaMod), key_clauses(SchemaMod, Spec)),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {error, _} = Err -> Err
    end.

%% Build the primary-key WHERE clauses from a key spec: a bare value is
%% sugar for a single-key schema; a map addresses each key column.
key_clauses(SchemaMod, Spec) when is_map(Spec) ->
    [{F, fetch_key_field(F, Spec, SchemaMod)} || F <- kura_schema:key(SchemaMod)];
key_clauses(SchemaMod, Value) ->
    case kura_schema:key(SchemaMod) of
        [F] -> [{F, Value}];
        Key -> error({key_spec_required, SchemaMod, Key})
    end.

fetch_key_field(F, Spec, SchemaMod) ->
    case Spec of
        #{F := V} -> V;
        #{} -> error({incomplete_key, SchemaMod, F})
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
    Key = kura_schema:key(SchemaMod),
    case maps:with(Key, Record) of
        Spec when map_size(Spec) =:= length(Key) -> get(RepoMod, SchemaMod, Spec);
        _ -> {error, no_primary_key}
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
    {SQL, Params} = kura_query_compiler:to_sql(RepoMod, AggQ),
    case kura_db:query(RepoMod, SQL, Params) of
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

-doc """
Soft-delete a record by setting `deleted_at` to the current timestamp.
The record remains in the database but is excluded from normal queries.

Returns `{error, not_soft_deletable}` if the schema has no `deleted_at` field.
""".
-spec soft_delete(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
soft_delete(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case has_soft_delete(SchemaMod) of
        true ->
            Now = calendar:universal_time(),
            CS1 = kura_changeset:put_change(CS, deleted_at, Now),
            update(RepoMod, CS1);
        false ->
            {error, not_soft_deletable}
    end.

-doc """
Restore a soft-deleted record by clearing its `deleted_at` field.
""".
-spec restore(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
restore(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case has_soft_delete(SchemaMod) of
        true ->
            CS1 = kura_changeset:put_change(CS, deleted_at, undefined),
            update(RepoMod, CS1);
        false ->
            {error, not_soft_deletable}
    end.

guard_write(RepoMod, Fun) ->
    case kura_repo:read_only(RepoMod) of
        true -> {error, read_only};
        false -> Fun()
    end.

-doc "Insert a record from a changeset. Returns `{error, Changeset}` with errors on failure.".
-spec insert(module(), #kura_changeset{}) ->
    {ok, map()} | {error, #kura_changeset{}} | write_error().
insert(RepoMod, CS) ->
    guard_write(RepoMod, fun() -> do_insert(RepoMod, CS) end).

do_insert(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = insert}};
do_insert(RepoMod, CS = #kura_changeset{assoc_changes = AC}) when map_size(AC) > 0 ->
    atomically(RepoMod, fun() ->
        case insert_record(RepoMod, CS) of
            {ok, ParentRow} ->
                persist_assoc_changes(RepoMod, CS#kura_changeset{action = insert}, ParentRow);
            {error, _} = Err ->
                Err
        end
    end);
do_insert(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case needs_transaction(SchemaMod, insert) of
        true -> atomically(RepoMod, fun() -> insert_record(RepoMod, CS) end);
        false -> insert_record(RepoMod, CS)
    end.

-spec insert(module(), #kura_changeset{}, map()) ->
    {ok, map()} | {error, #kura_changeset{}} | write_error().
insert(RepoMod, CS, Opts) ->
    guard_write(RepoMod, fun() -> do_insert(RepoMod, CS, Opts) end).

do_insert(_RepoMod, CS = #kura_changeset{valid = false}, _Opts) ->
    {error, CS#kura_changeset{action = insert}};
do_insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}, Opts0) ->
    Opts = guard_upsert(Opts0),
    Changes0 = maybe_generate_pk(RepoMod, SchemaMod, maybe_apply_tenant_changes(Changes)),
    Changes1 = maybe_add_timestamps(SchemaMod, Changes0, insert),
    case dump_changes(SchemaMod, Changes1) of
        {error, DumpErr} ->
            dump_error_changeset(CS, insert, DumpErr);
        {ok, DumpedChanges} ->
            Fields = maps:keys(DumpedChanges),
            {SQL, Params} = kura_query_compiler:insert(
                RepoMod, SchemaMod, Fields, DumpedChanges, Opts
            ),
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := insert, rows := [Row]} ->
                    {ok, kura_db:load_row(SchemaMod, Row)};
                #{command := insert, rows := []} ->
                    {ok, kura_changeset:apply_changes(CS)};
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
            end
    end.

-doc "Update a record from a changeset. No-op if there are no changes.".
-spec update(module(), #kura_changeset{}) ->
    {ok, map()} | {error, #kura_changeset{}} | write_error().
update(RepoMod, CS) ->
    guard_write(RepoMod, fun() -> do_update(RepoMod, CS) end).

do_update(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = update}};
do_update(RepoMod, CS = #kura_changeset{assoc_changes = AC}) when map_size(AC) > 0 ->
    atomically(RepoMod, fun() ->
        case update_record(RepoMod, CS) of
            {ok, ParentRow} ->
                persist_assoc_changes(RepoMod, CS#kura_changeset{action = update}, ParentRow);
            {error, _} = Err ->
                Err
        end
    end);
do_update(RepoMod, CS = #kura_changeset{schema = SchemaMod}) ->
    case needs_transaction(SchemaMod, update) of
        true -> atomically(RepoMod, fun() -> update_record(RepoMod, CS) end);
        false -> update_record(RepoMod, CS)
    end.

-doc "Delete the record referenced by the changeset's data.".
-spec delete(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
delete(RepoMod, CS) ->
    guard_write(RepoMod, fun() -> do_delete(RepoMod, CS) end).

do_delete(RepoMod, #kura_changeset{schema = SchemaMod, data = Data}) ->
    case needs_transaction(SchemaMod, delete) of
        true -> atomically(RepoMod, fun() -> delete_record(RepoMod, SchemaMod, Data) end);
        false -> delete_record(RepoMod, SchemaMod, Data)
    end.

%% A `{error, _}` from `Fun` is thrown so the transaction rolls back, nested or
%% not, rather than commit whatever was written before it.
atomically(RepoMod, Fun) ->
    Rollback = make_ref(),
    try
        kura_db:transaction_ok(RepoMod, fun() ->
            case Fun() of
                {ok, _} = Ok -> Ok;
                {error, Reason} -> throw({Rollback, Reason})
            end
        end)
    catch
        throw:{Rollback, Reason} -> {error, Reason}
    end.

-doc "Bulk update all rows matching the query, returning the count of affected rows.".
-spec update_all(module(), #kura_query{}, map()) -> {ok, non_neg_integer()} | {error, term()}.
update_all(RepoMod, Query, Updates) ->
    guard_write(RepoMod, fun() -> do_update_all(RepoMod, Query, Updates) end).

do_update_all(RepoMod, Query, Updates) ->
    Q1 = maybe_apply_soft_delete(maybe_apply_tenant_prefix(Query)),
    case dump_set_map(Q1#kura_query.from, pin_tenant(Updates)) of
        {error, _} = DumpErr ->
            DumpErr;
        {ok, Dumped} ->
            {SQL, Params} = kura_query_compiler:update_all(RepoMod, Q1, Dumped),

            case kura_db:query(RepoMod, SQL, Params) of
                #{command := update, num_rows := Count} -> {ok, Count};
                {error, _} = Err -> Err
            end
    end.

-doc "Bulk delete all rows matching the query, returning the count of deleted rows.".
-spec delete_all(module(), #kura_query{}) -> {ok, non_neg_integer()} | {error, term()}.
delete_all(RepoMod, Query) ->
    guard_write(RepoMod, fun() -> do_delete_all(RepoMod, Query) end).

do_delete_all(RepoMod, Query) ->
    Q1 = maybe_apply_soft_delete(maybe_apply_tenant_prefix(Query)),
    {SQL, Params} = kura_query_compiler:delete_all(RepoMod, Q1),

    case kura_db:query(RepoMod, SQL, Params) of
        #{command := delete, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-doc "Bulk insert a list of maps, returning the count of inserted rows.".
-spec insert_all(module(), module(), [map()]) -> {ok, non_neg_integer()} | {error, term()}.
insert_all(RepoMod, SchemaMod, Entries) ->
    guard_write(RepoMod, fun() -> do_insert_all(RepoMod, SchemaMod, Entries) end).

do_insert_all(RepoMod, SchemaMod, Entries) ->
    do_insert_all(RepoMod, SchemaMod, Entries, #{}).

-doc "Bulk insert with options. `#{returning => true | [atom()]}` returns inserted rows.".
-spec insert_all(module(), module(), [map()], map()) ->
    {ok, non_neg_integer()} | {ok, non_neg_integer(), [map()]} | {error, term()}.
insert_all(RepoMod, SchemaMod, Entries, Opts) ->
    guard_write(RepoMod, fun() -> do_insert_all(RepoMod, SchemaMod, Entries, Opts) end).

do_insert_all(RepoMod, SchemaMod, Entries, Opts) ->
    case dump_entries(SchemaMod, [maybe_apply_tenant_changes(E) || E <- Entries]) of
        {error, _} = DumpErr -> DumpErr;
        {ok, Rows} -> do_insert_all_rows(RepoMod, SchemaMod, Rows, guard_upsert(Opts))
    end.

do_insert_all_rows(RepoMod, SchemaMod, Rows, Opts) ->
    [First | _] = Rows,
    Fields = maps:keys(First),
    {SQL, Params} = kura_query_compiler:insert_all(RepoMod, SchemaMod, Fields, Rows, Opts),

    case Opts of
        #{returning := RetOpt} when RetOpt =:= true; is_list(RetOpt) ->
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := insert, num_rows := Count, rows := RetRows} ->
                    Loaded = [kura_db:load_row(SchemaMod, R) || R <- RetRows],
                    {ok, Count, Loaded};
                {error, _} = Err ->
                    Err
            end;
        #{} ->
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := insert, num_rows := Count} -> {ok, Count};
                {error, _} = Err -> Err
            end
    end.

-doc "Execute a function inside a database transaction.".
-spec transaction(module(), fun(() -> term())) -> term().
transaction(RepoMod, Fun) ->
    kura_db:transaction(RepoMod, Fun).

-doc "Execute a `kura_multi` pipeline inside a transaction.".
-spec multi(module(), term()) ->
    {ok, map()} | {error, atom(), term(), map()}.
multi(RepoMod, Multi) ->
    Ops = kura_multi:to_list(Multi),
    try
        kura_db:transaction_ok(RepoMod, fun() ->
            execute_multi_ops(RepoMod, Ops, #{})
        end)
    catch
        throw:{multi_error, Name, Value, Completed} ->
            {error, Name, Value, Completed}
    end.

-doc "Execute raw SQL with parameters.".
-spec query(module(), iodata(), [term()]) -> {ok, list()} | {error, term()}.
query(RepoMod, SQL, Params) ->
    case kura_db:query(RepoMod, SQL, Params) of
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
%% Internal: pgo bridge (delegates to kura_db)
%%----------------------------------------------------------------------

pgo_query(RepoMod, SQL, Params) ->
    kura_db:query(RepoMod, SQL, Params).

build_telemetry_metadata(RepoMod, SQL, Params, Result) ->
    kura_db:build_telemetry_metadata(RepoMod, SQL, Params, Result).

extract_source(SQL) ->
    kura_db:extract_source(SQL).

insert_record(RepoMod, CS0 = #kura_changeset{schema = SchemaMod}) ->
    CS1 = run_prepare(CS0),
    case kura_schema:run_before_insert(SchemaMod, CS1) of
        {error, ErrCS} ->
            {error, ErrCS#kura_changeset{action = insert}};
        {ok, CS} ->
            Changes = maybe_apply_tenant_changes(CS#kura_changeset.changes),
            Changes0 = maybe_generate_pk(RepoMod, SchemaMod, Changes),
            Changes1 = maybe_add_timestamps(SchemaMod, Changes0, insert),
            case dump_changes(SchemaMod, Changes1) of
                {error, DumpErr} ->
                    dump_error_changeset(CS, insert, DumpErr);
                {ok, DumpedChanges} ->
                    Fields = maps:keys(DumpedChanges),
                    {SQL, Params} = kura_query_compiler:insert(
                        RepoMod, SchemaMod, Fields, DumpedChanges
                    ),
                    case kura_db:query(RepoMod, SQL, Params) of
                        #{command := insert, rows := [Row]} ->
                            Row1 = kura_db:load_row(SchemaMod, Row),
                            kura_schema:run_after_insert(SchemaMod, Row1);
                        {error, PgError} ->
                            {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
                    end
            end
    end.

update_record(RepoMod, CS0 = #kura_changeset{schema = SchemaMod, data = Data}) ->
    CS1 = run_prepare(CS0),
    case kura_schema:run_before_update(SchemaMod, CS1) of
        {error, ErrCS} ->
            {error, ErrCS#kura_changeset{action = update}};
        {ok, CS} ->
            Changes = CS#kura_changeset.changes,
            case map_size(Changes) of
                0 when map_size(CS#kura_changeset.assoc_changes) > 0 ->
                    owned_by_tenant(RepoMod, CS, Data);
                0 ->
                    {ok, Data};
                _ ->
                    KeyClauses = tenant_key_clauses(key_pairs(SchemaMod, Data)),
                    Changes1 = maybe_add_timestamps(SchemaMod, pin_tenant(Changes), update),
                    case dump_changes(SchemaMod, Changes1) of
                        {error, DumpErr} ->
                            dump_error_changeset(CS, update, DumpErr);
                        {ok, DumpedChanges} ->
                            Fields = maps:keys(DumpedChanges),
                            case
                                do_update_query(
                                    RepoMod, CS, SchemaMod, Fields, DumpedChanges, KeyClauses
                                )
                            of
                                {ok, Row} ->
                                    kura_schema:run_after_update(SchemaMod, Row);
                                {error, _} = Err ->
                                    Err
                            end
                    end
            end
    end.

%% With no field to write the key never reaches the database, so under attribute
%% tenancy the record is looked up before its associations are written.
owned_by_tenant(RepoMod, CS = #kura_changeset{schema = SchemaMod}, Data) ->
    case kura_tenant:get_tenant() of
        {attribute, _} ->
            Q0 = apply_clauses(kura_query:from(SchemaMod), key_pairs(SchemaMod, Data)),
            case all(RepoMod, kura_query:limit(Q0#kura_query{include_deleted = true}, 1)) of
                {ok, [Row]} ->
                    {ok, Row};
                {ok, []} ->
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, base, ~"record not found"
                        )};
                {error, _} = Err ->
                    Err
            end;
        _ ->
            {ok, Data}
    end.

do_update_query(RepoMod, CS, SchemaMod, Fields, DumpedChanges, KeyClauses) ->
    case CS#kura_changeset.optimistic_lock of
        undefined ->
            {SQL, Params} = kura_query_compiler:update(
                RepoMod, SchemaMod, Fields, DumpedChanges, KeyClauses
            ),
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, kura_db:load_row(SchemaMod, Row)};
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
            {SQL, Params} = kura_query_compiler:update(
                RepoMod, SchemaMod, Fields, DumpedChanges, KeyClauses ++ [{LockField, LockValue}]
            ),
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, kura_db:load_row(SchemaMod, Row)};
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

%% One `col = $n` condition per key column, using the binary quoter.
key_conds(KeyClauses, Start) ->
    key_conds(KeyClauses, Start, [], []).

key_conds([], Counter, Conds, Params) ->
    {lists:reverse(Conds), lists:reverse(Params), Counter};
key_conds([{Field, Value} | Rest], Counter, Conds, Params) ->
    Cond = [quote_ident_bin(atom_to_binary(Field, utf8)), ~" = $", integer_to_binary(Counter)],
    key_conds(Rest, Counter + 1, [Cond | Conds], [Value | Params]).

%% The key column/value pairs from a record, in key order.
key_pairs(SchemaMod, Data) ->
    [{K, maps:get(K, Data)} || K <- kura_schema:key(SchemaMod)].

quote_ident_bin(Name) when is_binary(Name) ->
    Escaped = binary:replace(Name, ~"\"", ~"\"\"", [global]),
    <<$", Escaped/binary, $">>.

-spec apply_clauses(#kura_query{}, [{atom(), term()}]) -> #kura_query{}.
apply_clauses(Q, []) ->
    Q;
apply_clauses(Q, [{Field, Value} | Rest]) ->
    apply_clauses(kura_query:where(Q, {Field, Value}), Rest).

persist_assoc_changes(RepoMod, CS = #kura_changeset{assoc_changes = AC}, ParentRow) ->
    persist_assocs(RepoMod, CS, ParentRow, maps:to_list(AC)).

persist_assocs(_RepoMod, _CS, Row, []) ->
    {ok, Row};
persist_assocs(RepoMod, CS, Row, [{AssocName, ChildCSs} | Rest]) ->
    SchemaMod = CS#kura_changeset.schema,
    {ok, Assoc} = kura_schema:association(SchemaMod, AssocName),
    Persisted =
        case Assoc#kura_assoc.type of
            many_to_many -> persist_many_to_many(RepoMod, SchemaMod, Row, Assoc, ChildCSs);
            _ -> persist_owned_assoc(RepoMod, SchemaMod, Row, Assoc, ChildCSs)
        end,
    case Persisted of
        {ok, Children} ->
            persist_assocs(RepoMod, CS, Row#{AssocName => Children}, Rest);
        {child_failed, Failed} ->
            AC = CS#kura_changeset.assoc_changes,
            {error, CS#kura_changeset{valid = false, assoc_changes = AC#{AssocName => Failed}}};
        {join_failed, PgError} ->
            {error, join_error(CS, AssocName, PgError)};
        {error, Reason} ->
            {error, kura_changeset:add_error(CS, base, child_error(Reason))}
    end.

%% Only a constraint violation says something about the parent's own fields.
join_error(CS, _AssocName, {pgsql_error, #{code := Code}} = PgError) when
    Code =:= ~"23505";
    Code =:= ~"23503";
    Code =:= ~"23514";
    Code =:= ~"23P01";
    Code =:= ~"23502"
->
    handle_pg_error(CS, PgError);
join_error(CS, AssocName, _PgError) ->
    kura_changeset:add_error(CS, AssocName, ~"could not be saved").

child_error(stale) -> ~"an associated record is stale";
child_error(_Reason) -> ~"an associated record could not be saved".

-spec persist_owned_assoc(
    module(),
    module(),
    map(),
    #kura_assoc{},
    #kura_changeset{} | [#kura_changeset{}]
) -> {ok, map() | [map()]} | {child_failed, assoc_change()} | {error, term()}.
persist_owned_assoc(RepoMod, SchemaMod, AccRow, Assoc, ChildCSs) when is_list(ChildCSs) ->
    Binding = owned_fk_binding(Assoc, SchemaMod, AccRow),
    Bound = [set_owned_fk(ChildCS, Binding) || ChildCS <- ChildCSs],
    persist_children(RepoMod, owned, Bound, ChildCSs);
persist_owned_assoc(RepoMod, SchemaMod, AccRow, Assoc, ChildCS) ->
    Binding = owned_fk_binding(Assoc, SchemaMod, AccRow),
    case persist_child(RepoMod, owned, set_owned_fk(ChildCS, Binding)) of
        {ok, Child} -> {ok, Child};
        {error, #kura_changeset{} = Failed} -> {child_failed, Failed};
        {error, _} = Err -> Err
    end.

%% On a failure the failed changeset takes its place in the list the caller
%% passed, so the parent's `assoc_changes` shows which child it was.
-spec persist_children(
    module(), owned | many_to_many, [#kura_changeset{}], [#kura_changeset{}]
) -> {ok, [map()]} | {child_failed, [#kura_changeset{}]} | {error, term()}.
persist_children(_RepoMod, _Kind, [], _Given) ->
    {ok, []};
persist_children(RepoMod, Kind, [ChildCS | Rest], [Given | GivenRest]) ->
    case persist_child(RepoMod, Kind, ChildCS) of
        {ok, Child} ->
            case persist_children(RepoMod, Kind, Rest, GivenRest) of
                {ok, Children} -> {ok, [Child | Children]};
                {child_failed, Failed} -> {child_failed, [Given | Failed]};
                {error, _} = Err -> Err
            end;
        {error, #kura_changeset{} = Failed} ->
            {child_failed, [Failed | GivenRest]};
        {error, _} = Err ->
            Err
    end.

%% Single-column FKs are the one-element case of the same binding.
owned_fk_binding(Assoc, SchemaMod, AccRow) ->
    FKCols = kura_schema:assoc_fields(Assoc),
    ParentKey =
        case kura_schema:assoc_target_key(Assoc) of
            undefined -> kura_schema:key(SchemaMod);
            Cols -> Cols
        end,
    lists:zip(FKCols, [maps:get(K, AccRow) || K <- ParentKey]).

set_owned_fk(CS, []) ->
    CS;
set_owned_fk(CS, [{Col, Value} | Rest]) ->
    set_owned_fk(kura_changeset:put_change(CS, Col, Value), Rest).

persist_child(RepoMod, _Kind, CS = #kura_changeset{action = insert}) ->
    insert_record(RepoMod, CS);
persist_child(RepoMod, _Kind, CS = #kura_changeset{action = update}) ->
    update_record(RepoMod, CS);
persist_child(_RepoMod, many_to_many, CS = #kura_changeset{action = undefined}) ->
    {ok, kura_changeset:apply_changes(CS)}.

-spec persist_many_to_many(
    module(),
    module(),
    map(),
    #kura_assoc{},
    [#kura_changeset{}]
) -> {ok, [map()]} | {child_failed, assoc_change()} | {join_failed, term()} | {error, term()}.
persist_many_to_many(RepoMod, SchemaMod, AccRow, Assoc, ChildCSs) ->
    case persist_children(RepoMod, many_to_many, ChildCSs, ChildCSs) of
        {ok, Children} -> join_many_to_many(RepoMod, SchemaMod, AccRow, Assoc, Children);
        Failed -> Failed
    end.

join_many_to_many(RepoMod, SchemaMod, AccRow, Assoc, Children) ->
    #kura_assoc{schema = RelSchema, join_through = JoinThrough} = Assoc,
    {OwnerCols, RelatedCols} = kura_schema:assoc_join_keys(Assoc),
    OwnerValues = [maps:get(K, AccRow) || K <- kura_schema:key(SchemaMod)],
    RelKey = kura_schema:key(RelSchema),
    JoinTable =
        case JoinThrough of
            B when is_binary(B) -> B;
            M when is_atom(M) -> M:table()
        end,
    {OwnerConds, OwnerParams, _} = key_conds(lists:zip(OwnerCols, OwnerValues), 1),
    DeleteSQL = iolist_to_binary([
        ~"DELETE FROM ",
        kura_preloader:quote_table_bin(JoinTable),
        ~" WHERE ",
        lists:join(~" AND ", OwnerConds)
    ]),
    Inserts = [
        m2m_insert_sql(
            JoinTable, OwnerCols ++ RelatedCols, OwnerValues ++ [maps:get(K, Child) || K <- RelKey]
        )
     || Child <- Children
    ],
    case join_writes(RepoMod, [{DeleteSQL, OwnerParams} | Inserts]) of
        ok -> {ok, Children};
        {error, PgError} -> {join_failed, PgError}
    end.

join_writes(_RepoMod, []) ->
    ok;
join_writes(RepoMod, [{SQL, Params} | Rest]) ->
    case kura_db:query(RepoMod, SQL, Params) of
        {error, _} = Err -> Err;
        _ -> join_writes(RepoMod, Rest)
    end.

m2m_insert_sql(JoinTable, Cols, Values) ->
    ColSQL = lists:join(~", ", [quote_ident_bin(atom_to_binary(C, utf8)) || C <- Cols]),
    Placeholders = lists:join(~", ", [
        [~"$", integer_to_binary(N)]
     || N <- lists:seq(1, length(Cols))
    ]),
    SQL = iolist_to_binary([
        ~"INSERT INTO ",
        kura_preloader:quote_table_bin(JoinTable),
        ~" (",
        ColSQL,
        ~") VALUES (",
        Placeholders,
        ~")"
    ]),
    {SQL, Values}.

delete_record(RepoMod, SchemaMod, Data) ->
    case kura_schema:run_before_delete(SchemaMod, Data) of
        {error, _} = Err ->
            Err;
        ok ->
            KeyClauses = tenant_key_clauses(key_pairs(SchemaMod, Data)),
            {SQL, Params} = kura_query_compiler:delete(RepoMod, SchemaMod, KeyClauses),
            case kura_db:query(RepoMod, SQL, Params) of
                #{command := delete, rows := [Row]} ->
                    Row1 = kura_db:load_row(SchemaMod, Row),
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

-spec dump_set_map(atom() | module() | undefined, map()) ->
    {ok, map()} | {error, {dump_failed, atom(), binary()}}.
dump_set_map(From, Updates) when is_atom(From), From =/= undefined ->
    case is_schema_module(From) of
        true -> dump_set_fields(maps:to_list(Updates), kura_schema:field_types(From), #{});
        false -> {ok, Updates}
    end;
dump_set_map(_From, Updates) ->
    {ok, Updates}.

dump_set_fields([], _Types, Acc) ->
    {ok, Acc};
dump_set_fields([{K, V} | Rest], Types, Acc) ->
    case Types of
        #{K := Type} ->
            case dump_field(Type, V) of
                {ok, Dumped} -> dump_set_fields(Rest, Types, Acc#{K => Dumped});
                {error, Msg} -> {error, {dump_failed, K, Msg}}
            end;
        #{} ->
            dump_set_fields(Rest, Types, Acc#{K => V})
    end.

%% The bulk paths take raw maps rather than cast changesets, so a caller
%% reasonably passes a value in the form `cast/2` accepts - `~"admin"` for an
%% enum, `~"2026-01-01"` for a date. Dump first so the cast costs nothing on
%% the happy path, and fall back to casting before failing the write. When the
%% cast also fails its message is the one reported - `dump/2` only knows the
%% shape is wrong, while `cast/2` knows why the value is invalid.
dump_field(Type, V) ->
    case kura_types:dump(Type, V) of
        {ok, Dumped} ->
            {ok, Dumped};
        {error, Msg} ->
            %% cast/2 is not total: it reaches list_to_binary/1 and the
            %% datetime parsers unguarded, and `{custom, Mod}` calls arbitrary
            %% consumer code that can raise anything. Catching keeps the write
            %% fail closed. An {encrypted, _} value never reaches here - its
            %% dump raises rather than returning {error, _} - so the crypto
            %% fail-loud guarantee does not depend on this catch being narrow.
            %% The `of` body is unprotected, so a raise from the re-dump in
            %% dump_cast/3 still propagates.
            try kura_types:cast(Type, V) of
                {ok, Cast} -> dump_cast(Type, Cast, Msg);
                {error, CastMsg} -> {error, CastMsg}
            catch
                error:_ -> {error, Msg};
                throw:_ -> {error, Msg}
            end
    end.

dump_cast(Type, Cast, Msg) ->
    case kura_types:dump(Type, Cast) of
        {ok, Dumped} -> {ok, Dumped};
        {error, _} -> {error, Msg}
    end.

is_schema_module(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} -> erlang:function_exported(Mod, table, 0);
        _ -> false
    end.

-spec dump_entries(module(), [map()]) -> {ok, [map()]} | {error, {dump_failed, atom(), binary()}}.
dump_entries(SchemaMod, Entries) ->
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    dump_entries(SchemaMod, NonVirtual, Entries, []).

dump_entries(_SchemaMod, _NonVirtual, [], Acc) ->
    {ok, lists:reverse(Acc)};
dump_entries(SchemaMod, NonVirtual, [Entry | Rest], Acc) ->
    WithTS = maybe_add_timestamps(SchemaMod, Entry, insert),
    Filtered = maps:with(NonVirtual, WithTS),
    case dump_changes(SchemaMod, Filtered) of
        {ok, Row} -> dump_entries(SchemaMod, NonVirtual, Rest, [Row | Acc]);
        {error, {Field, Msg}} -> {error, {dump_failed, Field, Msg}}
    end.

-spec dump_changes(module(), map()) -> {ok, map()} | {error, {atom(), binary()}}.
dump_changes(SchemaMod, Changes) ->
    Types = kura_schema:field_types(SchemaMod),
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    dump_change_fields(maps:to_list(Changes), Types, NonVirtual, #{}).

dump_change_fields([], _Types, _NonVirtual, Acc) ->
    {ok, Acc};
dump_change_fields([{K, V} | Rest], Types, NonVirtual, Acc) ->
    case {lists:member(K, NonVirtual), Types} of
        {false, _} ->
            dump_change_fields(Rest, Types, NonVirtual, Acc);
        {true, #{K := Type}} ->
            case dump_field(Type, V) of
                {ok, Dumped} -> dump_change_fields(Rest, Types, NonVirtual, Acc#{K => Dumped});
                {error, Msg} -> {error, {K, Msg}}
            end;
        {true, #{}} ->
            dump_change_fields(Rest, Types, NonVirtual, Acc#{K => V})
    end.

dump_error_changeset(CS, Action, {Field, Msg}) ->
    {error, kura_changeset:add_error(CS#kura_changeset{action = Action}, Field, Msg)}.

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

-spec maybe_generate_pk(module(), module(), map()) -> map().
maybe_generate_pk(RepoMod, SchemaMod, Changes) ->
    case kura_schema:key(SchemaMod) of
        [PK] -> maybe_generate_single_pk(RepoMod, SchemaMod, PK, Changes);
        _ -> Changes
    end.

-spec maybe_generate_single_pk(module(), module(), atom(), map()) -> map().
maybe_generate_single_pk(RepoMod, SchemaMod, PK, Changes) ->
    case maps:is_key(PK, Changes) of
        true ->
            Changes;
        false ->
            case kura_schema:generate_id(SchemaMod) of
                {ok, Id} ->
                    Changes#{PK => Id};
                undefined ->
                    Types = kura_schema:field_types(SchemaMod),
                    case maps:get(PK, Types, undefined) of
                        uuid -> Changes#{PK => generate_uuid(uuid_version(RepoMod))};
                        _ -> Changes
                    end
            end
    end.

-spec uuid_version(module()) -> v7 | v4.
uuid_version(RepoMod) ->
    case maps:get(uuid_version, kura_repo:config(RepoMod), v4) of
        v7 -> v7;
        _ -> v4
    end.

-spec generate_uuid(v7 | v4) -> binary().
generate_uuid(v7) -> generate_uuid_v7();
generate_uuid(v4) -> generate_uuid_v4().

-spec generate_uuid_v7() -> binary().
generate_uuid_v7() ->
    TsMs = os:system_time(millisecond),
    <<RandA:12, RandB:62, _:6>> = crypto:strong_rand_bytes(10),
    format_uuid(<<TsMs:48, 7:4, RandA:12, 2:2, RandB:62>>).

-spec generate_uuid_v4() -> binary().
generate_uuid_v4() ->
    <<A:48, _:4, B:12, _:2, C:62>> = crypto:strong_rand_bytes(16),
    format_uuid(<<A:48, 4:4, B:12, 2:2, C:62>>).

-spec format_uuid(binary()) -> binary().
format_uuid(Bytes) ->
    Hex = binary:encode_hex(Bytes, lowercase),
    <<P1:8/binary, P2:4/binary, P3:4/binary, P4:4/binary, P5:12/binary>> = Hex,
    <<P1/binary, "-", P2/binary, "-", P3/binary, "-", P4/binary, "-", P5/binary>>.

handle_pg_error(CS, {pgsql_error, Fields}) when is_map(Fields) ->
    handle_pg_error(CS, Fields);
handle_pg_error(CS, {pgsql_error, Fields}) when is_list(Fields) ->
    handle_pg_error(CS, maps:from_list(Fields));
handle_pg_error(CS, #{code := Code, constraint := Constraint}) when
    Code =:= ~"23505"; Code =:= ~"23503"; Code =:= ~"23514"; Code =:= ~"23P01"
->
    DefaultType =
        case Code of
            ~"23505" -> unique;
            ~"23503" -> foreign_key;
            ~"23514" -> check;
            ~"23P01" -> exclusion
        end,
    DefaultMsg =
        case DefaultType of
            unique -> ~"has already been taken";
            foreign_key -> ~"does not exist";
            check -> ~"is invalid";
            exclusion -> ~"violates an exclusion constraint"
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
    kura_db:build_log_event(Repo, SQL, Params, Result, DurationUs).

-doc "Return a default logger function that logs queries via logger:info.".
-spec default_logger() -> fun((map()) -> ok).
default_logger() ->
    kura_db:default_logger().

%%----------------------------------------------------------------------
%% Internal: multitenancy
%%----------------------------------------------------------------------

needs_transaction(undefined, _Action) ->
    false;
needs_transaction(SchemaMod, Action) ->
    kura_schema:has_after_hook(SchemaMod, Action).

maybe_apply_soft_delete(Query) ->
    case Query#kura_query.include_deleted of
        true ->
            Query;
        false ->
            Schema = Query#kura_query.from,
            case has_soft_delete(Schema) of
                true -> kura_query:where(Query, {deleted_at, is_nil});
                false -> Query
            end
    end.

has_soft_delete(undefined) ->
    false;
has_soft_delete(SchemaMod) ->
    Types = kura_schema:field_types(SchemaMod),
    maps:is_key(deleted_at, Types).

maybe_apply_tenant_prefix(Query) ->
    case kura_tenant:get_tenant() of
        {prefix, Prefix} ->
            case Query#kura_query.prefix of
                undefined -> Query#kura_query{prefix = Prefix};
                _ -> Query
            end;
        _ ->
            Query
    end.

tenant_key_clauses(KeyClauses) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, Value}} -> KeyClauses ++ [{Field, Value}];
        _ -> KeyClauses
    end.

pin_tenant(Changes) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, Value}} when is_map_key(Field, Changes) -> Changes#{Field => Value};
        _ -> Changes
    end.

guard_upsert(#{on_conflict := OnConflict} = Opts) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, _Value}} -> Opts#{on_conflict => guarded(OnConflict, Field)};
        _ -> Opts
    end;
guard_upsert(Opts) ->
    Opts.

guarded({_Target, nothing} = OnConflict, _Field) ->
    OnConflict;
guarded({Target, Action}, Field) ->
    {Target, Action, #{guard => [Field]}};
guarded({Target, Action, Opts}, Field) when is_map(Opts) ->
    {Target, Action, Opts#{guard => lists:usort([Field | maps:get(guard, Opts, [])])}}.

maybe_apply_tenant_changes(Changes) ->
    case kura_tenant:get_tenant() of
        {attribute, {Field, Value}} ->
            Changes#{Field => Value};
        _ ->
            Changes
    end.
