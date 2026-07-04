-module(kura_migrator).
-moduledoc """
Runs, rolls back, and reports status of migrations.

Discovers migration modules automatically from the application that owns the
repo module. Any module named `m<YYYYMMDDHHMMSS>_<name>` in the application's
module list is treated as a migration. Tracks applied versions in a
`schema_migrations` table.

All migrations run within a single transaction protected by a PostgreSQL
advisory lock (`pg_advisory_xact_lock`) to prevent concurrent execution
across multiple nodes.
""".

-include("kura.hrl").

%% Arbitrary constant used as the advisory lock key. All Kura migrators
%% sharing a PostgreSQL database contend on this single lock.
-define(MIGRATION_LOCK_KEY, 571629482).

%% Defaults for the pool-readiness wait at the start of migrator operations.
%% pgo workers connect asynchronously, so the pool may exist but have no
%% available connections yet; we retry `SELECT 1` until the pool answers.
-define(POOL_READY_TIMEOUT_MS, 5000).
-define(POOL_READY_INTERVAL_MS, 50).

-export([
    migrate/1,
    rollback/1, rollback/2,
    status/1,
    ensure_database/1,
    ensure_schema_migrations/1,
    wait_for_pool/1, wait_for_pool/2,
    compile_operation/2,
    check_unsafe_operations/2
]).

-ifdef(TEST).
-export([
    sort_migrations/1,
    sort_integers/1,
    reverse_integers/2,
    take_integers/2,
    partition_ints/4,
    partition_migrations/4,
    build_rollback_pairs/2,
    narrow_migration_result/1,
    get_app_modules/1,
    to_module_list/1,
    parse_version/1,
    exec_operations/3,
    check_alter_ops/3,
    topo_sort_ops/1
]).
-endif.

%% eqWAlizer: operation() union has >7 variants - exceeds clause narrowing limit
-eqwalizer({nowarn_function, compile_operation/2}).
-eqwalizer({nowarn_function, default_default/1}).

-doc "Run all pending migrations in order.".
-spec migrate(module()) -> {ok, [integer()]} | {error, term()}.
migrate(RepoMod) ->
    case application:get_env(kura, ensure_database, true) of
        true -> ensure_database(RepoMod);
        false -> ok
    end,
    case wait_for_pool(RepoMod) of
        ok ->
            ensure_schema_migrations(RepoMod),
            T0 = erlang:monotonic_time(),
            Applied = get_applied_versions(RepoMod),
            Migrations = discover_migrations(RepoMod),
            Pending = [{V, M} || {V, M} <- Migrations, not lists:member(V, Applied)],
            Sorted = sort_migrations(Pending),
            telemetry_event(
                [kura, migrator, migrate, start],
                #{system_time => erlang:system_time()},
                #{repo => RepoMod, pending_count => length(Sorted), direction => up}
            ),
            Result = with_migration_lock(RepoMod, fun() ->
                run_migrations(Sorted, up, RepoMod, [])
            end),
            emit_migrate_stop(RepoMod, T0, up, Result),
            Result;
        {error, _} = Err ->
            Err
    end.

-doc "Roll back the last migration.".
-spec rollback(module()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod) ->
    rollback(RepoMod, 1).

-doc "Roll back the last `Steps` migrations.".
-spec rollback(module(), non_neg_integer()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod, Steps) ->
    case wait_for_pool(RepoMod) of
        ok ->
            ensure_schema_migrations(RepoMod),
            T0 = erlang:monotonic_time(),
            Applied = get_applied_versions(RepoMod),
            Migrations = discover_migrations(RepoMod),
            SortedApplied = sort_integers(Applied),
            ToRollback = take_integers(reverse_integers(SortedApplied, []), Steps),
            Pairs = build_rollback_pairs(ToRollback, maps:from_list(Migrations)),
            telemetry_event(
                [kura, migrator, migrate, start],
                #{system_time => erlang:system_time()},
                #{repo => RepoMod, pending_count => length(Pairs), direction => down}
            ),
            Result = with_migration_lock(RepoMod, fun() ->
                run_migrations(Pairs, down, RepoMod, [])
            end),
            emit_migrate_stop(RepoMod, T0, down, Result),
            Result;
        {error, _} = Err ->
            Err
    end.

-spec emit_migrate_stop(module(), integer(), up | down, term()) -> ok.
emit_migrate_stop(RepoMod, T0, Direction, Result) ->
    DurationNative = erlang:monotonic_time() - T0,
    {ResultStatus, AppliedCount, ErrorReason} =
        case Result of
            {ok, Versions} when is_list(Versions) -> {ok, length(Versions), undefined};
            {error, R} -> {error, 0, R}
        end,
    telemetry_event(
        [kura, migrator, migrate, stop],
        #{duration => DurationNative},
        #{
            repo => RepoMod,
            direction => Direction,
            result => ResultStatus,
            applied_count => AppliedCount,
            error_reason => ErrorReason
        }
    ).

-spec telemetry_event([atom()], map(), map()) -> ok.
telemetry_event(EventName, Measurements, Metadata) ->
    %% telemetry is a hard dependency of kura via pgo, but we still
    %% guard the call: telemetry handlers run inline and an exception
    %% inside one must not abort a migration. The whole point of
    %% emitting these events is operational observability, and dropping
    %% an event is far less bad than dropping a transaction.
    try
        telemetry:execute(EventName, Measurements, Metadata),
        ok
    catch
        _:_ -> ok
    end.

-doc "Return the status of all discovered migrations (`:up` or `:pending`).".
-spec status(module()) -> [{integer(), module(), up | pending}].
status(RepoMod) ->
    case wait_for_pool(RepoMod) of
        ok ->
            ensure_schema_migrations(RepoMod),
            Applied = get_applied_versions(RepoMod),
            Migrations = discover_migrations(RepoMod),
            Sorted = sort_migrations(Migrations),
            [tag_status(V, M, Applied) || {V, M} <- Sorted];
        {error, _} ->
            []
    end.

tag_status(V, M, Applied) ->
    case lists:member(V, Applied) of
        true -> {V, M, up};
        false -> {V, M, pending}
    end.

%%----------------------------------------------------------------------
%% Database creation
%%----------------------------------------------------------------------

-spec ensure_database(module()) -> ok.
ensure_database(RepoMod) ->
    Driver = kura_db:get_driver_module(RepoMod),
    _ = code:ensure_loaded(Driver),
    case erlang:function_exported(Driver, ensure_database, 1) of
        true ->
            Config = kura_repo:config(RepoMod),
            _ = Driver:ensure_database(Config),
            ok;
        false ->
            ok
    end.

%%----------------------------------------------------------------------
%% Schema migrations table
%%----------------------------------------------------------------------

-spec ensure_schema_migrations(module()) -> ok.
ensure_schema_migrations(RepoMod) ->
    %% Portable SQL: TIMESTAMP + CURRENT_TIMESTAMP work in both Postgres
    %% and SQLite. Postgres treats TIMESTAMP as WITHOUT TIME ZONE which
    %% is fine for an internal bookkeeping column.
    SQL = <<
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version BIGINT PRIMARY KEY, "
        "inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ")"
    >>,
    _ = kura_db:query(RepoMod, SQL, []),
    ok.

%%----------------------------------------------------------------------
%% Pool readiness
%%----------------------------------------------------------------------

-doc """
Wait until the repo's connection pool has at least one ready worker.

`pgo` workers connect asynchronously, so when an application starts
and immediately calls `migrate/1` the pool may exist but reject queries
with `none_available` until handshake completes. The migrator races
this every time on fast CT startup. Other operations (rollback, status)
share the same hazard. We retry `SELECT 1` until it succeeds or the
configured timeout elapses; any successful round-trip proves the pool
is serving queries.

Timeout/interval are configurable via the `kura` app env keys
`migration_pool_ready_timeout` and `migration_pool_ready_interval`
(both in milliseconds).
""".
%% Acquire a session-wide migration lock when the backend supports
%% advisory locks. SQLite and other single-writer backends rely on the
%% transaction itself for serialization.
-spec maybe_acquire_advisory_lock(module(), module()) -> ok.
maybe_acquire_advisory_lock(RepoMod, PoolMod) ->
    case kura_capabilities:has(PoolMod, advisory_locks) of
        true ->
            #{command := _} = kura_db:query(
                RepoMod,
                ~"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock",
                [?MIGRATION_LOCK_KEY]
            ),
            ok;
        false ->
            ok
    end.

-spec wait_for_pool(module()) -> ok | {error, pool_unavailable}.
wait_for_pool(RepoMod) ->
    Timeout = read_pos_int_env(migration_pool_ready_timeout, ?POOL_READY_TIMEOUT_MS),
    wait_for_pool(RepoMod, Timeout).

-spec wait_for_pool(module(), non_neg_integer()) -> ok | {error, pool_unavailable}.
wait_for_pool(RepoMod, Timeout) ->
    Interval = read_pos_int_env(migration_pool_ready_interval, ?POOL_READY_INTERVAL_MS),
    Pool = kura_db:get_pool(RepoMod),
    Driver = kura_db:get_driver_module(RepoMod),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_pool_loop(Driver, Pool, Interval, Deadline).

-spec read_pos_int_env(atom(), non_neg_integer()) -> non_neg_integer().
read_pos_int_env(Key, Default) ->
    case application:get_env(kura, Key, Default) of
        N when is_integer(N), N >= 0 -> N;
        _ -> Default
    end.

-spec wait_for_pool_loop(module(), atom(), non_neg_integer(), integer()) ->
    ok | {error, pool_unavailable}.
wait_for_pool_loop(Driver, Pool, Interval, Deadline) ->
    case probe_pool(Driver, Pool) of
        ok ->
            ok;
        {error, Reason} ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    logger:error(#{
                        msg => ~"kura_migrator pool not ready",
                        pool => Pool,
                        reason => Reason
                    }),
                    {error, pool_unavailable};
                false ->
                    timer:sleep(Interval),
                    wait_for_pool_loop(Driver, Pool, Interval, Deadline)
            end
    end.

-spec probe_pool(module(), atom()) -> ok | {error, term()}.
probe_pool(Driver, Pool) ->
    _ = code:ensure_loaded(Driver),
    case erlang:function_exported(Driver, probe_pool, 1) of
        true ->
            try Driver:probe_pool(Pool) of
                ok -> ok;
                {error, Reason} -> {error, Reason}
            catch
                Class:CatchReason -> {error, {Class, CatchReason}}
            end;
        false ->
            ok
    end.

%%----------------------------------------------------------------------
%% Migration discovery
%%----------------------------------------------------------------------

-spec discover_migrations(module()) -> [{integer(), module()}].
discover_migrations(RepoMod) ->
    case application:get_application(RepoMod) of
        {ok, App} ->
            discover_app_migrations(App);
        undefined ->
            []
    end.

-spec discover_app_migrations(atom()) -> [{integer(), module()}].
discover_app_migrations(App) ->
    Modules = get_app_modules(App),
    lists:filtermap(fun parse_migration_module/1, Modules).

-spec parse_migration_module(module()) -> {true, {integer(), module()}} | false.
parse_migration_module(Module) ->
    Name = atom_to_list(Module),
    case re:run(Name, "^m(\\d{14})_(.+)$", [{capture, [1, 2], list}]) of
        {match, [VersionStr, _Name]} ->
            Version = parse_version(VersionStr),
            {true, {Version, Module}};
        _ ->
            false
    end.

%%----------------------------------------------------------------------
%% Advisory lock
%%----------------------------------------------------------------------

-spec with_migration_lock(module(), fun(() -> [integer()])) ->
    {ok, [integer()]} | {error, term()}.
with_migration_lock(RepoMod, Fun) ->
    PoolMod = kura_db:get_pool_module(RepoMod),
    try
        narrow_migration_result(
            kura_db:transaction(RepoMod, fun() ->
                maybe_acquire_advisory_lock(RepoMod, PoolMod),
                Fun()
            end)
        )
    catch
        error:{migration_failed, Version, MigReason}:Stack ->
            logger:error(#{
                msg => ~"migration_failed",
                version => Version,
                reason => MigReason,
                stacktrace => Stack
            }),
            {error, {migration_failed, Version, MigReason}};
        Class:ExReason:Stack ->
            logger:error(#{
                msg => ~"migration_failed",
                class => Class,
                reason => ExReason,
                stacktrace => Stack
            }),
            {error, ExReason}
    end.

%%----------------------------------------------------------------------
%% Migration execution (runs inside advisory lock transaction)
%%----------------------------------------------------------------------

-spec run_migrations([{integer(), module()}], up | down, module(), [integer()]) -> [integer()].
run_migrations([], _Dir, _RepoMod, Acc) ->
    reverse_integers(Acc, []);
run_migrations([{Version, Module} | Rest], Dir, RepoMod, Acc) ->
    T0 = erlang:monotonic_time(),
    Ops = get_migration_ops(Module, Dir),
    SafeEntries = get_safe_entries(Module),
    Warnings = check_unsafe_operations(Ops, SafeEntries),
    log_warnings(Module, Warnings),
    exec_operations(Ops, Version, RepoMod),
    case Dir of
        up ->
            exec(
                ~"INSERT INTO schema_migrations (version) VALUES ($1)",
                [Version],
                Version,
                RepoMod
            );
        down ->
            exec(
                ~"DELETE FROM schema_migrations WHERE version = $1",
                [Version],
                Version,
                RepoMod
            )
    end,
    logger:info("Kura: ~s migration ~p (~p)", [Dir, Version, Module]),
    telemetry_event(
        [kura, migrator, migration, apply],
        #{duration => erlang:monotonic_time() - T0},
        #{
            version => Version,
            module => Module,
            direction => Dir,
            op_count => length(Ops)
        }
    ),
    run_migrations(Rest, Dir, RepoMod, [Version | Acc]).

-spec exec_operations([kura_migration:operation()], integer(), module()) -> ok.
exec_operations(Ops, Version, RepoMod) ->
    Sorted = topo_sort_ops(Ops),
    exec_operations_seq(Sorted, Version, RepoMod).

exec_operations_seq([], _Version, _RepoMod) ->
    ok;
exec_operations_seq([Op | Rest], Version, RepoMod) ->
    SQL = compile_operation(RepoMod, Op),
    exec(SQL, [], Version, RepoMod),
    exec_operations_seq(Rest, Version, RepoMod).

-spec exec(binary(), list(), integer(), module()) -> ok.
exec(SQL, Params, Version, RepoMod) ->
    case kura_db:query(RepoMod, SQL, Params) of
        #{command := _} ->
            ok;
        {error, Reason} ->
            error({migration_failed, Version, Reason})
    end.

%%----------------------------------------------------------------------
%% DDL compilation
%%----------------------------------------------------------------------

-doc "Compile a single DDL operation to SQL using `RepoMod`'s dialect.".
-spec compile_operation(module(), kura_migration:operation()) -> binary().
compile_operation(RepoMod, {create_table, Name, Columns}) ->
    compile_operation(RepoMod, {create_table, Name, Columns, []});
compile_operation(RepoMod, {create_table, Name, Columns, Constraints}) ->
    ColDefs = [compile_column_def(RepoMod, C) || C <- Columns],
    ConstraintDefs = [compile_table_constraint(TC) || TC <- Constraints],
    AllDefs = ColDefs ++ ConstraintDefs,
    iolist_to_binary([
        ~"CREATE TABLE ",
        quote(Name),
        ~" (\n  ",
        iolist_to_binary(lists:join(~",\n  ", AllDefs)),
        ~"\n)"
    ]);
compile_operation(_RepoMod, {drop_table, Name}) ->
    iolist_to_binary([~"DROP TABLE ", quote(Name)]);
compile_operation(RepoMod, {alter_table, Name, AlterOps}) ->
    Ops = [compile_alter_op(RepoMod, Op) || Op <- AlterOps],
    iolist_to_binary([
        ~"ALTER TABLE ",
        quote(Name),
        ~" ",
        iolist_to_binary(lists:join(~", ", Ops))
    ]);
compile_operation(_RepoMod, {create_index, Table, Columns, Opts}) when is_map(Opts) ->
    IdxName = kura_migration:index_name(Table, Columns),
    Unique =
        case maps:get(unique, Opts, false) of
            true -> ~"UNIQUE ";
            false -> <<>>
        end,
    Cols = iolist_to_binary(lists:join(~", ", [quote(atom_to_binary(C, utf8)) || C <- Columns])),
    Where =
        case maps:get(where, Opts, undefined) of
            undefined -> <<>>;
            Expr when is_binary(Expr) -> <<" WHERE ", Expr/binary>>
        end,
    iolist_to_binary([
        ~"CREATE ",
        Unique,
        ~"INDEX ",
        quote(IdxName),
        ~" ON ",
        quote(Table),
        ~" (",
        Cols,
        ~")",
        Where
    ]);
compile_operation(_RepoMod, {create_index, IdxName, Table, Columns, Opts}) ->
    Unique =
        case lists:member(unique, Opts) of
            true -> ~"UNIQUE ";
            false -> <<>>
        end,
    Cols = iolist_to_binary(lists:join(~", ", [quote(atom_to_binary(C, utf8)) || C <- Columns])),
    Where =
        case proplists:get_value(where, Opts) of
            undefined -> <<>>;
            Expr when is_binary(Expr) -> <<" WHERE ", Expr/binary>>
        end,
    iolist_to_binary([
        ~"CREATE ",
        Unique,
        ~"INDEX ",
        quote(IdxName),
        ~" ON ",
        quote(Table),
        ~" (",
        Cols,
        ~")",
        Where
    ]);
compile_operation(_RepoMod, {drop_index, Name}) ->
    iolist_to_binary([~"DROP INDEX ", quote(Name)]);
compile_operation(_RepoMod, {execute, SQL}) ->
    SQL.

-spec compile_column_def(module(), #kura_column{}) -> binary().
compile_column_def(RepoMod, #kura_column{
    name = Name,
    type = Type,
    nullable = Nullable,
    default = Default,
    primary_key = PK,
    references = Refs,
    on_delete = OnDelete,
    on_update = OnUpdate
}) ->
    NameBin = atom_to_binary(Name, utf8),
    TypeBin = column_type(RepoMod, Type),
    PKPart =
        case PK of
            true -> ~" PRIMARY KEY";
            false -> <<>>
        end,
    NullPart =
        case Nullable of
            true -> <<>>;
            false -> ~" NOT NULL"
        end,
    DefaultPart =
        case Default of
            undefined -> <<>>;
            Val -> <<" DEFAULT ", (format_default(RepoMod, Val))/binary>>
        end,
    RefsPart =
        case Refs of
            undefined ->
                <<>>;
            {Table, Col} ->
                <<" REFERENCES ", (quote(Table))/binary, "(",
                    (quote(atom_to_binary(Col, utf8)))/binary, ")">>
        end,
    OnDeletePart = compile_fk_action(~"ON DELETE", OnDelete),
    OnUpdatePart = compile_fk_action(~"ON UPDATE", OnUpdate),
    iolist_to_binary([
        quote(NameBin),
        ~" ",
        TypeBin,
        PKPart,
        NullPart,
        DefaultPart,
        RefsPart,
        OnDeletePart,
        OnUpdatePart
    ]).

-spec compile_table_constraint(kura_migration:table_constraint()) -> binary().
compile_table_constraint({unique, Cols}) ->
    ColList = iolist_to_binary(
        join_comma_iodata([quote(atom_to_binary(C, utf8)) || C <- Cols])
    ),
    <<"UNIQUE (", ColList/binary, ")">>;
compile_table_constraint({check, Expr}) ->
    <<"CHECK (", Expr/binary, ")">>.

-spec join_comma_iodata([binary()]) -> [binary()].
join_comma_iodata([]) -> [];
join_comma_iodata([H]) -> [H];
join_comma_iodata([H | T]) -> [H, ~", " | join_comma_iodata(T)].

-spec compile_fk_action(binary(), cascade | restrict | set_null | no_action | undefined) ->
    binary().
compile_fk_action(_Prefix, undefined) ->
    <<>>;
compile_fk_action(Prefix, cascade) ->
    <<" ", Prefix/binary, " CASCADE">>;
compile_fk_action(Prefix, restrict) ->
    <<" ", Prefix/binary, " RESTRICT">>;
compile_fk_action(Prefix, set_null) ->
    <<" ", Prefix/binary, " SET NULL">>;
compile_fk_action(Prefix, no_action) ->
    <<" ", Prefix/binary, " NO ACTION">>.

-spec compile_alter_op(module(), kura_migration:alter_op()) -> iodata().
compile_alter_op(RepoMod, {add_column, ColDef}) ->
    [~"ADD COLUMN ", compile_column_def(RepoMod, ColDef)];
compile_alter_op(_RepoMod, {drop_column, Name}) ->
    [~"DROP COLUMN ", quote(atom_to_binary(Name, utf8))];
compile_alter_op(_RepoMod, {rename_column, From, To}) ->
    [
        ~"RENAME COLUMN ",
        quote(atom_to_binary(From, utf8)),
        ~" TO ",
        quote(atom_to_binary(To, utf8))
    ];
compile_alter_op(RepoMod, {modify_column, Name, Type}) ->
    [
        ~"ALTER COLUMN ",
        quote(atom_to_binary(Name, utf8)),
        ~" TYPE ",
        column_type(RepoMod, Type)
    ].

%%----------------------------------------------------------------------
%% Unsafe operation detection
%%----------------------------------------------------------------------

-doc "Check migration operations for actions that are unsafe during rolling deployments.".
-spec check_unsafe_operations([kura_migration:operation()], [kura_migration:safe_entry()]) ->
    [map()].
check_unsafe_operations([], _SafeEntries) ->
    [];
check_unsafe_operations([Op | Rest], SafeEntries) ->
    check_op(Op, SafeEntries) ++ check_unsafe_operations(Rest, SafeEntries).

-spec check_op(kura_migration:operation(), [kura_migration:safe_entry()]) -> [map()].
check_op({drop_table, Table}, SafeEntries) ->
    case lists:member(drop_table, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => drop_table,
                    target => Table,
                    risk => ~"Old code still references this table",
                    safe_alt =>
                        ~"Deploy code that stops using the table first, then drop in a later migration"
                }
            ]
    end;
check_op({alter_table, Table, AlterOps}, SafeEntries) ->
    check_alter_ops(AlterOps, Table, SafeEntries);
check_op(_Op, _SafeEntries) ->
    [].

-spec check_alter_op(kura_migration:alter_op(), binary(), [kura_migration:safe_entry()]) -> [map()].
check_alter_op({drop_column, Col}, Table, SafeEntries) ->
    case lists:member({drop_column, Col}, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => drop_column,
                    target => Col,
                    table => Table,
                    risk => ~"Old code still queries this column",
                    safe_alt =>
                        ~"Deploy code that stops using the column first, then drop in a later migration"
                }
            ]
    end;
check_alter_op({rename_column, Old, _New}, Table, SafeEntries) ->
    case lists:member({rename_column, Old}, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => rename_column,
                    target => Old,
                    table => Table,
                    risk => ~"Old code still queries column by original name",
                    safe_alt =>
                        ~"Add a new column, backfill, deploy code using the new column, then drop the old one"
                }
            ]
    end;
check_alter_op({modify_column, Col, _Type}, Table, SafeEntries) ->
    case lists:member({modify_column, Col}, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => modify_column,
                    target => Col,
                    table => Table,
                    risk => ~"Type change may be incompatible with old code expectations",
                    safe_alt =>
                        ~"Add a new column with the new type, backfill, migrate reads, then drop the old column"
                }
            ]
    end;
check_alter_op(
    {add_column, #kura_column{name = Col, nullable = false, default = undefined}},
    Table,
    SafeEntries
) ->
    case lists:member({add_column, Col}, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => add_column_not_null,
                    target => Col,
                    table => Table,
                    risk =>
                        ~"Old code inserting rows without this column will fail the NOT NULL constraint",
                    safe_alt =>
                        ~"Add as nullable first, backfill, then set NOT NULL in a later migration"
                }
            ]
    end;
check_alter_op(_Op, _Table, _SafeEntries) ->
    [].

-spec get_safe_entries(module()) -> [kura_migration:safe_entry()].
get_safe_entries(Module) ->
    case erlang:function_exported(Module, safe, 0) of
        true -> Module:safe();
        false -> []
    end.

-spec log_warnings(module(), [map()]) -> ok.
log_warnings(_Module, []) ->
    ok;
log_warnings(Module, Warnings) ->
    lists:foreach(
        fun(#{op := Op, target := Target, safe_alt := SafeAlt} = Warning) ->
            logger:warning(
                "Kura: unsafe operation in ~p: ~s ~p~s - ~s",
                [
                    Module,
                    Op,
                    Target,
                    case Warning of
                        #{table := T} -> [" on ", T];
                        #{} -> ""
                    end,
                    SafeAlt
                ]
            )
        end,
        Warnings
    ).

%%----------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------

-spec get_applied_versions(module()) -> [integer()].
get_applied_versions(RepoMod) ->
    case kura_db:query(RepoMod, ~"SELECT version FROM schema_migrations ORDER BY version", []) of
        #{rows := Rows} ->
            [V || #{version := V} <- Rows];
        _ ->
            []
    end.

-spec quote(binary() | atom()) -> binary().
quote(Name) when is_binary(Name) ->
    <<$", Name/binary, $">>;
quote(Name) when is_atom(Name) ->
    quote(atom_to_binary(Name, utf8)).

%% Dispatch DDL emission through the repo's configured dialect, falling
%% back to PG when no dialect is configured. erlang:function_exported/3
%% only sees loaded modules, so ensure_loaded the dialect first.
-spec column_type(module(), kura_types:kura_type()) -> binary().
column_type(RepoMod, Type) ->
    case ddl_dialect(RepoMod) of
        {ok, M} ->
            _ = code:ensure_loaded(M),
            case erlang:function_exported(M, column_type, 1) of
                true -> M:column_type(Type);
                false -> kura_types:to_pg_type(Type)
            end;
        none ->
            kura_types:to_pg_type(Type)
    end.

-spec format_default(module(), term()) -> binary().
format_default(RepoMod, Val) ->
    case ddl_dialect(RepoMod) of
        {ok, M} ->
            _ = code:ensure_loaded(M),
            case erlang:function_exported(M, format_default, 1) of
                true -> M:format_default(Val);
                false -> default_default(Val)
            end;
        none ->
            default_default(Val)
    end.

-spec ddl_dialect(module()) -> {ok, module()} | none.
ddl_dialect(RepoMod) ->
    Cfg = kura_repo:config(RepoMod),
    case maps:get(dialect, Cfg, undefined) of
        M when is_atom(M), M =/= undefined ->
            {ok, M};
        _ ->
            case application:get_env(kura, dialect) of
                {ok, GD} when is_atom(GD) -> {ok, GD};
                _ -> none
            end
    end.

-spec default_default(term()) -> binary().
default_default(Val) when is_integer(Val) ->
    integer_to_binary(Val);
default_default(Val) when is_float(Val) ->
    float_to_binary(Val);
default_default(Val) when is_binary(Val) ->
    <<"'", Val/binary, "'">>;
default_default(true) ->
    ~"TRUE";
default_default(false) ->
    ~"FALSE";
default_default(Val) when is_atom(Val) ->
    <<"'", (atom_to_binary(Val))/binary, "'">>;
default_default(Val) when is_map(Val) ->
    Json = iolist_to_binary(json:encode(Val)),
    <<"'", Json/binary, "'::jsonb">>;
default_default(Val) when is_list(Val) ->
    Json = iolist_to_binary(json:encode(Val)),
    <<"'", Json/binary, "'::jsonb">>.

%%----------------------------------------------------------------------
%% Internal: type narrowing helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec narrow_migration_result(term()) -> {ok, [integer()]} | {error, term()}.
narrow_migration_result(Result) ->
    case Result of
        {error, Reason} -> {error, Reason};
        _ when is_list(Result) -> {ok, to_integer_list(Result, [])};
        _ -> {error, Result}
    end.

-spec to_integer_list([term()], [integer()]) -> [integer()].
to_integer_list([], Acc) -> reverse_integers(Acc, []);
to_integer_list([H | T], Acc) when is_integer(H) -> to_integer_list(T, [H | Acc]).

-spec get_migration_ops(module(), up | down) -> [kura_migration:operation()].
get_migration_ops(Module, up) -> Module:up();
get_migration_ops(Module, down) -> Module:down().

-spec get_app_modules(atom()) -> [module()].
get_app_modules(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} -> to_module_list(Modules);
        _ -> []
    end.

-spec to_module_list(term()) -> [module()].
to_module_list(L) when is_list(L) ->
    [M || M <- L, is_atom(M)];
to_module_list(_) ->
    [].

-spec parse_version(term()) -> integer().
parse_version(Str) when is_list(Str) ->
    list_to_integer([C || C <- Str, is_integer(C)]).

-spec reverse_integers([integer()], [integer()]) -> [integer()].
reverse_integers([], Acc) -> Acc;
reverse_integers([H | T], Acc) -> reverse_integers(T, [H | Acc]).

-spec sort_integers([integer()]) -> [integer()].
sort_integers([]) ->
    [];
sort_integers([Pivot | Rest]) ->
    {Less, Greater} = partition_ints(Pivot, Rest, [], []),
    sort_integers(Less) ++ [Pivot | sort_integers(Greater)].

-spec partition_ints(integer(), [integer()], [integer()], [integer()]) ->
    {[integer()], [integer()]}.
partition_ints(_Pivot, [], Less, Greater) ->
    {Less, Greater};
partition_ints(Pivot, [H | T], Less, Greater) ->
    case H =< Pivot of
        true -> partition_ints(Pivot, T, [H | Less], Greater);
        false -> partition_ints(Pivot, T, Less, [H | Greater])
    end.

-spec take_integers([integer()], non_neg_integer()) -> [integer()].
take_integers(_, 0) -> [];
take_integers([], _) -> [];
take_integers([H | T], N) -> [H | take_integers(T, N - 1)].

-spec sort_migrations([{integer(), module()}]) -> [{integer(), module()}].
sort_migrations([]) ->
    [];
sort_migrations([{V, M} | Rest]) ->
    {Less, Greater} = partition_migrations(V, Rest, [], []),
    sort_migrations(Less) ++ [{V, M} | sort_migrations(Greater)].

-spec partition_migrations(
    integer(),
    [{integer(), module()}],
    [{integer(), module()}],
    [{integer(), module()}]
) ->
    {[{integer(), module()}], [{integer(), module()}]}.
partition_migrations(_Pivot, [], Less, Greater) ->
    {Less, Greater};
partition_migrations(Pivot, [{V, M} | Rest], Less, Greater) ->
    case V =< Pivot of
        true -> partition_migrations(Pivot, Rest, [{V, M} | Less], Greater);
        false -> partition_migrations(Pivot, Rest, Less, [{V, M} | Greater])
    end.

-spec build_rollback_pairs([integer()], #{integer() => module()}) -> [{integer(), module()}].
build_rollback_pairs([], _MigMap) ->
    [];
build_rollback_pairs([V | Rest], MigMap) ->
    case MigMap of
        #{V := M} -> [{V, M} | build_rollback_pairs(Rest, MigMap)];
        #{} -> build_rollback_pairs(Rest, MigMap)
    end.

-spec check_alter_ops([kura_migration:alter_op()], binary(), [kura_migration:safe_entry()]) ->
    [map()].
check_alter_ops([], _Table, _SafeEntries) ->
    [];
check_alter_ops([AltOp | Rest], Table, SafeEntries) ->
    check_alter_op(AltOp, Table, SafeEntries) ++ check_alter_ops(Rest, Table, SafeEntries).

%%----------------------------------------------------------------------
%% Topological sort for create_table operations
%%----------------------------------------------------------------------

-spec topo_sort_ops([kura_migration:operation()]) -> [kura_migration:operation()].
topo_sort_ops(Ops) ->
    Creates = [Op || Op <- Ops, is_create_table(Op)],
    case Creates of
        [] ->
            Ops;
        _ ->
            Sorted = topo_sort_creates(Creates),
            replace_creates(Ops, Sorted)
    end.

is_create_table({create_table, _, _}) -> true;
is_create_table({create_table, _, _, _}) -> true;
is_create_table(_) -> false.

table_name({create_table, Name, _}) -> Name;
table_name({create_table, Name, _, _}) -> Name.

table_columns({create_table, _, Cols}) -> Cols;
table_columns({create_table, _, Cols, _}) -> Cols.

table_deps(Op) ->
    Name = table_name(Op),
    Cols = table_columns(Op),
    lists:usort([T || #kura_column{references = {T, _}} <- Cols, T =/= Name]).

-spec topo_sort_creates([kura_migration:operation()]) -> [kura_migration:operation()].
topo_sort_creates(Creates) ->
    ByName = #{table_name(C) => C || C <- Creates},
    DepMap = #{table_name(C) => table_deps(C) || C <- Creates},
    AllNames = maps:keys(ByName),
    InDeg = build_indeg(AllNames, DepMap, ByName, #{}),
    Queue = [N || N <- AllNames, maps:get(N, InDeg) =:= 0],
    kahn_loop(Queue, InDeg, DepMap, ByName, []).

-spec build_indeg([binary()], #{binary() => [binary()]}, #{binary() => term()}, #{
    binary() => non_neg_integer()
}) -> #{binary() => non_neg_integer()}.
build_indeg([], _DepMap, _ByName, Acc) ->
    Acc;
build_indeg([Name | Rest], DepMap, ByName, Acc) ->
    Deps = maps:get(Name, DepMap),
    Count = length([D || D <- Deps, maps:is_key(D, ByName)]),
    build_indeg(Rest, DepMap, ByName, Acc#{Name => Count}).

-spec kahn_loop(
    [binary()],
    #{binary() => non_neg_integer()},
    #{binary() => [binary()]},
    #{binary() => kura_migration:operation()},
    [kura_migration:operation()]
) -> [kura_migration:operation()].
kahn_loop([], _InDeg, _DepMap, _ByName, Acc) ->
    reverse_ops(Acc, []);
kahn_loop([Name | Rest], InDeg, DepMap, ByName, Acc) ->
    Op = maps:get(Name, ByName),
    Dependents = [
        N
     || N <- maps:keys(ByName),
        lists:member(Name, maps:get(N, DepMap, []))
    ],
    {NewQueue, NewInDeg} = decrement_deps(Dependents, Rest, InDeg),
    kahn_loop(NewQueue, NewInDeg, DepMap, ByName, [Op | Acc]).

-spec reverse_ops([kura_migration:operation()], [kura_migration:operation()]) ->
    [kura_migration:operation()].
reverse_ops([], Acc) -> Acc;
reverse_ops([H | T], Acc) -> reverse_ops(T, [H | Acc]).

-spec decrement_deps([binary()], [binary()], #{binary() => non_neg_integer()}) ->
    {[binary()], #{binary() => non_neg_integer()}}.
decrement_deps([], Q, ID) ->
    {Q, ID};
decrement_deps([Dep | Rest], Q, ID) ->
    NewDeg = maps:get(Dep, ID) - 1,
    ID1 = ID#{Dep => NewDeg},
    case NewDeg of
        0 -> decrement_deps(Rest, Q ++ [Dep], ID1);
        _ -> decrement_deps(Rest, Q, ID1)
    end.

replace_creates([], _Sorted) ->
    [];
replace_creates([{create_table, _, _} | Rest], [Next | Sorted]) ->
    [Next | replace_creates(Rest, Sorted)];
replace_creates([{create_table, _, _, _} | Rest], [Next | Sorted]) ->
    [Next | replace_creates(Rest, Sorted)];
replace_creates([Op | Rest], Sorted) ->
    [Op | replace_creates(Rest, Sorted)].
