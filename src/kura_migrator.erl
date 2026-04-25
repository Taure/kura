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
    compile_operation/1,
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

%% eqWAlizer: operation() union has >7 variants — exceeds clause narrowing limit
-eqwalizer({nowarn_function, compile_operation/1}).

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
            Result = with_migration_lock(RepoMod, fun(PoolOpts) ->
                run_migrations(Sorted, up, PoolOpts, [])
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
            Result = with_migration_lock(RepoMod, fun(PoolOpts) ->
                run_migrations(Pairs, down, PoolOpts, [])
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
    Config = kura_repo:config(RepoMod),
    case maps:find(database, Config) of
        error -> ok;
        {ok, DbName} -> do_ensure_database(Config, binary_to_list(DbName))
    end.

do_ensure_database(Config, Database) ->
    TmpPool = kura_migrator_tmp_pool,
    TmpBase = base_pool_config(Config, Database),
    TmpConfig = apply_pool_extras(TmpBase),
    case try_connect(TmpPool, TmpConfig) of
        ok ->
            stop_tmp_pool(TmpPool);
        {error, _} ->
            stop_tmp_pool(TmpPool),
            create_database(Config, Database)
    end.

base_pool_config(Config, Database) ->
    #{
        host => binary_to_list(maps:get(hostname, Config, ~"localhost")),
        port => maps:get(port, Config, 5432),
        database => Database,
        user => binary_to_list(maps:get(username, Config, ~"postgres")),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => 1,
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    }.

apply_pool_extras(Base) ->
    WithSocket =
        case application:get_env(kura, socket_options, []) of
            Opts when is_list(Opts), Opts =/= [] -> Base#{socket_options => Opts};
            _ -> Base
        end,
    WithSSL =
        case application:get_env(kura, ssl, false) of
            true -> WithSocket#{ssl => true};
            _ -> WithSocket
        end,
    case application:get_env(kura, ssl_options, []) of
        SSLOpts when is_list(SSLOpts), SSLOpts =/= [] ->
            WithSSL#{ssl_options => SSLOpts};
        _ ->
            WithSSL
    end.

try_connect(TmpPool, TmpConfig) ->
    case pgo_sup:start_child(TmpPool, TmpConfig) of
        {ok, _} ->
            case pgo:query(~"SELECT 1", [], #{pool => TmpPool}) of
                #{rows := _} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

create_database(Config, Database) ->
    TmpPool = kura_migrator_create_pool,
    TmpBase = base_pool_config(Config, "postgres"),
    TmpConfig = apply_pool_extras(TmpBase),
    case pgo_sup:start_child(TmpPool, TmpConfig) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    DbBin = list_to_binary(Database),
    QuotedDb = iolist_to_binary([<<"\"">>, DbBin, <<"\"">>]),
    SQL = iolist_to_binary([~"CREATE DATABASE ", QuotedDb]),
    _ = pgo:query(SQL, [], #{pool => TmpPool}),
    logger:info("Kura: created database ~s", [Database]),
    stop_tmp_pool(TmpPool).

-spec stop_tmp_pool(atom()) -> ok.
stop_tmp_pool(TmpPool) ->
    _ = supervisor:terminate_child(pgo_sup, TmpPool),
    _ = supervisor:delete_child(pgo_sup, TmpPool),
    ok.

%%----------------------------------------------------------------------
%% Schema migrations table
%%----------------------------------------------------------------------

-spec ensure_schema_migrations(module()) -> ok.
ensure_schema_migrations(RepoMod) ->
    Pool = kura_db:get_pool(RepoMod),
    SQL = <<
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version BIGINT PRIMARY KEY, "
        "inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()"
        ")"
    >>,
    _ = pgo:query(SQL, [], #{pool => Pool}),
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
-spec wait_for_pool(module()) -> ok | {error, pool_unavailable}.
wait_for_pool(RepoMod) ->
    Timeout = read_pos_int_env(migration_pool_ready_timeout, ?POOL_READY_TIMEOUT_MS),
    wait_for_pool(RepoMod, Timeout).

-spec wait_for_pool(module(), non_neg_integer()) -> ok | {error, pool_unavailable}.
wait_for_pool(RepoMod, Timeout) ->
    Interval = read_pos_int_env(migration_pool_ready_interval, ?POOL_READY_INTERVAL_MS),
    Pool = kura_db:get_pool(RepoMod),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_pool_loop(Pool, Interval, Deadline).

-spec read_pos_int_env(atom(), non_neg_integer()) -> non_neg_integer().
read_pos_int_env(Key, Default) ->
    case application:get_env(kura, Key, Default) of
        N when is_integer(N), N >= 0 -> N;
        _ -> Default
    end.

-spec wait_for_pool_loop(atom(), non_neg_integer(), integer()) ->
    ok | {error, pool_unavailable}.
wait_for_pool_loop(Pool, Interval, Deadline) ->
    case probe_pool(Pool) of
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
                    wait_for_pool_loop(Pool, Interval, Deadline)
            end
    end.

-spec probe_pool(atom()) -> ok | {error, term()}.
probe_pool(Pool) ->
    try pgo:query(~"SELECT 1", [], #{pool => Pool}) of
        #{rows := _} -> ok;
        {error, Reason} -> {error, Reason}
    catch
        Class:CatchReason -> {error, {Class, CatchReason}}
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

-spec with_migration_lock(module(), fun((map()) -> [integer()])) ->
    {ok, [integer()]} | {error, term()}.
with_migration_lock(RepoMod, Fun) ->
    Pool = kura_db:get_pool(RepoMod),
    PoolOpts = #{pool => Pool},
    try
        narrow_migration_result(
            pgo:transaction(
                fun() ->
                    #{command := _} = pgo:query(
                        ~"SELECT 1 FROM (SELECT pg_advisory_xact_lock($1)) AS _lock",
                        [?MIGRATION_LOCK_KEY],
                        PoolOpts
                    ),
                    Fun(PoolOpts)
                end,
                PoolOpts
            )
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

-spec run_migrations([{integer(), module()}], up | down, map(), [integer()]) -> [integer()].
run_migrations([], _Dir, _PoolOpts, Acc) ->
    reverse_integers(Acc, []);
run_migrations([{Version, Module} | Rest], Dir, PoolOpts, Acc) ->
    T0 = erlang:monotonic_time(),
    Ops = get_migration_ops(Module, Dir),
    SafeEntries = get_safe_entries(Module),
    Warnings = check_unsafe_operations(Ops, SafeEntries),
    log_warnings(Module, Warnings),
    exec_operations(Ops, Version, PoolOpts),
    case Dir of
        up ->
            exec(
                ~"INSERT INTO schema_migrations (version) VALUES ($1)",
                [Version],
                Version,
                PoolOpts
            );
        down ->
            exec(
                ~"DELETE FROM schema_migrations WHERE version = $1",
                [Version],
                Version,
                PoolOpts
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
    run_migrations(Rest, Dir, PoolOpts, [Version | Acc]).

-spec exec_operations([kura_migration:operation()], integer(), map()) -> ok.
exec_operations(Ops, Version, PoolOpts) ->
    Sorted = topo_sort_ops(Ops),
    exec_operations_seq(Sorted, Version, PoolOpts).

exec_operations_seq([], _Version, _PoolOpts) ->
    ok;
exec_operations_seq([Op | Rest], Version, PoolOpts) ->
    SQL = compile_operation(Op),
    exec(SQL, [], Version, PoolOpts),
    exec_operations_seq(Rest, Version, PoolOpts).

-spec exec(binary(), list(), integer(), map()) -> ok.
exec(SQL, Params, Version, PoolOpts) ->
    case pgo:query(SQL, Params, PoolOpts) of
        #{command := _} ->
            ok;
        {error, Reason} ->
            error({migration_failed, Version, Reason})
    end.

%%----------------------------------------------------------------------
%% DDL compilation
%%----------------------------------------------------------------------

-doc "Compile a single DDL operation to SQL.".
-spec compile_operation(kura_migration:operation()) -> binary().
compile_operation({create_table, Name, Columns}) ->
    compile_operation({create_table, Name, Columns, []});
compile_operation({create_table, Name, Columns, Constraints}) ->
    ColDefs = [compile_column_def(C) || C <- Columns],
    ConstraintDefs = [compile_table_constraint(TC) || TC <- Constraints],
    AllDefs = ColDefs ++ ConstraintDefs,
    iolist_to_binary([
        ~"CREATE TABLE ",
        quote(Name),
        ~" (\n  ",
        iolist_to_binary(lists:join(~",\n  ", AllDefs)),
        ~"\n)"
    ]);
compile_operation({drop_table, Name}) ->
    iolist_to_binary([~"DROP TABLE ", quote(Name)]);
compile_operation({alter_table, Name, AlterOps}) ->
    Ops = [compile_alter_op(Op) || Op <- AlterOps],
    iolist_to_binary([
        ~"ALTER TABLE ",
        quote(Name),
        ~" ",
        iolist_to_binary(lists:join(~", ", Ops))
    ]);
compile_operation({create_index, Table, Columns, Opts}) when is_map(Opts) ->
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
compile_operation({create_index, IdxName, Table, Columns, Opts}) ->
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
compile_operation({drop_index, Name}) ->
    iolist_to_binary([~"DROP INDEX ", quote(Name)]);
compile_operation({execute, SQL}) ->
    SQL.

-spec compile_column_def(#kura_column{}) -> binary().
compile_column_def(#kura_column{
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
    TypeBin = kura_types:to_pg_type(Type),
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
            Val -> <<" DEFAULT ", (format_default(Val))/binary>>
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

-spec compile_alter_op(kura_migration:alter_op()) -> iodata().
compile_alter_op({add_column, ColDef}) ->
    [~"ADD COLUMN ", compile_column_def(ColDef)];
compile_alter_op({drop_column, Name}) ->
    [~"DROP COLUMN ", quote(atom_to_binary(Name, utf8))];
compile_alter_op({rename_column, From, To}) ->
    [
        ~"RENAME COLUMN ",
        quote(atom_to_binary(From, utf8)),
        ~" TO ",
        quote(atom_to_binary(To, utf8))
    ];
compile_alter_op({modify_column, Name, Type}) ->
    [
        ~"ALTER COLUMN ",
        quote(atom_to_binary(Name, utf8)),
        ~" TYPE ",
        kura_types:to_pg_type(Type)
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
                "Kura: unsafe operation in ~p: ~s ~p~s — ~s",
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
    Pool = kura_db:get_pool(RepoMod),
    case
        pgo:query(~"SELECT version FROM schema_migrations ORDER BY version", [], #{pool => Pool})
    of
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

-spec format_default(term()) -> binary().
format_default(Val) when is_integer(Val) ->
    integer_to_binary(Val);
format_default(Val) when is_float(Val) ->
    float_to_binary(Val);
format_default(Val) when is_binary(Val) ->
    <<"'", Val/binary, "'">>;
format_default(true) ->
    ~"TRUE";
format_default(false) ->
    ~"FALSE";
format_default(Val) when is_map(Val) ->
    <<"'", (json:encode(Val))/binary, "'::jsonb">>;
format_default(Val) when is_list(Val) ->
    <<"'", (json:encode(Val))/binary, "'::jsonb">>.

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

topo_sort_creates(Creates) ->
    ByName = maps:from_list([{table_name(C), C} || C <- Creates]),
    DepMap = maps:from_list([{table_name(C), table_deps(C)} || C <- Creates]),
    AllNames = maps:keys(ByName),
    InDeg = lists:foldl(
        fun(Name, Acc) ->
            Deps = maps:get(Name, DepMap),
            Count = length([D || D <- Deps, maps:is_key(D, ByName)]),
            Acc#{Name => Count}
        end,
        #{},
        AllNames
    ),
    Queue = [N || N <- AllNames, maps:get(N, InDeg) =:= 0],
    kahn_loop(Queue, InDeg, DepMap, ByName, []).

kahn_loop([], _InDeg, _DepMap, _ByName, Acc) ->
    lists:reverse(Acc);
kahn_loop([Name | Rest], InDeg, DepMap, ByName, Acc) ->
    Op = maps:get(Name, ByName),
    Dependents = [
        N
     || N <- maps:keys(ByName),
        lists:member(Name, maps:get(N, DepMap, []))
    ],
    {NewQueue, NewInDeg} = lists:foldl(
        fun(Dep, {Q, ID}) ->
            NewDeg = maps:get(Dep, ID) - 1,
            ID1 = ID#{Dep => NewDeg},
            case NewDeg of
                0 -> {Q ++ [Dep], ID1};
                _ -> {Q, ID1}
            end
        end,
        {Rest, InDeg},
        Dependents
    ),
    kahn_loop(NewQueue, NewInDeg, DepMap, ByName, [Op | Acc]).

replace_creates([], _Sorted) ->
    [];
replace_creates([{create_table, _, _} | Rest], [Next | Sorted]) ->
    [Next | replace_creates(Rest, Sorted)];
replace_creates([{create_table, _, _, _} | Rest], [Next | Sorted]) ->
    [Next | replace_creates(Rest, Sorted)];
replace_creates([Op | Rest], Sorted) ->
    [Op | replace_creates(Rest, Sorted)].
