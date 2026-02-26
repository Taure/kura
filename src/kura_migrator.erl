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

-export([
    migrate/1,
    rollback/1, rollback/2,
    status/1,
    ensure_schema_migrations/1,
    compile_operation/1,
    check_unsafe_operations/2
]).

-doc "Run all pending migrations in order.".
-spec migrate(module()) -> {ok, [integer()]} | {error, term()}.
migrate(RepoMod) ->
    ensure_schema_migrations(RepoMod),
    with_migration_lock(RepoMod, fun(PoolOpts) ->
        Applied = get_applied_versions(RepoMod),
        Migrations = discover_migrations(RepoMod),
        Pending = [{V, M} || {V, M} <- Migrations, not lists:member(V, Applied)],
        Sorted = lists:sort(fun({V1, _}, {V2, _}) -> V1 =< V2 end, Pending),
        run_migrations(Sorted, up, PoolOpts, [])
    end).

-doc "Roll back the last migration.".
-spec rollback(module()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod) ->
    rollback(RepoMod, 1).

-doc "Roll back the last `Steps` migrations.".
-spec rollback(module(), non_neg_integer()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod, Steps) ->
    ensure_schema_migrations(RepoMod),
    with_migration_lock(RepoMod, fun(PoolOpts) ->
        Applied = get_applied_versions(RepoMod),
        Migrations = discover_migrations(RepoMod),
        ToRollback = lists:sublist(lists:reverse(lists:sort(Applied)), Steps),
        MigMap = maps:from_list(Migrations),
        Pairs = [{V, maps:get(V, MigMap)} || V <- ToRollback, maps:is_key(V, MigMap)],
        run_migrations(Pairs, down, PoolOpts, [])
    end).

-doc "Return the status of all discovered migrations (`:up` or `:pending`).".
-spec status(module()) -> [{integer(), module(), up | pending}].
status(RepoMod) ->
    ensure_schema_migrations(RepoMod),
    Applied = get_applied_versions(RepoMod),
    Migrations = discover_migrations(RepoMod),
    [
        {V, M,
            case lists:member(V, Applied) of
                true -> up;
                false -> pending
            end}
     || {V, M} <- lists:sort(fun({V1, _}, {V2, _}) -> V1 =< V2 end, Migrations)
    ].

%%----------------------------------------------------------------------
%% Schema migrations table
%%----------------------------------------------------------------------

-spec ensure_schema_migrations(module()) -> ok.
ensure_schema_migrations(RepoMod) ->
    Pool = get_pool(RepoMod),
    SQL = <<
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version BIGINT PRIMARY KEY, "
        "inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()"
        ")"
    >>,
    _ = pgo:query(SQL, [], #{pool => Pool}),
    ok.

%%----------------------------------------------------------------------
%% Migration discovery
%%----------------------------------------------------------------------

discover_migrations(RepoMod) ->
    case application:get_application(RepoMod) of
        {ok, App} ->
            discover_app_migrations(App);
        undefined ->
            []
    end.

discover_app_migrations(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            lists:filtermap(fun parse_migration_module/1, Modules);
        undefined ->
            []
    end.

parse_migration_module(Module) ->
    Name = atom_to_list(Module),
    case re:run(Name, "^m(\\d{14})_(.+)$", [{capture, [1, 2], list}]) of
        {match, [VersionStr, _Name]} ->
            Version = list_to_integer(VersionStr),
            {true, {Version, Module}};
        nomatch ->
            false
    end.

%%----------------------------------------------------------------------
%% Advisory lock
%%----------------------------------------------------------------------

-spec with_migration_lock(module(), fun((map()) -> [integer()])) ->
    {ok, [integer()]} | {error, term()}.
with_migration_lock(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    PoolOpts = #{pool => Pool},
    try
        pgo:transaction(
            fun() ->
                #{command := _} = pgo:query(
                    <<"SELECT pg_advisory_xact_lock($1)">>,
                    [?MIGRATION_LOCK_KEY],
                    PoolOpts
                ),
                Fun(PoolOpts)
            end,
            PoolOpts
        )
    of
        Versions when is_list(Versions) ->
            {ok, Versions};
        {error, Reason} ->
            {error, Reason}
    catch
        error:{migration_failed, Version, MigReason}:_ ->
            logger:error("Kura: migration ~p failed: ~p", [Version, MigReason]),
            {error, {migration_failed, Version, MigReason}};
        _:ExReason:_ ->
            logger:error("Kura: migration failed: ~p", [ExReason]),
            {error, ExReason}
    end.

%%----------------------------------------------------------------------
%% Migration execution (runs inside advisory lock transaction)
%%----------------------------------------------------------------------

run_migrations([], _Dir, _PoolOpts, Acc) ->
    lists:reverse(Acc);
run_migrations([{Version, Module} | Rest], Dir, PoolOpts, Acc) ->
    Ops =
        case Dir of
            up -> Module:up();
            down -> Module:down()
        end,
    SafeEntries = get_safe_entries(Module),
    Warnings = check_unsafe_operations(Ops, SafeEntries),
    log_warnings(Module, Warnings),
    lists:foreach(
        fun(Op) ->
            SQL = compile_operation(Op),
            exec(SQL, [], Version, PoolOpts)
        end,
        Ops
    ),
    case Dir of
        up ->
            exec(
                <<"INSERT INTO schema_migrations (version) VALUES ($1)">>,
                [Version],
                Version,
                PoolOpts
            );
        down ->
            exec(
                <<"DELETE FROM schema_migrations WHERE version = $1">>,
                [Version],
                Version,
                PoolOpts
            )
    end,
    logger:info("Kura: ~s migration ~p (~p)", [Dir, Version, Module]),
    run_migrations(Rest, Dir, PoolOpts, [Version | Acc]).

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
    ColDefs = [compile_column_def(C) || C <- Columns],
    iolist_to_binary([
        <<"CREATE TABLE ">>,
        quote(Name),
        <<" (\n  ">>,
        lists:join(<<",\n  ">>, ColDefs),
        <<"\n)">>
    ]);
compile_operation({drop_table, Name}) ->
    iolist_to_binary([<<"DROP TABLE ">>, quote(Name)]);
compile_operation({alter_table, Name, AlterOps}) ->
    Ops = [compile_alter_op(Op) || Op <- AlterOps],
    iolist_to_binary([
        <<"ALTER TABLE ">>,
        quote(Name),
        <<" ">>,
        lists:join(<<", ">>, Ops)
    ]);
compile_operation({create_index, IdxName, Table, Columns, Opts}) ->
    Unique =
        case lists:member(unique, Opts) of
            true -> <<"UNIQUE ">>;
            false -> <<>>
        end,
    Cols = lists:join(<<", ">>, [quote(atom_to_binary(C, utf8)) || C <- Columns]),
    Where =
        case proplists:get_value(where, Opts) of
            undefined -> <<>>;
            Expr -> <<" WHERE ", Expr/binary>>
        end,
    iolist_to_binary([
        <<"CREATE ">>,
        Unique,
        <<"INDEX ">>,
        quote(IdxName),
        <<" ON ">>,
        quote(Table),
        <<" (">>,
        Cols,
        <<")">>,
        Where
    ]);
compile_operation({drop_index, Name}) ->
    iolist_to_binary([<<"DROP INDEX ">>, quote(Name)]);
compile_operation({execute, SQL}) ->
    SQL.

compile_column_def(#kura_column{
    name = Name,
    type = Type,
    nullable = Nullable,
    default = Default,
    primary_key = PK
}) ->
    NameBin = atom_to_binary(Name, utf8),
    TypeBin = kura_types:to_pg_type(Type),
    PKPart =
        case PK of
            true -> <<" PRIMARY KEY">>;
            false -> <<>>
        end,
    NullPart =
        case Nullable of
            true -> <<>>;
            false -> <<" NOT NULL">>
        end,
    DefaultPart =
        case Default of
            undefined -> <<>>;
            Val -> <<" DEFAULT ", (format_default(Val))/binary>>
        end,
    iolist_to_binary([quote(NameBin), <<" ">>, TypeBin, PKPart, NullPart, DefaultPart]).

compile_alter_op({add_column, ColDef}) ->
    [<<"ADD COLUMN ">>, compile_column_def(ColDef)];
compile_alter_op({drop_column, Name}) ->
    [<<"DROP COLUMN ">>, quote(atom_to_binary(Name, utf8))];
compile_alter_op({rename_column, From, To}) ->
    [
        <<"RENAME COLUMN ">>,
        quote(atom_to_binary(From, utf8)),
        <<" TO ">>,
        quote(atom_to_binary(To, utf8))
    ];
compile_alter_op({modify_column, Name, Type}) ->
    [
        <<"ALTER COLUMN ">>,
        quote(atom_to_binary(Name, utf8)),
        <<" TYPE ">>,
        kura_types:to_pg_type(Type)
    ].

%%----------------------------------------------------------------------
%% Unsafe operation detection
%%----------------------------------------------------------------------

-doc "Check migration operations for actions that are unsafe during rolling deployments.".
-spec check_unsafe_operations([kura_migration:operation()], [kura_migration:safe_entry()]) ->
    [map()].
check_unsafe_operations(Ops, SafeEntries) ->
    lists:flatmap(fun(Op) -> check_op(Op, SafeEntries) end, Ops).

check_op({drop_table, Table}, SafeEntries) ->
    case lists:member(drop_table, SafeEntries) of
        true ->
            [];
        false ->
            [
                #{
                    op => drop_table,
                    target => Table,
                    risk => <<"Old code still references this table">>,
                    safe_alt =>
                        <<"Deploy code that stops using the table first, then drop in a later migration">>
                }
            ]
    end;
check_op({alter_table, Table, AlterOps}, SafeEntries) ->
    lists:flatmap(fun(AltOp) -> check_alter_op(AltOp, Table, SafeEntries) end, AlterOps);
check_op(_Op, _SafeEntries) ->
    [].

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
                    risk => <<"Old code still queries this column">>,
                    safe_alt =>
                        <<"Deploy code that stops using the column first, then drop in a later migration">>
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
                    risk => <<"Old code still queries column by original name">>,
                    safe_alt =>
                        <<"Add a new column, backfill, deploy code using the new column, then drop the old one">>
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
                    risk => <<"Type change may be incompatible with old code expectations">>,
                    safe_alt =>
                        <<"Add a new column with the new type, backfill, migrate reads, then drop the old column">>
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
                        <<"Old code inserting rows without this column will fail the NOT NULL constraint">>,
                    safe_alt =>
                        <<"Add as nullable first, backfill, then set NOT NULL in a later migration">>
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
        fun(Warning) ->
            logger:warning(
                "Kura: unsafe operation in ~p: ~s ~p~s — ~s",
                [
                    Module,
                    maps:get(op, Warning),
                    maps:get(target, Warning),
                    case maps:find(table, Warning) of
                        {ok, T} -> [" on ", T];
                        error -> ""
                    end,
                    maps:get(safe_alt, Warning)
                ]
            )
        end,
        Warnings
    ).

%%----------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------

get_pool(RepoMod) ->
    Config = RepoMod:config(),
    maps:get(pool, Config, RepoMod).

get_applied_versions(RepoMod) ->
    Pool = get_pool(RepoMod),
    case
        pgo:query(<<"SELECT version FROM schema_migrations ORDER BY version">>, [], #{pool => Pool})
    of
        #{rows := Rows} ->
            [V || #{version := V} <- Rows];
        _ ->
            []
    end.

quote(Name) when is_binary(Name) ->
    <<$", Name/binary, $">>;
quote(Name) when is_atom(Name) ->
    quote(atom_to_binary(Name, utf8)).

format_default(Val) when is_integer(Val) ->
    integer_to_binary(Val);
format_default(Val) when is_float(Val) ->
    float_to_binary(Val);
format_default(Val) when is_binary(Val) ->
    <<"'", Val/binary, "'">>;
format_default(true) ->
    <<"TRUE">>;
format_default(false) ->
    <<"FALSE">>.
