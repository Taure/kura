-module(kura_migrator).
-moduledoc """
Runs, rolls back, and reports status of migrations.

Discovers migration modules from configured paths, tracks applied versions
in a `schema_migrations` table, and executes DDL within transactions.
""".

-include("kura.hrl").

-export([
    migrate/1,
    rollback/1, rollback/2,
    status/1,
    ensure_schema_migrations/1,
    compile_operation/1
]).

-doc "Run all pending migrations in order.".
-spec migrate(module()) -> {ok, [integer()]} | {error, term()}.
migrate(RepoMod) ->
    ensure_schema_migrations(RepoMod),
    Applied = get_applied_versions(RepoMod),
    Migrations = discover_migrations(),
    Pending = [{V, M} || {V, M} <- Migrations, not lists:member(V, Applied)],
    Sorted = lists:sort(fun({V1, _}, {V2, _}) -> V1 =< V2 end, Pending),
    run_migrations(RepoMod, Sorted, up, []).

-doc "Roll back the last migration.".
-spec rollback(module()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod) ->
    rollback(RepoMod, 1).

-doc "Roll back the last `Steps` migrations.".
-spec rollback(module(), non_neg_integer()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod, Steps) ->
    ensure_schema_migrations(RepoMod),
    Applied = get_applied_versions(RepoMod),
    Migrations = discover_migrations(),
    ToRollback = lists:sublist(lists:reverse(lists:sort(Applied)), Steps),
    MigMap = maps:from_list(Migrations),
    Pairs = [{V, maps:get(V, MigMap)} || V <- ToRollback, maps:is_key(V, MigMap)],
    run_migrations(RepoMod, Pairs, down, []).

-doc "Return the status of all discovered migrations (`:up` or `:pending`).".
-spec status(module()) -> [{integer(), module(), up | pending}].
status(RepoMod) ->
    ensure_schema_migrations(RepoMod),
    Applied = get_applied_versions(RepoMod),
    Migrations = discover_migrations(),
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

discover_migrations() ->
    Paths = application:get_env(kura, migration_paths, [<<"priv/migrations">>]),
    lists:flatmap(fun discover_in_path/1, Paths).

discover_in_path({priv_dir, App, SubDir}) ->
    case code:priv_dir(App) of
        {error, bad_name} -> [];
        PrivDir -> discover_in_path(filename:join(PrivDir, SubDir))
    end;
discover_in_path(Path) when is_binary(Path) ->
    discover_in_path(binary_to_list(Path));
discover_in_path(Path) ->
    case filelib:wildcard(Path ++ "/m*.beam") of
        [] ->
            case filelib:wildcard(Path ++ "/m*.erl") of
                [] -> [];
                ErlFiles -> parse_migration_files(ErlFiles)
            end;
        BeamFiles ->
            parse_migration_files(BeamFiles)
    end.

parse_migration_files(Files) ->
    lists:filtermap(
        fun(File) ->
            BaseName = filename:basename(File, filename:extension(File)),
            case re:run(BaseName, "^m(\\d{14})_(.+)$", [{capture, [1, 2], list}]) of
                {match, [VersionStr, _Name]} ->
                    Version = list_to_integer(VersionStr),
                    Module = list_to_atom(BaseName),
                    {true, {Version, Module}};
                nomatch ->
                    false
            end
        end,
        Files
    ).

%%----------------------------------------------------------------------
%% Migration execution
%%----------------------------------------------------------------------

run_migrations(_RepoMod, [], _Dir, Acc) ->
    {ok, lists:reverse(Acc)};
run_migrations(RepoMod, [{Version, Module} | Rest], Dir, Acc) ->
    Pool = get_pool(RepoMod),
    PoolOpts = #{pool => Pool},
    case
        pgo:transaction(
            fun() ->
                _ = code:ensure_loaded(Module),
                Ops =
                    case Dir of
                        up -> Module:up();
                        down -> Module:down()
                    end,
                lists:foreach(
                    fun(Op) ->
                        SQL = compile_operation(Op),
                        pgo:query(SQL, [], PoolOpts)
                    end,
                    Ops
                ),
                case Dir of
                    up ->
                        pgo:query(
                            <<"INSERT INTO schema_migrations (version) VALUES ($1)">>,
                            [Version],
                            PoolOpts
                        );
                    down ->
                        pgo:query(
                            <<"DELETE FROM schema_migrations WHERE version = $1">>,
                            [Version],
                            PoolOpts
                        )
                end
            end,
            PoolOpts
        )
    of
        {ok, _} ->
            logger:info("Kura: ~s migration ~p (~p)", [Dir, Version, Module]),
            run_migrations(RepoMod, Rest, Dir, [Version | Acc]);
        #{command := _} ->
            logger:info("Kura: ~s migration ~p (~p)", [Dir, Version, Module]),
            run_migrations(RepoMod, Rest, Dir, [Version | Acc]);
        {error, Reason} ->
            logger:error("Kura: migration ~p failed: ~p", [Version, Reason]),
            {error, {migration_failed, Version, Reason}}
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
