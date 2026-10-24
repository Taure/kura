-module(kura_schema_verify).
-moduledoc """
Runtime verification that declared schemas match the live database.

Schemas declare their structure via `c:kura_schema:fields/0` and
`c:kura_schema:indexes/0`. Migrations are *supposed* to bring the
database into agreement with those declarations — but every step in
that chain has been a silent failure mode in production:

- a generated migration was forgotten and never committed,
- a hand-written migration had a typo,
- the migration was applied on staging but rolled back on prod
  (e.g. a deploy aborted partway),
- the schema was edited without regenerating the migration,
- `rebar3 kura compile` missed the diff (e.g. an index that was
  declared after the table-creation migration but before
  index-diffing was supported by the plugin).

Each of these leaves an app that boots cleanly and only fails on
the first query that touches the missing column or constraint —
often hours later, in production. `verify/1,2` is the runtime
guardrail: call it from a healthcheck or readiness probe, fail
loudly if drift is detected.

The check is structural, not behavioural: column type comparisons
across the postgres⇄kura type boundary are too noisy to be useful
(e.g. `string` ⇄ `character varying(255)` ⇄ `text` are equivalent
in practice and would generate false positives). We pin only:

- declared tables exist
- declared columns exist (by name) on those tables
- declared indexes exist as valid indexes, matched by what they are,
  not by what they are called: an index created under a custom name,
  or renamed since, counts, and one carrying the generated name over
  the wrong columns does not

A declared index is satisfied by a live one over the same key columns,
in order. A declared unique index needs a unique one that is partial
exactly when the declaration is. A declared plain index is also
satisfied by a unique one, and a partial declaration by a full index,
since either serves the same lookups. Expression columns never match a
declared column, and `INCLUDE` columns are not key columns. A partial
index's predicate is not compared, because PostgreSQL stores its own
rewrite of the `WHERE` text (`status = 'active'` comes back as
`((status)::text = 'active'::text)`). An index left invalid by a failed
`CREATE INDEX CONCURRENTLY` does not count: PostgreSQL ignores it, for
`ON CONFLICT` inference too.

Out of scope: an `on_conflict` target naming a constraint
(`{constraint, Name}`) resolves through `pg_constraint`, which kura's
`CREATE UNIQUE INDEX` never populates. Only `current_schema()` is
checked, so under schema-prefix tenancy this does not show whether any
tenant's schema is migrated.

Each call runs two catalog queries. Run it at boot, or cache the result
behind a readiness probe, rather than once per probe request.

False negatives are possible (e.g. column type drift), but every
issue this *does* report is a real, blocking problem.
""".

-include("kura.hrl").

-export([
    verify/1,
    verify/2,
    discover_schemas/1,
    declared_index_name/2
]).

-export_type([drift_issue/0]).

-deprecated({declared_index_name, 2, "use kura_migration:index_name/2"}).

-type drift_issue() ::
    {missing_table, binary()}
    | {missing_column, binary(), binary()}
    | {missing_index, binary(), [atom()], map()}.

-type index_shape() :: {KeyColumns :: [binary()], Unique :: boolean(), Partial :: boolean()}.

%%----------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------

-doc """
Verify all schemas reachable from the repo's owning application.

Returns `ok` if every declared schema's table, columns, and indexes
exist in the live database. Returns `{drift, Issues}` listing each
discrepancy. Returns `{error, Reason}` if the database itself can't
be reached.
""".
-spec verify(module()) -> ok | {drift, [drift_issue()]} | {error, term()}.
verify(RepoMod) ->
    Schemas = discover_schemas(RepoMod),
    verify(RepoMod, Schemas).

-doc """
Verify a specific list of schema modules. Useful when callers want
to scope the check (for example, to a single domain) rather than
cover the whole app.
""".
-spec verify(module(), [module()]) ->
    ok | {drift, [drift_issue()]} | {error, term()}.
verify(_RepoMod, []) ->
    ok;
verify(RepoMod, SchemaMods) ->
    case kura_migrator:wait_for_pool(RepoMod, 0) of
        {error, _} = Err ->
            Err;
        ok ->
            run_checks(RepoMod, SchemaMods)
    end.

-doc """
Discover all `kura_schema`-implementing modules reachable from the
application that owns `RepoMod`. Uses the same app-modules-list
discovery contract that the migrator uses internally to find
migration modules. Useful as a starting point for building scoped
verifiers.
""".
-spec discover_schemas(module()) -> [module()].
discover_schemas(RepoMod) ->
    case application:get_application(RepoMod) of
        {ok, App} -> discover_app_schemas(App);
        undefined -> []
    end.

-doc """
The conventional name kura generates for an index declared via
`c:kura_schema:indexes/0`. Deprecated: call `kura_migration:index_name/2`.
""".
-spec declared_index_name(binary(), [atom()]) -> binary().
declared_index_name(Table, Cols) ->
    kura_migration:index_name(Table, Cols).

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

-spec discover_app_schemas(atom()) -> [module()].
discover_app_schemas(App) ->
    Modules =
        case application:get_key(App, modules) of
            {ok, Ms} when is_list(Ms) -> Ms;
            _ -> []
        end,
    [M || M <- Modules, is_atom(M), implements_kura_schema(M)].

-spec implements_kura_schema(module()) -> boolean().
implements_kura_schema(Module) ->
    %% A schema must export both required callbacks. Don't trust the
    %% behaviour attribute — modules that *use* schemas (e.g. helpers)
    %% may declare the behaviour without implementing it.
    _ = code:ensure_loaded(Module),
    erlang:function_exported(Module, table, 0) andalso
        erlang:function_exported(Module, fields, 0).

-spec run_checks(module(), [module()]) ->
    ok | {drift, [drift_issue()]} | {error, term()}.
run_checks(RepoMod, SchemaMods) ->
    case load_existing_state(RepoMod) of
        {error, _} = Err ->
            Err;
        {ok, ColState, IdxState} ->
            case collect_issues(SchemaMods, ColState, IdxState) of
                [] -> ok;
                Issues -> {drift, Issues}
            end
    end.

-spec collect_issues(
    [module()],
    #{binary() => sets:set(binary())},
    #{binary() => [index_shape()]}
) -> [drift_issue()].
collect_issues([], _Cols, _Idx) ->
    [];
collect_issues([Mod | Rest], Cols, Idx) ->
    check_schema(Mod, Cols, Idx) ++ collect_issues(Rest, Cols, Idx).

-spec load_existing_state(module()) ->
    {ok, #{binary() => sets:set(binary())}, #{binary() => [index_shape()]}}
    | {error, term()}.
load_existing_state(RepoMod) ->
    case load_columns(RepoMod) of
        {error, _} = Err ->
            Err;
        {ok, ColState} ->
            case load_indexes(RepoMod) of
                {error, _} = IdxErr -> IdxErr;
                {ok, IdxState} -> {ok, ColState, IdxState}
            end
    end.

-spec load_columns(module()) ->
    {ok, #{binary() => sets:set(binary())}} | {error, term()}.
load_columns(RepoMod) ->
    %% Store column names as binaries (not atoms) to avoid atom-table
    %% exhaustion when the live DB has columns the running app doesn't
    %% know about. Schema-declared column names are converted to binary
    %% at compare time.
    SQL =
        ~"SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = pg_catalog.current_schema()",
    case kura_db:query(RepoMod, SQL, []) of
        #{rows := Rows} when is_list(Rows) ->
            {ok, build_col_state(Rows, #{})};
        {error, Reason} ->
            {error, Reason}
    end.

-spec build_col_state([map()], #{binary() => sets:set(binary())}) ->
    #{binary() => sets:set(binary())}.
build_col_state([], Acc) ->
    Acc;
build_col_state([#{table_name := TableBin, column_name := ColBin} | Rest], Acc) ->
    Table = ensure_binary(TableBin),
    Col = ensure_binary(ColBin),
    Existing = maps:get(Table, Acc, sets:new([{version, 2}])),
    build_col_state(Rest, Acc#{Table => sets:add_element(Col, Existing)});
build_col_state([_ | Rest], Acc) ->
    build_col_state(Rest, Acc).

-spec load_indexes(module()) ->
    {ok, #{binary() => [index_shape()]}} | {error, term()}.
load_indexes(RepoMod) ->
    %% An expression column has no attname and reads as `<<>>`, so it never
    %% matches a declared column. INCLUDE columns sit past indnkeyatts.
    %% Catalog names are qualified so a search_path cannot shadow them.
    SQL =
        ~"""
    SELECT c.relname::text AS tablename,
           i.relname::text AS indexname,
           ix.indisunique AS is_unique,
           ix.indpred IS NOT NULL AS is_partial,
           COALESCE(a.attname::text, '') AS column_name
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL pg_catalog.unnest(ix.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
    WHERE n.nspname = pg_catalog.current_schema()
      AND ix.indisvalid
      AND k.ord <= ix.indnkeyatts
    ORDER BY i.relname, k.ord
    """,
    case kura_db:query(RepoMod, SQL, []) of
        #{rows := Rows} when is_list(Rows) ->
            {ok, add_shapes(maps:to_list(group_index_columns(Rows, #{})), #{})};
        {error, Reason} ->
            {error, Reason}
    end.

-spec group_index_columns([map()], #{{binary(), binary()} => index_shape()}) ->
    #{{binary(), binary()} => index_shape()}.
group_index_columns([], Acc) ->
    Acc;
group_index_columns(
    [
        #{
            tablename := TableBin,
            indexname := IdxBin,
            is_unique := Unique,
            is_partial := Partial,
            column_name := ColBin
        }
        | Rest
    ],
    Acc
) when is_boolean(Unique), is_boolean(Partial) ->
    Key = {ensure_binary(TableBin), ensure_binary(IdxBin)},
    {Cols, _, _} = maps:get(Key, Acc, {[], Unique, Partial}),
    group_index_columns(Rest, Acc#{Key => {Cols ++ [ensure_binary(ColBin)], Unique, Partial}});
group_index_columns([_ | Rest], Acc) ->
    group_index_columns(Rest, Acc).

-spec add_shapes([{{binary(), binary()}, index_shape()}], #{binary() => [index_shape()]}) ->
    #{binary() => [index_shape()]}.
add_shapes([], Acc) ->
    Acc;
add_shapes([{{Table, _Name}, Shape} | Rest], Acc) ->
    add_shapes(Rest, Acc#{Table => [Shape | maps:get(Table, Acc, [])]}).

-spec check_schema(
    module(),
    #{binary() => sets:set(binary())},
    #{binary() => [index_shape()]}
) -> [drift_issue()].
check_schema(Mod, ColState, IdxState) ->
    Table = Mod:table(),
    case ColState of
        #{Table := Cols} ->
            check_columns(Mod, Table, Cols) ++
                check_indexes(Mod, Table, IdxState);
        #{} ->
            [{missing_table, Table}]
    end.

-spec check_columns(module(), binary(), sets:set(binary())) -> [drift_issue()].
check_columns(Mod, Table, ExistingCols) ->
    Fields = [F || F <- Mod:fields(), F#kura_field.virtual =/= true],
    check_columns_loop(Fields, Table, ExistingCols).

-spec check_columns_loop(
    [#kura_field{}],
    binary(),
    sets:set(binary())
) -> [drift_issue()].
check_columns_loop([], _Table, _Existing) ->
    [];
check_columns_loop([F | Rest], Table, Existing) ->
    ColBin = column_binary(F#kura_field.name, F#kura_field.column),
    case sets:is_element(ColBin, Existing) of
        true ->
            check_columns_loop(Rest, Table, Existing);
        false ->
            [{missing_column, Table, ColBin} | check_columns_loop(Rest, Table, Existing)]
    end.

-spec column_binary(atom(), undefined | binary()) -> binary().
column_binary(Name, undefined) -> atom_to_binary(Name, utf8);
column_binary(_Name, Column) when is_binary(Column) -> Column.

-spec check_indexes(
    module(),
    binary(),
    #{binary() => [index_shape()]}
) -> [drift_issue()].
check_indexes(Mod, Table, IdxState) ->
    case erlang:function_exported(Mod, indexes, 0) of
        false ->
            [];
        true ->
            Existing = maps:get(Table, IdxState, []),
            [
                {missing_index, Table, Cols, Opts}
             || {Cols, Opts} <- Mod:indexes(),
                not satisfied(declared_shape(Cols, Opts), Existing)
            ]
    end.

-spec satisfied(index_shape(), [index_shape()]) -> boolean().
satisfied(_Declared, []) -> false;
satisfied(Declared, [Live | Rest]) -> satisfies(Live, Declared) orelse satisfied(Declared, Rest).

-spec satisfies(index_shape(), index_shape()) -> boolean().
satisfies({Cols, true, Partial}, {Cols, true, Partial}) -> true;
satisfies({Cols, _Unique, LivePartial}, {Cols, false, Partial}) -> Partial orelse not LivePartial;
satisfies(_Live, _Declared) -> false.

-spec declared_shape([atom()], kura_migration:index_opts_map()) -> index_shape().
declared_shape(Cols, Opts) ->
    {
        [atom_to_binary(C, utf8) || C <- Cols],
        maps:get(unique, Opts, false) =:= true,
        is_binary(maps:get(where, Opts, undefined))
    }.

-spec ensure_binary(binary() | string()) -> binary().
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(L) when is_list(L) -> list_to_binary(L).
