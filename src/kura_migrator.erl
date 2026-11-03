-module(kura_migrator).
-moduledoc """
Runs, rolls back, and reports status of migrations.

Discovers migration modules automatically from the application that owns the
repo module. Any module named `m<YYYYMMDDHHMMSS>_<name>` in the application's
module list is treated as a migration. Tracks applied versions in a
`schema_migrations` table.

A repo may pull in migrations from further applications - an extension
shipping its own schema, for instance - by exporting the optional
`kura_repo` callback `migration_apps/0`. The applications are ordered by
the OTP `applications` dependency graph, so a dependency's migrations
always run before its dependents'. See `migration_apps/1`.

`schema_migrations` records versions only. A version is therefore global
across every application feeding a repo: two applications shipping the
same version is a hard error (`duplicate_migration_version`) rather than
a silent last-one-wins.

Pending migrations run within a single transaction holding a
transaction-level PostgreSQL advisory lock, so two nodes cannot run them
at once. The applied-version set is read inside that lock, so two nodes
booting together cannot both decide the same migration is pending, and
`schema_migrations` is created under the same lock in its own
transaction, so they cannot race to create it either.

## Non-transactional migrations

A migration exporting `transactional() -> false` (see `m:kura_migration`)
splits the run. The pending set is read once, under the lock, and the
transactional migrations before the non-transactional one run in that
first transaction and commit. The non-transactional migration then runs
on a connection checked out for it alone, which takes the session-level
lock (`pg_advisory_lock`) before it re-reads `schema_migrations` and
releases it after recording the version. Each of its statements commits
on its own. The migrations after it run in further transactions, each
taking the lock again. The lock is released between these steps, so a
node booting meanwhile may apply the next step itself, but every step
re-reads `schema_migrations` under the lock and skips what is already
done: each migration still runs once, and in order.

A statement failing in a non-transactional migration leaves the
statements before it committed and the version unrecorded, so the
migration stays pending and the error names how many statements were
committed. Migrations earlier in the run stay applied, later ones do not
run, and the result is `{error, {migration_failed, Version, Reason}}` as
for a transactional migration. `rollback/1,2,3` runs a non-transactional
migration's `down/0` the same way.

`{create_index, Table, Columns, #{concurrently => true}}` in a migration
that is still transactional is refused with `{error,
{concurrently_in_transaction, Version, Module}}` before anything in the
run executes. On a non-Postgres backend whose pool does not declare the
`concurrent_indexes` capability (SQLite) the option compiles to a plain
`CREATE INDEX`, so one migration serves both backends. A Postgres repo
(dialect `kura_dialect_pg`, or none configured) on a pool without it is
refused with `{error, {concurrent_indexes_unsupported, PoolModule}}`
while the run is planned, rather than given a build that blocks writes.

A run with a non-transactional migration pending is refused with
`{error, {non_transactional_in_transaction, Version, Module}}`, before
anything in it executes, when the caller is inside
`kura_db:transaction/2` or holds a `m:kura_sandbox` connection for the
repo's pool. The enclosing transaction would keep the first step's
lock held while the migration waited for it on another connection.

## The advisory lock

The lock is taken with `pg_try_advisory_xact_lock` or
`pg_try_advisory_lock`, never with the blocking forms, and a transaction
that misses it ends before the next try, `migration_lock_retry_interval`
milliseconds later (`kura` app env, default 100, at least 10). `CREATE INDEX
CONCURRENTLY` on the node holding the lock waits for every transaction
that had a snapshot when it started: a waiter blocked inside a statement,
or retrying inside one long transaction, would never end, and the two
would deadlock. A node still on an older kura waits the blocking way and
ignores `transactional/0`, so deploy this version on every node before
shipping the first non-transactional migration.

Inside `kura_db:transaction/2` or with a sandbox connection checked
out, the lock is tried once and a miss returns `{error,
migration_lock_busy}`: retrying would keep that one transaction open.

A waiting migrator logs a warning every 30 seconds with the time it has
waited. The wait is unbounded unless `migration_lock_timeout` (`kura` app
env, milliseconds, default `infinity`) is set; past it the operation
returns `{error, migration_lock_timeout}`. The lock key is a fixed public
constant, so any role that can connect to the database can take it and
stall every deploy's migrations; set a finite `migration_lock_timeout`
in production.

A backend whose pool does not declare `advisory_locks` (SQLite) takes no
lock and relies on its single writer. There a non-transactional
migration runs each statement in autocommit on one connection, so a
`PRAGMA` it sets holds for the statements after it.

`migrate/1` and `status/1` span every application. The two operations
that record versions without running the DDL they stand for -
`rollback/1,2` and `fake/1` - refuse a multi-application set rather than
guess which application an operator meant; `rollback/3` and `fake/2` name
one.
""".

-include("kura.hrl").
-include_lib("kernel/include/logger.hrl").

%% Arbitrary constant used as the advisory lock key. All Kura migrators
%% sharing a PostgreSQL database contend on this single lock.
-define(MIGRATION_LOCK_KEY, 571629482).
-define(LOCK_RETRY_INTERVAL_MS, 100).
-define(LOCK_RETRY_FLOOR_MS, 10).
-define(LOCK_WARN_INTERVAL_MS, 30000).

%% Defaults for the pool-readiness wait at the start of migrator operations.
%% A pool can exist before its connections are open; we retry `SELECT 1`
%% until the pool answers.
-define(POOL_READY_TIMEOUT_MS, 5000).
-define(POOL_READY_INTERVAL_MS, 50).

-export([
    migrate/1,
    fake/1, fake/2, fake/3,
    rollback/1, rollback/2, rollback/3,
    status/1,
    migration_apps/1,
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
    topo_sort_ops/1,
    discover_migrations/1,
    collect_migrations/1,
    collect_migration_groups/1,
    topo_sort_apps/1,
    pending_migrations/2,
    runs_in_transaction/1,
    concurrent_index/1,
    split_transactional/1,
    still_todo/3,
    lock_wait_step/4,
    release_session_lock/1,
    run_non_transactional/3,
    refuse_unsupported_concurrently/3,
    lock_retry_interval/0,
    without_row_values/1
]).
-endif.

-type migration_group() :: {atom(), [{integer(), module()}]}.
-type migration() :: {integer(), module()}.
-type conn_target() :: {conn, module(), kura_pool:conn()}.
-type target() :: module() | conn_target().
-type lock_wait() :: first | {waiting, Since :: integer(), WarnAt :: integer()}.
-export_type([migration_group/0]).

-doc "Run all pending migrations in order.".
-spec migrate(module()) -> {ok, [integer()]} | {error, term()}.
migrate(RepoMod) ->
    case application:get_env(kura, ensure_database, true) of
        true -> ensure_database(RepoMod);
        false -> ok
    end,
    case prepare(RepoMod) of
        {ok, Groups} ->
            Ordered = flatten_groups(Groups),
            run_plan(RepoMod, up, fun() ->
                pending_migrations(Ordered, applied_versions_strict(RepoMod))
            end);
        {error, _} = Err ->
            Err
    end.

%% Every entry point needs the pool up, `schema_migrations` present and
%% the discovered set grouped by application - grouped rather than flat
%% because the single-application refusals and the per-application forms
%% both key off the application the migrations came from.
-spec prepare(module()) -> {ok, [migration_group()]} | {error, term()}.
prepare(RepoMod) ->
    case wait_for_pool(RepoMod) of
        ok ->
            case ensure_schema_migrations(RepoMod) of
                ok -> discover_migration_groups(RepoMod);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

-doc """
Stamp all pending migrations as applied WITHOUT running their DDL.

Baseline for brownfield adoption: after `rebar3 kura gen_schemas`
bootstraps schema modules from an existing database, the first
`compile` emits `create_table` migrations for tables that already
exist. `fake/1` records those in `schema_migrations` so real
migrations proceed from there. It never executes migration DDL.

Refuses a repo drawing migrations from more than one application with
`{error, {ambiguous_fake, Apps}}`, and does not stamp anything. An
operator baselining a brownfield database has the host's tables in
front of them; an extension added at the same time has none, and
stamping its migrations too means its tables are never created and
`migrate/1` thereafter believes there is nothing to do. Name the one
application being baselined with `fake/2`.

Precondition: `fake/1` stamps EVERY pending migration of the single
application it covers. Only run it when every pending migration
corresponds to schema that already exists - a genuinely-new migration
in the pending set would be stamped without its table ever being
created, and a later `migrate/1` would then treat it as done. Check
`status/1` first; the versions about to be stamped are also logged at
warning level. For the mixed new/existing case baseline up to a version
with `fake/2`.
""".
-spec fake(module()) -> {ok, [integer()]} | {error, term()}.
fake(RepoMod) ->
    case prepare(RepoMod) of
        {ok, [_, _ | _] = Groups} ->
            refuse_multi_app(ambiguous_fake, RepoMod, group_apps(Groups));
        {ok, Groups} ->
            run_fake(RepoMod, flatten_groups(Groups));
        {error, _} = Err ->
            Err
    end.

-doc """
Stamp pending migrations as applied WITHOUT running their DDL, either
those of one application or those up to a version.

`fake(RepoMod, App)` is the per-application form of `fake/1`, for a
repo drawing migrations from more than one application. Only `App`'s
pending migrations are stamped; every other application's stay pending,
so a host can be baselined while a freshly installed extension still
gets its tables created by `migrate/1`. `App` must be one of
`migration_apps/1`, otherwise `{error, {unknown_migration_app, App,
Apps}}` and nothing is stamped.

`fake(RepoMod, UpTo)` stamps only the pending migrations with a version
at or below `UpTo`, for the partial-brownfield case: an introspected
baseline followed by genuinely new migrations. Baseline the prefix, then
`migrate/1` runs the rest for real. `UpTo` is a version, not a count as
in `rollback/2`. Every pending migration at or below it is stamped,
including one with an older timestamp merged in from another branch
after later versions were applied. `UpTo` must be the version of a
discovered migration, otherwise `{error, {unknown_migration_version,
UpTo}}` and nothing is stamped, so a mistyped timestamp cannot widen the
window to every pending migration. Like `fake/1` it refuses a
multi-application repo; use `fake/3`.

`fake/1`'s precondition applies unchanged to the migrations selected:
each must correspond to schema that already exists.
""".
-spec fake
    (module(), atom()) -> {ok, [integer()]} | {error, term()};
    (module(), pos_integer()) -> {ok, [integer()]} | {error, term()}.
fake(RepoMod, App) when is_atom(App) ->
    case prepare(RepoMod) of
        {ok, Groups} ->
            case app_migrations(App, Groups) of
                {ok, Ordered} -> run_fake(RepoMod, Ordered);
                error -> refuse_unknown_app(RepoMod, App, group_apps(Groups))
            end;
        {error, _} = Err ->
            Err
    end;
fake(RepoMod, UpTo) when is_integer(UpTo) ->
    case prepare(RepoMod) of
        {ok, [_, _ | _] = Groups} ->
            refuse_multi_app(ambiguous_fake, RepoMod, group_apps(Groups));
        {ok, Groups} ->
            run_fake_up_to(RepoMod, flatten_groups(Groups), UpTo);
        {error, _} = Err ->
            Err
    end.

-doc """
Stamp `App`'s pending migrations up to version `UpTo` as applied WITHOUT
running their DDL.

Both scopes of `fake/2` at once, for a multi-application repo: `UpTo`
must be the version of one of `App`'s migrations, and every other
application's migrations stay pending whatever their version.
""".
-spec fake(module(), atom(), pos_integer()) -> {ok, [integer()]} | {error, term()}.
fake(RepoMod, App, UpTo) when is_atom(App), is_integer(UpTo) ->
    case prepare(RepoMod) of
        {ok, Groups} ->
            case app_migrations(App, Groups) of
                {ok, Ordered} -> run_fake_up_to(RepoMod, Ordered, UpTo);
                error -> refuse_unknown_app(RepoMod, App, group_apps(Groups))
            end;
        {error, _} = Err ->
            Err
    end.

-spec run_fake_up_to(module(), [{integer(), module()}], integer()) ->
    {ok, [integer()]} | {error, term()}.
run_fake_up_to(RepoMod, Ordered, UpTo) ->
    case lists:keymember(UpTo, 1, Ordered) of
        true ->
            run_fake(RepoMod, [M || {V, _} = M <- Ordered, V =< UpTo]);
        false ->
            logger:error(#{
                msg => ~"kura: fake up to a version no migration has",
                repo => RepoMod,
                version => UpTo
            }),
            {error, {unknown_migration_version, UpTo}}
    end.

-spec run_fake(module(), [{integer(), module()}]) -> {ok, [integer()]} | {error, term()}.
run_fake(RepoMod, Ordered) ->
    T0 = erlang:monotonic_time(),
    Result = with_migration_lock(RepoMod, fun() ->
        Pending = pending_migrations(Ordered, applied_versions_strict(RepoMod)),
        log_fake_stamp(RepoMod, Pending),
        emit_migrate_start(RepoMod, fake, length(Pending)),
        stamp_migrations(Pending, RepoMod, [])
    end),
    emit_migrate_stop(RepoMod, T0, fake, Result),
    Result.

-spec log_fake_stamp(module(), [{integer(), module()}]) -> ok.
log_fake_stamp(_RepoMod, []) ->
    ok;
log_fake_stamp(RepoMod, Pending) ->
    logger:warning(#{
        msg => ~"kura: fake-stamping migrations as applied without running DDL",
        repo => RepoMod,
        versions => [V || {V, _M} <- Pending]
    }).

-doc "Roll back the last migration.".
-spec rollback(module()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod) ->
    rollback(RepoMod, 1).

-doc """
Roll back the last `Steps` migrations.

The window is the `Steps` highest applied versions. Every version in it
must resolve to a discovered migration module: if one does not, the
rollback aborts with `{error, {unknown_applied_versions, Versions}}`
rather than quietly rolling back fewer migrations than asked for. The
usual cause is a migration module deleted from source while its
`schema_migrations` row remains.

Within the window, migrations run in reverse apply order, so a
dependency's `down/0` never runs before its dependents'.

Refuses a repo drawing migrations from more than one application with
`{error, {ambiguous_rollback, Apps}}`, and rolls nothing back.
`schema_migrations` records versions and nothing else, so the only
window this function can compute is one ordered by version - and
"the last three" across unrelated applications can take one migration
each from three of them and leave every one half-migrated, in an order
their dependency graph never sanctioned. Name the application with
`rollback/3`.
""".
-spec rollback(module(), non_neg_integer()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod, Steps) ->
    case prepare(RepoMod) of
        {ok, [_, _ | _] = Groups} ->
            refuse_multi_app(ambiguous_rollback, RepoMod, group_apps(Groups));
        {ok, Groups} ->
            Ordered = flatten_groups(Groups),
            run_plan(RepoMod, down, fun() ->
                rollback_window(RepoMod, Ordered, Steps, applied_versions_strict(RepoMod))
            end);
        {error, _} = Err ->
            Err
    end.

-doc """
Roll back the last `Steps` migrations of `App`.

The per-application form of `rollback/2`, for a repo drawing migrations
from more than one application. The window is the `Steps` highest
applied versions **among those `App`'s discovered migrations claim**, so
no other application's migrations can enter it whatever their versions
are. Within the window, execution reverses `App`'s apply order.

`App` must be one of `migration_apps/1`, otherwise
`{error, {unknown_migration_app, App, Apps}}` and nothing is rolled back.

Rolling an application back below a version a *dependent* application
builds on is not detected: `schema_migrations` has no record of which
application applied a version, so kura cannot tell an orphan row of
`App`'s from any other application's. Roll dependents back first.
""".
-spec rollback(module(), atom(), non_neg_integer()) -> {ok, [integer()]} | {error, term()}.
rollback(RepoMod, App, Steps) ->
    case prepare(RepoMod) of
        {ok, Groups} ->
            case app_migrations(App, Groups) of
                {ok, Ordered} ->
                    run_plan(RepoMod, down, fun() ->
                        rollback_window(RepoMod, Ordered, Steps, app_applied(RepoMod, Ordered))
                    end);
                error ->
                    refuse_unknown_app(RepoMod, App, group_apps(Groups))
            end;
        {error, _} = Err ->
            Err
    end.

%% Restricting the applied set to versions this application's own
%% migrations claim is what confines the window to one application. It
%% also makes the `unknown_applied_versions` guard below unreachable on
%% this path, which is correct rather than lax: an applied version no
%% module claims cannot be attributed to any application, so it is not
%% `App`'s to refuse over.
-spec app_applied(module(), [{integer(), module()}]) -> [integer()].
app_applied(RepoMod, Ordered) ->
    Known = #{V => [] || {V, _M} <- Ordered},
    [V || V <- applied_versions_strict(RepoMod), is_map_key(V, Known)].

-spec rollback_window(module(), [migration()], non_neg_integer(), [integer()]) ->
    [migration()].
rollback_window(RepoMod, Ordered, Steps, Applied) ->
    Selected = take_integers(reverse_integers(sort_integers(Applied), []), Steps),
    case build_rollback_pairs(Selected, Ordered) of
        {ok, Pairs} ->
            Pairs;
        {error, {unknown_applied_versions, Missing} = Reason} ->
            logger:error(#{
                msg => ~"kura: refusing to roll back past migrations with no module",
                repo => RepoMod,
                versions => Missing,
                hint =>
                    ~"missing from migration_apps/0, or the modules were deleted"
            }),
            error({migration_aborted, Reason})
    end.

%% `migrate/1` runs every discovered migration and `status/1` reports on
%% every one, so neither has anything to disambiguate. `rollback/1,2` and
%% `fake/1` each pick a subset by version alone, which stops meaning
%% anything the moment versions come from unrelated applications.
-spec refuse_multi_app(ambiguous_fake | ambiguous_rollback, module(), [atom()]) ->
    {error, {ambiguous_fake | ambiguous_rollback, [atom()]}}.
refuse_multi_app(Reason, RepoMod, Apps) ->
    logger:error(#{
        msg => ~"kura: refusing a version-chosen operation across applications",
        repo => RepoMod,
        reason => Reason,
        apps => Apps,
        hint => ~"name one application: rollback/3, fake(Repo, App) or fake/3"
    }),
    {error, {Reason, Apps}}.

-spec refuse_unknown_app(module(), atom(), [atom()]) ->
    {error, {unknown_migration_app, atom(), [atom()]}}.
refuse_unknown_app(RepoMod, App, Apps) ->
    logger:error(#{
        msg => ~"kura: application does not feed this repo's migrations",
        repo => RepoMod,
        app => App,
        apps => Apps
    }),
    {error, {unknown_migration_app, App, Apps}}.

%% Emitted inside the advisory lock: the pending count is only true once
%% no other node can still be applying migrations.
-spec emit_migrate_start(module(), up | down | fake, non_neg_integer()) -> ok.
emit_migrate_start(RepoMod, Direction, PendingCount) ->
    telemetry_event(
        [kura, migrator, migrate, start],
        #{system_time => erlang:system_time()},
        #{repo => RepoMod, pending_count => PendingCount, direction => Direction}
    ).

-spec emit_migrate_stop(module(), integer(), up | down | fake, term()) -> ok.
emit_migrate_stop(RepoMod, T0, Direction, Result) ->
    emit_migrate_stop(RepoMod, T0, Direction, Result, []).

%% `Committed` is what a split run committed before it failed: those
%% versions stay applied, so an error does not mean nothing changed.
-spec emit_migrate_stop(module(), integer(), up | down | fake, term(), [integer()]) -> ok.
emit_migrate_stop(RepoMod, T0, Direction, Result, Committed) ->
    DurationNative = erlang:monotonic_time() - T0,
    {ResultStatus, Applied, ErrorReason} =
        case Result of
            {ok, Versions} when is_list(Versions) -> {ok, Versions, undefined};
            {error, R} -> {error, Committed, R}
        end,
    telemetry_event(
        [kura, migrator, migrate, stop],
        #{duration => DurationNative},
        #{
            repo => RepoMod,
            direction => Direction,
            result => ResultStatus,
            applied_count => length(Applied),
            applied_versions => Applied,
            error_reason => ErrorReason
        }
    ).

-spec telemetry_event([atom()], map(), map()) -> ok.
telemetry_event(EventName, Measurements, Metadata) ->
    %% telemetry is a hard dependency of kura, but we still
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

-doc """
Return the status of all discovered migrations (`up` or `pending`).

Listed in apply order: applications in dependency order, versions
ascending within each. Spans every application, so it is the thing to
check before `fake/2` or `rollback/3`.

Returns `[]` when the pool is unavailable, and `{error, Reason}` when
`schema_migrations` cannot be created or discovery itself fails (a
duplicate version across applications, for instance).
""".
-spec status(module()) -> [{integer(), module(), up | pending}] | {error, term()}.
status(RepoMod) ->
    case prepare(RepoMod) of
        {ok, Groups} ->
            Applied = get_applied_versions(RepoMod),
            [tag_status(V, M, Applied) || {V, M} <- flatten_groups(Groups)];
        {error, pool_unavailable} ->
            [];
        {error, _} = Err ->
            Err
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

-doc """
Create `schema_migrations` if it is not already there.

Runs under the migration advisory lock in a transaction of its own.
`CREATE TABLE IF NOT EXISTS` is not atomic against a concurrent
creator: both sessions can pass the existence check and the loser
fails on `pg_type`'s unique index. Taking the lock first is what makes
two nodes booting together safe, and the transaction is separate from
the migration one so the lock is released before `migrate/1` re-takes
it - no lock is held across the two.

Returns `{error, {schema_migrations_failed, Reason}}` when the
statement does not succeed, rather than leaving every later query to
fail on a table that was never created, and `{error,
migration_lock_timeout}` when the lock wait passes
`migration_lock_timeout`.
""".
-spec ensure_schema_migrations(module()) -> ok | {error, term()}.
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
    try
        in_migration_lock(RepoMod, fun() -> create_schema_migrations(RepoMod, SQL) end)
    catch
        error:{migration_aborted, AbortReason} ->
            {error, AbortReason};
        Class:Reason:Stack ->
            logger:error(#{
                msg => ~"kura: schema_migrations bootstrap failed",
                repo => RepoMod,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, {schema_migrations_failed, Reason}}
    end.

%% Drivers differ on the success shape, so anything that is not an
%% explicit error counts as created. The point is that an error is no
%% longer thrown away.
-spec create_schema_migrations(module(), binary()) -> ok | {error, term()}.
create_schema_migrations(RepoMod, SQL) ->
    case kura_db:query(RepoMod, SQL, []) of
        {error, Reason} -> {error, {schema_migrations_failed, Reason}};
        _ -> ok
    end.

%%----------------------------------------------------------------------
%% Pool readiness
%%----------------------------------------------------------------------

-doc """
Wait until the repo's connection pool has at least one ready worker.

A pool can exist before its connections are open, so when an
application starts and immediately calls `migrate/1` the pool may
reject queries until the first handshake completes. The migrator races
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

-doc """
The applications whose migrations run for `RepoMod`, in dependency order.

The application owning the repo module is always included. A repo pulls
in further applications by exporting the optional `kura_repo` callback
`migration_apps/0`:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0, migration_apps/0]).

otp_app() -> my_app.

migration_apps() -> [my_gdpr_extension].
```

The result is a topological sort of the applications' OTP `applications`
lists, so a dependency's migrations always run before its dependents'.
There is no separate ordering configuration - the dependency graph OTP
already declares is the one that is used. Applications are compared by
their transitively reachable dependencies, so an intermediate
application that ships no migrations still orders the two that do.

Every declared application must be loaded; one that is not yields
`{error, {migration_apps_not_loaded, Apps}}` rather than contributing
zero migrations silently.
""".
-spec migration_apps(module()) -> {ok, [atom()]} | {error, term()}.
migration_apps(RepoMod) ->
    case application:get_application(RepoMod) of
        {ok, App} -> resolve_migration_apps(RepoMod, App);
        undefined -> {ok, []}
    end.

-spec resolve_migration_apps(module(), atom()) -> {ok, [atom()]} | {error, term()}.
resolve_migration_apps(RepoMod, OwnerApp) ->
    case declared_migration_apps(RepoMod) of
        {ok, Declared} ->
            Apps = lists:usort([OwnerApp | Declared]),
            case [A || A <- Apps, not app_loaded(A)] of
                [] -> topo_sort_apps(Apps);
                Missing -> {error, {migration_apps_not_loaded, Missing}}
            end;
        {error, _} = Err ->
            Err
    end.

-spec declared_migration_apps(module()) -> {ok, [atom()]} | {error, term()}.
declared_migration_apps(RepoMod) ->
    _ = code:ensure_loaded(RepoMod),
    case erlang:function_exported(RepoMod, migration_apps, 0) of
        false ->
            {ok, []};
        true ->
            case RepoMod:migration_apps() of
                Apps when is_list(Apps) -> narrow_app_list(RepoMod, Apps);
                Other -> {error, {invalid_migration_apps, RepoMod, Other}}
            end
    end.

-spec narrow_app_list(module(), [term()]) -> {ok, [atom()]} | {error, term()}.
narrow_app_list(RepoMod, Apps) ->
    case [A || A <- Apps, not is_atom(A)] of
        [] -> {ok, [A || A <- Apps, is_atom(A)]};
        Bad -> {error, {invalid_migration_apps, RepoMod, Bad}}
    end.

-spec app_loaded(atom()) -> boolean().
app_loaded(App) ->
    application:get_key(App, modules) =/= undefined.

-spec topo_sort_apps([atom()]) -> {ok, [atom()]} | {error, term()}.
topo_sort_apps([Single]) ->
    {ok, [Single]};
topo_sort_apps(Apps) ->
    DepMap = #{A => restricted_deps(A, Apps) || A <- Apps},
    topo_apps(Apps, DepMap, []).

-spec restricted_deps(atom(), [atom()]) -> [atom()].
restricted_deps(App, Apps) ->
    Reachable = reachable_apps([App], #{}),
    [A || A <- Apps, A =/= App, maps:is_key(A, Reachable)].

-spec reachable_apps([atom()], #{atom() => true}) -> #{atom() => true}.
reachable_apps([], Seen) ->
    Seen;
reachable_apps([App | Rest], Seen) ->
    case Seen of
        #{App := true} -> reachable_apps(Rest, Seen);
        #{} -> reachable_apps(app_dependencies(App) ++ Rest, Seen#{App => true})
    end.

-spec app_dependencies(atom()) -> [atom()].
app_dependencies(App) ->
    case application:get_key(App, applications) of
        {ok, Deps} when is_list(Deps) -> [D || D <- Deps, is_atom(D)];
        _ -> []
    end.

%% `Pending` arrives sorted and `lists:delete/2` preserves order, so ties
%% break alphabetically and the resulting order is stable across boots.
-spec topo_apps([atom()], #{atom() => [atom()]}, [atom()]) -> {ok, [atom()]} | {error, term()}.
topo_apps([], _DepMap, Acc) ->
    {ok, reverse_atoms(Acc, [])};
topo_apps(Pending, DepMap, Acc) ->
    case [A || A <- Pending, deps_placed(maps:get(A, DepMap, []), Acc)] of
        [] -> {error, {migration_app_cycle, Pending}};
        [Next | _] -> topo_apps(lists:delete(Next, Pending), DepMap, [Next | Acc])
    end.

-spec deps_placed([atom()], [atom()]) -> boolean().
deps_placed([], _Placed) ->
    true;
deps_placed([Dep | Rest], Placed) ->
    lists:member(Dep, Placed) andalso deps_placed(Rest, Placed).

-spec discover_migration_groups(module()) -> {ok, [migration_group()]} | {error, term()}.
discover_migration_groups(RepoMod) ->
    case migration_apps(RepoMod) of
        {ok, Apps} -> collect_migration_groups(Apps);
        {error, _} = Err -> Err
    end.

-ifdef(TEST).
%% Flattened views of the grouped forms. Production code needs the
%% grouping to decide the multi-application refusals, but the discovery
%% assertions read better against one ordered list.
-spec discover_migrations(module()) -> {ok, [{integer(), module()}]} | {error, term()}.
discover_migrations(RepoMod) ->
    case discover_migration_groups(RepoMod) of
        {ok, Groups} -> {ok, flatten_groups(Groups)};
        {error, _} = Err -> Err
    end.

-spec collect_migrations([atom()]) -> {ok, [{integer(), module()}]} | {error, term()}.
collect_migrations(Apps) ->
    case collect_migration_groups(Apps) of
        {ok, Groups} -> {ok, flatten_groups(Groups)};
        {error, _} = Err -> Err
    end.
-endif.

%% Every application in `Apps` gets a group, including one contributing
%% no migrations: the multi-application refusals are about the set the
%% repo declares, not about which members happen to ship migrations
%% right now, so adding a migration to a so-far-empty extension must not
%% change which operations are allowed.
-spec collect_migration_groups([atom()]) -> {ok, [migration_group()]} | {error, term()}.
collect_migration_groups(Apps) ->
    Groups = [{A, sort_migrations(discover_app_migrations(A))} || A <- Apps],
    case duplicate_versions(Groups) of
        [] ->
            {ok, Groups};
        Duplicates ->
            logger:error(#{
                msg => ~"kura: migration version claimed by more than one module",
                duplicates => Duplicates
            }),
            {error, {duplicate_migration_version, Duplicates}}
    end.

-spec flatten_groups([migration_group()]) -> [{integer(), module()}].
flatten_groups([]) -> [];
flatten_groups([{_App, Ms} | Rest]) -> Ms ++ flatten_groups(Rest).

-spec group_apps([migration_group()]) -> [atom()].
group_apps(Groups) ->
    [App || {App, _Ms} <- Groups].

-spec app_migrations(atom(), [migration_group()]) -> {ok, [{integer(), module()}]} | error.
app_migrations(App, Groups) ->
    case [Ms || {A, Ms} <- Groups, A =:= App] of
        [Ms] -> {ok, Ms};
        [] -> error
    end.

%% A version is the whole primary key of `schema_migrations`, so it has
%% to be unique across every application feeding the repo. Reporting
%% every claimant is the point: the operator has to know which two to
%% renumber.
-spec duplicate_versions([{atom(), [{integer(), module()}]}]) ->
    [{integer(), [{atom(), module()}]}].
duplicate_versions(Groups) ->
    Owners = version_owners(Groups, #{}),
    lists:keysort(1, [
        {V, reverse_owners(Os, [])}
     || {V, Os = [_, _ | _]} <- maps:to_list(Owners)
    ]).

-spec version_owners(
    [{atom(), [{integer(), module()}]}],
    #{integer() => [{atom(), module()}]}
) -> #{integer() => [{atom(), module()}]}.
version_owners([], Acc) ->
    Acc;
version_owners([{App, Migrations} | Rest], Acc) ->
    version_owners(Rest, add_owners(App, Migrations, Acc)).

-spec add_owners(atom(), [{integer(), module()}], #{integer() => [{atom(), module()}]}) ->
    #{integer() => [{atom(), module()}]}.
add_owners(_App, [], Acc) ->
    Acc;
add_owners(App, [{V, M} | Rest], Acc) ->
    add_owners(App, Rest, Acc#{V => [{App, M} | maps:get(V, Acc, [])]}).

-spec reverse_owners([{atom(), module()}], [{atom(), module()}]) -> [{atom(), module()}].
reverse_owners([], Acc) -> Acc;
reverse_owners([H | T], Acc) -> reverse_owners(T, [H | Acc]).

-spec pending_migrations([{integer(), module()}], [integer()]) -> [{integer(), module()}].
pending_migrations(Ordered, Applied) ->
    [{V, M} || {V, M} <- Ordered, not lists:member(V, Applied)].

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

%% Polled, never blocking: see "The advisory lock" in the moduledoc. A
%% transaction that misses the lock ends before the next try, so it never
%% sits open for a `CREATE INDEX CONCURRENTLY` elsewhere to wait on.
-spec in_migration_lock(module(), fun(() -> Result)) -> Result.
in_migration_lock(RepoMod, Fun) ->
    in_migration_lock(RepoMod, Fun, first).

-spec in_migration_lock(module(), fun(() -> Result), lock_wait()) -> Result.
in_migration_lock(RepoMod, Fun, Wait) ->
    PoolMod = kura_db:get_pool_module(RepoMod),
    Outcome = kura_db:transaction(RepoMod, fun() ->
        case xact_lock_taken(RepoMod, PoolMod) of
            true -> {locked, Fun()};
            false -> busy
        end
    end),
    case Outcome of
        {locked, Result} ->
            Result;
        busy ->
            in_migration_lock_again(RepoMod, Fun, Wait, enclosing_transaction(RepoMod))
    end.

%% Inside a transaction the caller opened, or the sandbox's, every try
%% would run in that one open transaction, which is the wait that stalls
%% a `CREATE INDEX CONCURRENTLY` on the node holding the lock. Try once.
-spec in_migration_lock_again(module(), fun(() -> Result), lock_wait(), boolean()) -> Result.
in_migration_lock_again(RepoMod, _Fun, _Wait, true) ->
    ?LOG_ERROR(#{
        msg => ~"kura: the migration lock is held elsewhere and this run is inside a transaction",
        repo => RepoMod,
        hint => ~"run the migrator outside kura_db:transaction/2 and the sandbox to wait for it"
    }),
    error({migration_aborted, migration_lock_busy});
in_migration_lock_again(RepoMod, Fun, Wait, false) ->
    in_migration_lock(RepoMod, Fun, wait_for_lock(RepoMod, Wait)).

%% SQLite and other single-writer backends declare no `advisory_locks`
%% and rely on their own write serialisation.
-spec xact_lock_taken(module(), module()) -> boolean().
xact_lock_taken(RepoMod, PoolMod) ->
    case kura_capabilities:has(PoolMod, advisory_locks) of
        true -> try_lock(RepoMod, ~"SELECT pg_try_advisory_xact_lock($1) AS locked");
        false -> true
    end.

-spec acquire_session_lock(conn_target(), lock_wait()) -> ok.
acquire_session_lock(Target, Wait) ->
    case try_lock(Target, ~"SELECT pg_try_advisory_lock($1) AS locked") of
        true -> ok;
        false -> acquire_session_lock(Target, wait_for_lock(target_repo(Target), Wait))
    end.

-spec try_lock(target(), binary()) -> boolean().
try_lock(Target, SQL) ->
    case run_sql(Target, SQL, [?MIGRATION_LOCK_KEY]) of
        #{rows := [#{locked := Locked}]} when is_boolean(Locked) -> Locked;
        Other -> error({migration_lock_failed, Other})
    end.

%% Raises `{migration_aborted, migration_lock_timeout}` once the wait
%% passes `migration_lock_timeout`, outside any transaction of its own.
-spec wait_for_lock(module(), lock_wait()) -> lock_wait().
wait_for_lock(RepoMod, first) ->
    ?LOG_INFO(#{
        msg => ~"kura: waiting for the migration lock held by another migrator", repo => RepoMod
    }),
    Now = erlang:monotonic_time(millisecond),
    pause_for_lock({waiting, Now, Now + ?LOCK_WARN_INTERVAL_MS});
wait_for_lock(RepoMod, {waiting, Since, WarnAt}) ->
    Now = erlang:monotonic_time(millisecond),
    case lock_wait_step(Now, Since, WarnAt, migration_lock_timeout()) of
        timeout ->
            ?LOG_ERROR(#{
                msg => ~"kura: gave up waiting for the migration lock",
                repo => RepoMod,
                waited_ms => Now - Since
            }),
            error({migration_aborted, migration_lock_timeout});
        {warn, NextWarnAt} ->
            ?LOG_WARNING(#{
                msg => ~"kura: still waiting for the migration lock held by another migrator",
                repo => RepoMod,
                waited_ms => Now - Since
            }),
            pause_for_lock({waiting, Since, NextWarnAt});
        wait ->
            pause_for_lock({waiting, Since, WarnAt})
    end.

-spec lock_wait_step(integer(), integer(), integer(), timeout()) ->
    timeout | {warn, integer()} | wait.
lock_wait_step(Now, Since, _WarnAt, Timeout) when is_integer(Timeout), Now - Since >= Timeout ->
    timeout;
lock_wait_step(Now, _Since, WarnAt, _Timeout) when Now >= WarnAt ->
    {warn, Now + ?LOCK_WARN_INTERVAL_MS};
lock_wait_step(_Now, _Since, _WarnAt, _Timeout) ->
    wait.

-spec pause_for_lock(lock_wait()) -> lock_wait().
pause_for_lock(Wait) ->
    timer:sleep(lock_retry_interval()),
    Wait.

-spec lock_retry_interval() -> pos_integer().
lock_retry_interval() ->
    max(
        ?LOCK_RETRY_FLOOR_MS,
        read_pos_int_env(migration_lock_retry_interval, ?LOCK_RETRY_INTERVAL_MS)
    ).

-spec migration_lock_timeout() -> timeout().
migration_lock_timeout() ->
    case application:get_env(kura, migration_lock_timeout, infinity) of
        N when is_integer(N), N >= 0 -> N;
        _ -> infinity
    end.

-spec with_migration_lock(module(), fun(() -> [integer()])) ->
    {ok, [integer()]} | {error, term()}.
with_migration_lock(RepoMod, Fun) ->
    try
        narrow_migration_result(in_migration_lock(RepoMod, Fun))
    catch
        Class:Reason:Stack -> migration_error(Class, Reason, Stack)
    end.

-spec locked_step(module(), fun(() -> Result)) -> {ok, Result} | {error, term()}.
locked_step(RepoMod, Fun) ->
    try
        {ok, in_migration_lock(RepoMod, Fun)}
    catch
        Class:Reason:Stack -> migration_error(Class, Reason, Stack)
    end.

%% `migration_aborted` and `migration_failed` are raised, not returned,
%% so the surrounding transaction aborts. An aborted run's reason is
%% already logged at the point of abort.
-spec migration_error(error | exit | throw, term(), erlang:stacktrace()) -> {error, term()}.
migration_error(error, {migration_aborted, AbortReason}, _Stack) ->
    {error, AbortReason};
migration_error(error, {migration_failed, Version, MigReason}, Stack) ->
    ?LOG_ERROR(#{
        msg => ~"migration_failed",
        version => Version,
        reason => MigReason,
        stacktrace => Stack
    }),
    {error, {migration_failed, Version, MigReason}};
migration_error(Class, Reason, Stack) ->
    ?LOG_ERROR(#{
        msg => ~"migration_failed",
        class => Class,
        reason => Reason,
        stacktrace => Stack
    }),
    {error, Reason}.

%%----------------------------------------------------------------------
%% Planned runs
%%----------------------------------------------------------------------

%% `PlanFun` picks the migrations to run and is called once, inside the
%% first lock. Every later step re-reads `schema_migrations` under the
%% lock and skips what another node did meanwhile.
-spec run_plan(module(), up | down, fun(() -> [migration()])) ->
    {ok, [integer()]} | {error, term()}.
run_plan(RepoMod, Dir, PlanFun) ->
    T0 = erlang:monotonic_time(),
    Enclosed = enclosing_transaction(RepoMod),
    First = locked_step(RepoMod, fun() ->
        Todo = PlanFun(),
        refuse_non_transactional_when_enclosed(Enclosed, Todo),
        refuse_concurrently_in_transaction(Todo, Dir),
        refuse_unsupported_concurrently(RepoMod, Todo, Dir),
        emit_migrate_start(RepoMod, Dir, length(Todo)),
        run_transactional_prefix(Todo, Dir, RepoMod)
    end),
    {Result, Committed} = continue_plan(First, Dir, RepoMod, []),
    emit_migrate_stop(RepoMod, T0, Dir, Result, Committed),
    Result.

-spec continue_plan(
    {ok, {[integer()], [migration()]}} | {error, term()}, up | down, module(), [integer()]
) -> {{ok, [integer()]} | {error, term()}, [integer()]}.
continue_plan({ok, {Versions, []}}, _Dir, _RepoMod, Done) ->
    {{ok, Done ++ Versions}, Done ++ Versions};
continue_plan({ok, {Versions, Todo}}, Dir, RepoMod, Done) ->
    continue_plan(next_step(Todo, Dir, RepoMod), Dir, RepoMod, Done ++ Versions);
continue_plan({error, _} = Err, _Dir, RepoMod, Done) ->
    log_kept(RepoMod, Done),
    {Err, Done}.

-spec next_step([migration()], up | down, module()) ->
    {ok, {[integer()], [migration()]}} | {error, term()}.
next_step([{_, Module} = Next | Rest] = Todo, Dir, RepoMod) ->
    case runs_in_transaction(Module) of
        false ->
            case run_non_transactional(Next, Dir, RepoMod) of
                {ok, Versions} -> {ok, {Versions, Rest}};
                {error, _} = Err -> Err
            end;
        true ->
            locked_step(RepoMod, fun() ->
                run_transactional_prefix(
                    still_todo(Dir, Todo, applied_versions_strict(RepoMod)), Dir, RepoMod
                )
            end)
    end.

-spec log_kept(module(), [integer()]) -> ok.
log_kept(_RepoMod, []) ->
    ok;
log_kept(RepoMod, Done) ->
    ?LOG_WARNING(#{
        msg => ~"kura: migrations committed before the failure stay committed",
        repo => RepoMod,
        versions => Done
    }).

-spec run_transactional_prefix([migration()], up | down, module()) ->
    {[integer()], [migration()]}.
run_transactional_prefix(Todo, Dir, RepoMod) ->
    {Prefix, Rest} = split_transactional(Todo),
    {run_migrations(Prefix, Dir, RepoMod, []), Rest}.

-spec split_transactional([migration()]) -> {[migration()], [migration()]}.
split_transactional(Todo) ->
    split_transactional(Todo, []).

-spec split_transactional([migration()], [migration()]) -> {[migration()], [migration()]}.
split_transactional([{_, Module} = Migration | Rest] = Todo, Acc) ->
    case runs_in_transaction(Module) of
        true -> split_transactional(Rest, [Migration | Acc]);
        false -> {reverse_migrations(Acc, []), Todo}
    end;
split_transactional([], Acc) ->
    {reverse_migrations(Acc, []), []}.

-spec still_todo(up | down, [migration()], [integer()]) -> [migration()].
still_todo(up, Migrations, Applied) ->
    pending_migrations(Migrations, Applied);
still_todo(down, Migrations, Applied) ->
    [M || {V, _} = M <- Migrations, lists:member(V, Applied)].

-spec runs_in_transaction(module()) -> boolean().
runs_in_transaction(Module) ->
    _ = code:ensure_loaded(Module),
    case erlang:function_exported(Module, transactional, 0) of
        true -> Module:transactional() =/= false;
        false -> true
    end.

%% A transaction the caller is in, or a sandbox connection, would keep
%% the first step's transaction-level lock held while the
%% non-transactional migration polls for it on a connection of its own.
-spec enclosing_transaction(module()) -> boolean().
enclosing_transaction(RepoMod) ->
    kura_db:in_transaction(RepoMod) orelse
        kura_sandbox:get_conn(kura_db:get_pool(RepoMod)) =/= not_found.

-spec refuse_non_transactional_when_enclosed(boolean(), [migration()]) -> ok.
refuse_non_transactional_when_enclosed(false, _Todo) ->
    ok;
refuse_non_transactional_when_enclosed(true, Todo) ->
    case [M || {_, Module} = M <- Todo, not runs_in_transaction(Module)] of
        [] ->
            ok;
        [{Version, Module} | _] ->
            ?LOG_ERROR(#{
                msg => ~"kura: a non-transactional migration cannot run inside a transaction",
                version => Version,
                module => Module,
                hint => ~"run the migrator outside kura_db:transaction/2 and the sandbox"
            }),
            error({migration_aborted, {non_transactional_in_transaction, Version, Module}})
    end.

-spec refuse_concurrently_in_transaction([migration()], up | down) -> ok.
refuse_concurrently_in_transaction([], _Dir) ->
    ok;
refuse_concurrently_in_transaction([{Version, Module} | Rest], Dir) ->
    case runs_in_transaction(Module) andalso has_concurrent_index(get_migration_ops(Module, Dir)) of
        true ->
            ?LOG_ERROR(#{
                msg => ~"kura: concurrently => true needs a non-transactional migration",
                version => Version,
                module => Module,
                hint => ~"export transactional() -> false from the migration"
            }),
            error({migration_aborted, {concurrently_in_transaction, Version, Module}});
        false ->
            refuse_concurrently_in_transaction(Rest, Dir)
    end.

%% Checked while planning so a migration is not refused part-way, after
%% statements before its index build have committed.
-spec refuse_unsupported_concurrently(module(), [migration()], up | down) -> ok.
refuse_unsupported_concurrently(RepoMod, Todo, Dir) ->
    case [M || {_, M} <- Todo, has_concurrent_index(get_migration_ops(M, Dir))] of
        [] ->
            ok;
        [_ | _] ->
            _ = concurrently(RepoMod, true),
            ok
    end.

-spec has_concurrent_index([kura_migration:operation()]) -> boolean().
has_concurrent_index(Ops) ->
    [Op || Op <- Ops, concurrent_index(Op)] =/= [].

-spec concurrent_index(kura_migration:operation()) -> boolean().
concurrent_index({create_index, _Table, _Columns, #{concurrently := true}}) ->
    true;
concurrent_index({create_index, _Name, _Table, _Columns, Opts}) when is_list(Opts) ->
    lists:member(concurrently, Opts);
concurrent_index(_Op) ->
    false.

%%----------------------------------------------------------------------
%% Non-transactional migrations
%%----------------------------------------------------------------------

-spec run_non_transactional(migration(), up | down, module()) ->
    {ok, [integer()]} | {error, term()}.
run_non_transactional(Migration, Dir, RepoMod) ->
    Pool = kura_db:get_pool(RepoMod),
    PoolMod = kura_db:get_pool_module(RepoMod),
    case PoolMod:checkout(Pool, #{}) of
        {ok, Conn, Token} ->
            try
                session_locked(Migration, Dir, PoolMod, {conn, RepoMod, Conn})
            after
                PoolMod:checkin(Pool, Token)
            end;
        {error, Reason} ->
            ?LOG_ERROR(#{
                msg => ~"kura: no connection for a non-transactional migration",
                repo => RepoMod,
                reason => Reason
            }),
            {error, {no_connection, Reason}}
    end.

-spec session_locked(migration(), up | down, module(), conn_target()) ->
    {ok, [integer()]} | {error, term()}.
session_locked(Migration, Dir, PoolMod, Target) ->
    try
        case kura_capabilities:has(PoolMod, advisory_locks) of
            true ->
                acquire_session_lock(Target, first),
                try
                    apply_non_transactional(Migration, Dir, Target)
                after
                    _ = release_session_lock(Target)
                end;
            false ->
                apply_non_transactional(Migration, Dir, Target)
        end
    catch
        Class:Reason:Stack -> migration_error(Class, Reason, Stack)
    end.

%% Never raises, so a failed unlock cannot turn a recorded migration into
%% an error. A connection that has gone takes its session lock with it,
%% and the pool unlocks every advisory lock when the connection is
%% checked in. Anything but `true` from the unlock means this session did
%% not hold the lock it was releasing, which is worth a warning too.
-spec release_session_lock(conn_target()) -> released | not_released.
release_session_lock(Target) ->
    try run_sql(Target, ~"SELECT pg_advisory_unlock($1) AS unlocked", [?MIGRATION_LOCK_KEY]) of
        #{rows := [#{unlocked := true}]} -> released;
        Other -> not_released(Target, #{answer => Other})
    catch
        Class:Reason -> not_released(Target, #{class => Class, reason => Reason})
    end.

-spec not_released(conn_target(), map()) -> not_released.
not_released(Target, Why) ->
    ?LOG_WARNING(Why#{
        msg => ~"kura: could not release the migration lock; checkin releases it",
        repo => target_repo(Target)
    }),
    not_released.

-spec apply_non_transactional(migration(), up | down, conn_target()) ->
    {ok, [integer()]} | {error, term()}.
apply_non_transactional({Version, Module} = Migration, Dir, Target) ->
    case still_todo(Dir, [Migration], applied_versions_strict(Target)) of
        [] ->
            {ok, []};
        [_] ->
            T0 = erlang:monotonic_time(),
            Ops = migration_ops(Module, Dir),
            case exec_each(topo_sort_ops(Ops), Target, 0) of
                ok ->
                    exec(record_sql(Dir), [Version], Version, Target),
                    migration_applied(Version, Module, Dir, length(Ops), T0),
                    {ok, [Version]};
                {error, Committed, Reason} ->
                    ?LOG_ERROR(#{
                        msg => ~"kura: non-transactional migration failed part-way",
                        version => Version,
                        module => Module,
                        committed_statements => Committed,
                        reason => Reason,
                        hint =>
                            ~"statements before the failing one stay committed; the version is not recorded"
                    }),
                    {error, {migration_failed, Version, Reason}}
            end
    end.

-spec exec_each([kura_migration:operation()], conn_target(), non_neg_integer()) ->
    ok | {error, non_neg_integer(), term()}.
exec_each([], _Target, _Committed) ->
    ok;
exec_each([Op | Rest], Target, Committed) ->
    case run_sql(Target, compile_operation(target_repo(Target), Op), []) of
        #{command := _} -> exec_each(Rest, Target, Committed + 1);
        {error, Reason} -> {error, Committed, Reason}
    end.

%%----------------------------------------------------------------------
%% Migration execution (runs inside advisory lock transaction)
%%----------------------------------------------------------------------

-spec run_migrations([migration()], up | down, module(), [integer()]) -> [integer()].
run_migrations([], _Dir, _RepoMod, Acc) ->
    reverse_integers(Acc, []);
run_migrations([{Version, Module} | Rest], Dir, RepoMod, Acc) ->
    T0 = erlang:monotonic_time(),
    Ops = migration_ops(Module, Dir),
    exec_operations(Ops, Version, RepoMod),
    exec(record_sql(Dir), [Version], Version, RepoMod),
    migration_applied(Version, Module, Dir, length(Ops), T0),
    run_migrations(Rest, Dir, RepoMod, [Version | Acc]).

-spec migration_ops(module(), up | down) -> [kura_migration:operation()].
migration_ops(Module, Dir) ->
    Ops = get_migration_ops(Module, Dir),
    log_warnings(Module, check_unsafe_operations(Ops, get_safe_entries(Module))),
    Ops.

-spec record_sql(up | down) -> binary().
record_sql(up) -> ~"INSERT INTO schema_migrations (version) VALUES ($1)";
record_sql(down) -> ~"DELETE FROM schema_migrations WHERE version = $1".

-spec migration_applied(integer(), module(), up | down, non_neg_integer(), integer()) -> ok.
migration_applied(Version, Module, Dir, OpCount, T0) ->
    logger:info("Kura: ~s migration ~p (~p)", [Dir, Version, Module]),
    telemetry_event(
        [kura, migrator, migration, apply],
        #{duration => erlang:monotonic_time() - T0},
        #{
            version => Version,
            module => Module,
            direction => Dir,
            op_count => OpCount
        }
    ).

%% Stamp migrations as applied without running any DDL (baseline / fake apply).
%% Physically separate from run_migrations so the "never executes DDL"
%% invariant is local and auditable: this path only ever INSERTs the version.
-spec stamp_migrations([{integer(), module()}], module(), [integer()]) -> [integer()].
stamp_migrations([], _RepoMod, Acc) ->
    reverse_integers(Acc, []);
stamp_migrations([{Version, Module} | Rest], RepoMod, Acc) ->
    exec(
        ~"INSERT INTO schema_migrations (version) VALUES ($1)",
        [Version],
        Version,
        RepoMod
    ),
    logger:info("Kura: fake migration ~p (~p)", [Version, Module]),
    telemetry_event(
        [kura, migrator, migration, apply],
        #{duration => 0},
        #{
            version => Version,
            module => Module,
            direction => fake,
            op_count => 0
        }
    ),
    stamp_migrations(Rest, RepoMod, [Version | Acc]).

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

-spec exec(binary(), list(), integer(), target()) -> ok.
exec(SQL, Params, Version, Target) ->
    case run_sql(Target, SQL, Params) of
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
compile_operation(RepoMod, {create_index, Table, Columns, Opts}) when is_map(Opts) ->
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
        concurrently(RepoMod, maps:get(concurrently, Opts, false)),
        quote(IdxName),
        ~" ON ",
        quote(Table),
        ~" (",
        Cols,
        ~")",
        Where
    ]);
compile_operation(RepoMod, {create_index, IdxName, Table, Columns, Opts}) ->
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
        concurrently(RepoMod, lists:member(concurrently, Opts)),
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

%% A non-Postgres backend without `concurrent_indexes` (SQLite) has no
%% online index build, and a plain `CREATE INDEX` is what it would run
%% anyway. A Postgres pool without it (kura_postgres 1.1.3 and earlier)
%% would take a lock blocking writes for the whole build, so it is refused.
-spec concurrently(module(), boolean()) -> binary().
concurrently(RepoMod, true) ->
    PoolMod = kura_db:get_pool_module(RepoMod),
    _ = code:ensure_loaded(PoolMod),
    case kura_capabilities:has(PoolMod, concurrent_indexes) of
        true -> ~"CONCURRENTLY ";
        false -> plain_index_build(RepoMod, PoolMod)
    end;
concurrently(_RepoMod, false) ->
    <<>>.

-spec plain_index_build(module(), module()) -> binary().
plain_index_build(RepoMod, PoolMod) ->
    case postgres_ddl(RepoMod) of
        false ->
            <<>>;
        true ->
            ?LOG_ERROR(#{
                msg => ~"kura: the pool cannot build an index concurrently",
                repo => RepoMod,
                pool_module => PoolMod,
                hint => ~"upgrade kura_postgres to a release declaring concurrent_indexes"
            }),
            error({migration_aborted, {concurrent_indexes_unsupported, PoolMod}})
    end.

-spec postgres_ddl(module()) -> boolean().
postgres_ddl(RepoMod) ->
    case ddl_dialect(RepoMod) of
        {ok, kura_dialect_pg} -> true;
        {ok, _Other} -> false;
        none -> true
    end.

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
compile_table_constraint({primary_key, Cols}) ->
    ColList = iolist_to_binary(
        join_comma_iodata([quote(atom_to_binary(C, utf8)) || C <- Cols])
    ),
    <<"PRIMARY KEY (", ColList/binary, ")">>;
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

-spec compile_fk_action(binary(), kura_schema:on_delete() | undefined) -> binary().
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

%% Lenient: `status/1` reports a table it cannot read as nothing applied.
-spec get_applied_versions(target()) -> [integer()].
get_applied_versions(Target) ->
    case run_sql(Target, ~"SELECT version FROM schema_migrations ORDER BY version", []) of
        #{rows := Rows} ->
            [V || #{version := V} <- Rows];
        _ ->
            []
    end.

%% For every path that decides what to run: a read that fails is not an
%% empty table, and taking it for one would run applied migrations again.
-spec applied_versions_strict(target()) -> [integer()].
applied_versions_strict(Target) ->
    case run_sql(Target, ~"SELECT version FROM schema_migrations ORDER BY version", []) of
        #{rows := Rows} ->
            [V || #{version := V} <- Rows];
        Other ->
            ?LOG_ERROR(#{
                msg => ~"kura: could not read schema_migrations; running nothing",
                reason => Other
            }),
            error({migration_aborted, {schema_migrations_unreadable, Other}})
    end.

%% Every statement the migrator runs goes through here, so no server
%% error reaches its logs, results or telemetry with row values in it.
-spec run_sql(target(), binary(), list()) -> map() | {error, term()}.
run_sql({conn, RepoMod, Conn}, SQL, Params) ->
    without_row_values(kura_db:query_on(RepoMod, Conn, SQL, Params));
run_sql(RepoMod, SQL, Params) ->
    without_row_values(kura_db:query(RepoMod, SQL, Params)).

%% A failed `CREATE UNIQUE INDEX` puts the duplicated key values in
%% `detail`, and `where` and `internal_query` can quote data too.
-spec without_row_values(map() | {error, term()}) -> map() | {error, term()}.
without_row_values({error, {pgsql_error, Fields}}) when is_map(Fields) ->
    {error,
        {pgsql_error,
            maps:with(
                [
                    code,
                    message,
                    severity,
                    severity_non_localised,
                    hint,
                    schema,
                    table,
                    column,
                    data_type,
                    constraint
                ],
                Fields
            )}};
without_row_values(Result) ->
    Result.

-spec target_repo(conn_target()) -> module().
target_repo({conn, RepoMod, _Conn}) -> RepoMod.

-spec quote(binary() | atom()) -> binary().
quote(Name) when is_binary(Name) ->
    Escaped = binary:replace(Name, ~"\"", ~"\"\"", [global]),
    <<$", Escaped/binary, $">>;
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
                false -> kura_dialect_pg:format_default(Val)
            end;
        none ->
            kura_dialect_pg:format_default(Val)
    end.

-spec ddl_dialect(module()) -> {ok, module()} | none.
ddl_dialect(RepoMod) ->
    Cfg = kura_repo:settings(RepoMod),
    case maps:get(dialect, Cfg, undefined) of
        M when is_atom(M), M =/= undefined ->
            {ok, M};
        _ ->
            case application:get_env(kura, dialect) of
                {ok, GD} when is_atom(GD) -> {ok, GD};
                _ -> none
            end
    end.

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

%% `Selected` is the rollback window taken from `schema_migrations`.
%% Versions in it that no discovered module claims used to be dropped
%% here, so `rollback(Repo, 3)` could roll back two - or none - and still
%% report success, leaving the rows behind and undoing migrations out of
%% order around the gap. They are now named and the rollback refuses.
-spec build_rollback_pairs([integer()], [{integer(), module()}]) ->
    {ok, [{integer(), module()}]} | {error, {unknown_applied_versions, [integer()]}}.
build_rollback_pairs(Selected, Ordered) ->
    Known = #{V => M || {V, M} <- Ordered},
    case [V || V <- Selected, not maps:is_key(V, Known)] of
        [] ->
            Reversed = reverse_migrations(Ordered, []),
            {ok, [{V, M} || {V, M} <- Reversed, lists:member(V, Selected)]};
        Missing ->
            {error, {unknown_applied_versions, sort_integers(Missing)}}
    end.

-spec reverse_migrations([{integer(), module()}], [{integer(), module()}]) ->
    [{integer(), module()}].
reverse_migrations([], Acc) -> Acc;
reverse_migrations([H | T], Acc) -> reverse_migrations(T, [H | Acc]).

-spec reverse_atoms([atom()], [atom()]) -> [atom()].
reverse_atoms([], Acc) -> Acc;
reverse_atoms([H | T], Acc) -> reverse_atoms(T, [H | Acc]).

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
