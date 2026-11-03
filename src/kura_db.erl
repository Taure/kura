-module(kura_db).
-moduledoc """
Central database wrapper. Routes all SQL through a configured
`kura_driver` impl over a configured `kura_pool` impl, so swapping
the underlying client (minato, esqlite, ...) is one config change.

All driver calls go through this module, providing a single place
for pool lookup, sandbox support, telemetry, and type loading.
""".

-export([
    query/3,
    query_on/4,
    query_on/5,
    transaction/2,
    transaction_ok/2,
    in_transaction/1,
    get_pool/1,
    get_pool_module/1,
    start_pool/1,
    start_pool/3,
    stop_pool/1,
    resolve/1,
    get_driver_module/1,
    load_row/2,
    build_log_event/5,
    build_telemetry_metadata/4,
    extract_source/1,
    default_logger/0
]).

-ifdef(TEST).
-export([
    narrow_ok_error/1,
    forget_pools/0,
    held/1,
    extract_result_status/1,
    extract_num_rows/1,
    emit_telemetry/6
]).
-endif.

-include_lib("kernel/include/logger.hrl").

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

%% `key` is the TLS private key in ssl options; `encryption` holds the
%% field-encryption keys and `keyring` names where they come from.
-define(IS_SECRET_KEY(Key),
    (Key =:= password orelse Key =:= key orelse Key =:= encryption orelse Key =:= keyring)
).

-define(REPO_KEYS, [
    backend,
    dialect,
    pool,
    pool_module,
    driver_module,
    read_only,
    replicas,
    uuid_version,
    encryption,
    keyring
]).

%%----------------------------------------------------------------------
%% Query
%%----------------------------------------------------------------------

-spec query(module(), iodata(), [term()]) -> map() | {error, term()}.
query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    DriverMod = get_driver_module(RepoMod),
    T0 = erlang:monotonic_time(),
    Result =
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                in_sandbox(DriverMod, Conn, SQL, Params);
            not_found ->
                PoolMod = get_pool_module(RepoMod),
                DriverMod:query(PoolMod, Pool, SQL, Params, #{decode_opts => ?DECODE_OPTS})
        end,
    observed(RepoMod, SQL, Params, T0, Result).

-doc """
Run `SQL` on `Conn`, a connection the caller checked out of `RepoMod`'s
pool itself, through the repo's driver.

Unlike `query/3` it neither looks for a sandbox connection nor routes to
a transaction the process is in: the statement runs on `Conn`, in
whatever transaction state `Conn` is. Telemetry is the same as for
`query/3`. `kura_migrator` uses it to run a non-transactional migration
and hold its session-level advisory lock on one connection.

Same as `query_on/5` with no options.
""".
-spec query_on(module(), kura_pool:conn(), iodata(), [term()]) -> map() | {error, term()}.
query_on(RepoMod, Conn, SQL, Params) ->
    query_on(RepoMod, Conn, SQL, Params, #{}).

-doc """
Run `SQL` on `Conn` as `query_on/4` does, handing `Opts` to the driver's
`c:kura_driver:query_on/4`.

`Opts` is driver-specific (see `t:kura_driver:opts/0`); the minato driver
honours `timeout`. kura's own `decode_opts` replace any given here, since
kura reads the rows in that shape.
""".
-spec query_on(module(), kura_pool:conn(), iodata(), [term()], kura_driver:opts()) ->
    map() | {error, term()}.
query_on(RepoMod, Conn, SQL, Params, Opts) ->
    DriverMod = get_driver_module(RepoMod),
    T0 = erlang:monotonic_time(),
    Result = DriverMod:query_on(Conn, SQL, Params, Opts#{decode_opts => ?DECODE_OPTS}),
    observed(RepoMod, SQL, Params, T0, Result).

-spec observed(module(), iodata(), [term()], integer(), Result) -> Result.
observed(RepoMod, SQL, Params, T0, Result) ->
    DurationNative = erlang:monotonic_time() - T0,
    DurationUs = erlang:convert_time_unit(DurationNative, native, microsecond),
    emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs),
    Result.

%%----------------------------------------------------------------------
%% Transaction
%%----------------------------------------------------------------------

-doc """
Run `Fun` in a transaction on `RepoMod`'s pool, through its driver.

The driver's `c:kura_driver:transaction/4` decides what a nested call
does and what a failure raises. In a sandboxed test the sandbox
connection is already inside a transaction, so `Fun` runs under a
`SAVEPOINT` on it instead, the way a driver runs a nested transaction:
an exception rolls back to the savepoint and is re-raised, and a
savepoint the database will not release raises
`transaction_rolled_back` after rolling back to it. A statement that ended
the sandbox's transaction raises `{DriverMod, transaction_ended_by_statement}`
here too, when the driver checks for it (see `m:kura_sandbox`). Another process
opening a transaction on the same sandbox connection while this one is
open raises `{kura_sandbox, overlapping_transactions}`.
""".
-spec transaction(module(), fun(() -> Result)) -> Result.
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    DriverMod = get_driver_module(RepoMod),
    Nested = erlang:get({?MODULE, transaction, Pool}) =:= true,
    _ = erlang:put({?MODULE, transaction, Pool}, true),
    try
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                sandboxed(DriverMod, Conn, Fun);
            not_found ->
                PoolMod = get_pool_module(RepoMod),
                DriverMod:transaction(PoolMod, Pool, Fun, #{})
        end
    after
        left_transaction(Pool, Nested)
    end.

-doc """
Whether this process is inside `transaction/2` on `RepoMod`'s pool. A
process it spawns is not, and would not see the transaction's
uncommitted work.
""".
-spec in_transaction(module()) -> boolean().
in_transaction(RepoMod) ->
    erlang:get({?MODULE, transaction, get_pool(RepoMod)}) =:= true.

-spec left_transaction(atom(), boolean()) -> ok.
left_transaction(Pool, false) ->
    _ = erlang:erase({?MODULE, transaction, Pool}),
    ok;
left_transaction(_Pool, true) ->
    ok.

-spec sandboxed(module(), kura_pool:conn(), fun(() -> Result)) -> Result.
sandboxed(DriverMod, Conn, Fun) ->
    ok = kura_sandbox:enter_transaction(Conn),
    try
        savepointed(DriverMod, Conn, Fun)
    after
        ok = kura_sandbox:leave_transaction(Conn)
    end.

-spec savepointed(module(), kura_pool:conn(), fun(() -> Result)) -> Result.
savepointed(DriverMod, Conn, Fun) ->
    Savepoint = [
        ~"kura_savepoint_", integer_to_binary(erlang:unique_integer([positive, monotonic]))
    ],
    case on_sandbox(DriverMod, Conn, [~"SAVEPOINT ", Savepoint]) of
        ended -> error({DriverMod, transaction_ended_by_statement});
        {error, Refused} -> error({DriverMod, {savepoint_failed, reduced(Refused)}});
        _Started -> ok
    end,
    try Fun() of
        Value -> released(DriverMod, Conn, Savepoint, Value)
    catch
        Class:Reason:Stack ->
            ok = rolled_back_to(DriverMod, Conn, Savepoint),
            erlang:raise(Class, Reason, Stack)
    end.

-spec released(module(), kura_pool:conn(), iodata(), Result) -> Result.
released(DriverMod, Conn, Savepoint, Value) ->
    case on_sandbox(DriverMod, Conn, [~"RELEASE SAVEPOINT ", Savepoint]) of
        ended ->
            error({DriverMod, transaction_ended_by_statement});
        {error, _} ->
            ok = rolled_back_to(DriverMod, Conn, Savepoint),
            error(transaction_rolled_back);
        _Released ->
            Value
    end.

-spec rolled_back_to(module(), kura_pool:conn(), iodata()) -> ok.
rolled_back_to(DriverMod, Conn, Savepoint) ->
    _ = on_sandbox(DriverMod, Conn, [~"ROLLBACK TO SAVEPOINT ", Savepoint]),
    _ = on_sandbox(DriverMod, Conn, [~"RELEASE SAVEPOINT ", Savepoint]),
    ok.

%% A server error's message and detail can carry row values.
-spec reduced(term()) -> term().
reduced({pgsql_error, Fields}) when is_map(Fields) ->
    {pgsql_error, maps:with([code, constraint, severity, severity_non_localised], Fields)};
reduced(Reason) ->
    Reason.

%% Once a statement has ended the sandbox's transaction, nothing meant for it
%% runs: it would run in autocommit and keep its work.
-spec in_sandbox(module(), kura_pool:conn(), iodata(), [term()]) -> dynamic().
in_sandbox(DriverMod, Conn, SQL, Params) ->
    case kura_sandbox:ended(Conn) of
        true ->
            error({DriverMod, transaction_ended_by_statement});
        false ->
            try
                DriverMod:query_on(Conn, SQL, Params, #{
                    decode_opts => ?DECODE_OPTS, transaction => inside
                })
            catch
                error:{DriverMod, transaction_ended_by_statement} = Ended:Stack ->
                    ok = kura_sandbox:note_ended(Conn),
                    erlang:raise(error, Ended, Stack)
            end
    end.

%% A raise here would replace the exception being cleaned up after.
-spec on_sandbox(module(), kura_pool:conn(), iodata()) -> dynamic().
on_sandbox(DriverMod, Conn, SQL) ->
    try
        in_sandbox(DriverMod, Conn, SQL, [])
    catch
        error:{DriverMod, transaction_ended_by_statement} -> ended;
        Class:_ -> {error, {raised, Class}}
    end.

-spec transaction_ok(module(), fun(() -> term())) -> {ok, term()} | {error, term()}.
transaction_ok(RepoMod, Fun) ->
    narrow_ok_error(transaction(RepoMod, Fun)).

%%----------------------------------------------------------------------
%% Pool
%%----------------------------------------------------------------------

-doc """
The pool `RepoMod`'s queries run on.

Raises `{pool_owned_by, Owner}` when another repo owns that pool name: a repo
kura has not started, or one stopped with `stop_pool/1`, reads its pool name
from its config, and must not reach a pool started with another repo's
credentials. A repo whose last start was refused raises
`{pool_owned_by, Owner}` too, and one whose last start moved it to a pool that
did not start raises `{pool_not_started, Pool}`, until a start succeeds or
`stop_pool/1` drops that state.
""".
-spec get_pool(module()) -> atom().
get_pool(RepoMod) ->
    Pool = pool_name(RepoMod, kura_repo:settings(RepoMod)),
    case owner(Pool) of
        Other when Other =/= undefined, Other =/= RepoMod -> error({pool_owned_by, Other});
        _OwnedOrUnclaimed -> Pool
    end.

-spec pool_name(module(), map()) -> atom().
pool_name(RepoMod, Config) ->
    maps:get(pool, Config, RepoMod).

-doc "Resolve the `kura_pool` implementation module for a repo.".
-spec get_pool_module(module()) -> module().
get_pool_module(RepoMod) ->
    pool_module(RepoMod, kura_repo:settings(RepoMod)).

-spec pool_module(module(), map()) -> module().
pool_module(RepoMod, Config) ->
    case maps:get(pool_module, Config, undefined) of
        M when is_atom(M), M =/= undefined -> M;
        _ -> error({no_pool_module_configured, RepoMod})
    end.

-doc """
Start `Repo`'s pool from its config, as `kura_repo:config/1` resolves it.

`Repo:init/1` has run on that config, and the pool takes its name from
the `pool` key, as `get_pool/1` does, so queries reach the pool started
here. The repo module is loaded first: `kura_repo:config/1` only calls an
`init/1` it finds loaded, and at boot nothing has called into the module
yet. Application boot and `kura_repo_worker:start/1` both start pools
through this function. The pool module applies its own connection defaults.

Deprecated: the legacy flat single-repo form (`{repo, Repo}` in the kura env,
no `repos` map) on `kura_dialect_pg` still gets `user` and `database` set to
`"postgres"` when it leaves them out, with a warning naming the keys filled.
Kura 3.0 removes this.

An `init/1` that raises starts no pool. Only the shape of its reason is kept:
an atom as it is, a tuple as `{Tag, redacted}` when its first element is an
atom, anything else as `redacted`. The failure is logged that way and returned
as `{error, {init_failed, {Class, Shape}}}`.

Once the pool runs, the repo's settings are kept (see `kura_repo:settings/1`)
and queries read them from there, so `init/1` does not run per query. Starting
the pool again, through `kura_repo_worker:start/1`, resolves the config anew
and replaces them.

The repo that starts a pool owns it until `stop_pool/1`, or until the repo
starts on another pool name: the pool it had is then stopped and released,
after the new one is started. If that stop raises, the start still returns
`ok`, the failure is logged with only the shape of its reason, and the old
pool stays owned until `stop_pool/1` stops it. A repo whose config
names a pool another repo owns is refused with `{error, {pool_owned_by, Owner}}`
before the pool module is called, so the answer does not depend on how the
pool module reports a name that is already running. The refusal is logged as a
pool that did not start, and the refused repo's queries raise
`{pool_owned_by, Owner}` instead of running on that pool. The owner starting
its pool again gets `ok`. A pool already running that kura did not start goes
to the first repo that starts it, with a warning that names the repo, the pool
and the pool module: that pool keeps the connection settings it was started
with. A pool stopped outside kura stays owned;
`stop_pool/1` releases it.

Starts and stops of one pool name are serialised by a node-local `global`
lock that waits without a limit: a pool module whose `start_pool/2` or
`stop_pool/1` hangs holds up every other start or stop of that name. Queries
never take the lock. The record of which pool a repo is on is changed under a
second lock, per repo and held only for that change, so two starts of one repo
on two pool names each find the pool the other moved it to, and the one moved
from is stopped.

A start that fails leaves the stored settings as they were when the repo's
pool is the one it was started on before, or when only `init/1` failed. When
the config now names another pool and that pool does not start, the repo's
queries raise `{pool_not_started, Pool}` rather than run on the previous pool.
""".
-spec start_pool(module()) -> ok | {error, term()}.
start_pool(Repo) ->
    case resolved(Repo) of
        {ok, Pool, Config, Filled} ->
            ok = legacy_defaults_filled(Repo, Filled),
            start_pool(Repo, Pool, Config);
        {error, Failed, Stack} ->
            ok = not_started(Repo, undefined, #{reason => Failed, stacktrace => arities(Stack)}),
            {error, Failed}
    end.

-doc false.
-spec resolve(module()) -> {ok, atom(), map()} | {error, init_failed()}.
resolve(Repo) ->
    case resolved(Repo) of
        {ok, Pool, Config, _Filled} -> {ok, Pool, Config};
        {error, Failed, _Stack} -> {error, Failed}
    end.

-type init_failed() :: {init_failed, {error | exit | throw, atom() | {atom(), redacted}}}.

-spec resolved(module()) ->
    {ok, atom(), map(), [atom()]} | {error, init_failed(), erlang:stacktrace()}.
resolved(Repo) ->
    _ = code:ensure_loaded(Repo),
    try kura_repo:config(Repo) of
        Resolved ->
            {Config, Filled} = legacy_defaults(Repo, Resolved),
            {ok, pool_name(Repo, Config), Config, Filled}
    catch
        Class:Reason:Stack -> {error, {init_failed, {Class, shape(Reason)}}, Stack}
    end.

%% Deprecated, removed in kura 3.0: the flat single-repo form on Postgres keeps
%% the `user` and `database` kura filled in before 2.24.
-spec legacy_defaults(module(), map()) -> {map(), [atom()]}.
legacy_defaults(Repo, #{dialect := kura_dialect_pg} = Config) ->
    case is_legacy_flat(Repo) of
        true ->
            Filled = [
                Key
             || {Key, Aliases} <- [{user, [user, username]}, {database, [database]}],
                not lists:any(fun(Alias) -> is_map_key(Alias, Config) end, Aliases)
            ],
            {maps:merge(#{Key => "postgres" || Key <- Filled}, Config), Filled};
        false ->
            {Config, []}
    end;
legacy_defaults(_Repo, Config) ->
    {Config, []}.

-spec is_legacy_flat(module()) -> boolean().
is_legacy_flat(Repo) ->
    case application:get_env(kura, repos) of
        {ok, Repos} when is_map(Repos) -> false;
        _ -> application:get_env(kura, repo) =:= {ok, Repo}
    end.

-spec legacy_defaults_filled(module(), [atom()]) -> ok.
legacy_defaults_filled(_Repo, []) ->
    ok;
legacy_defaults_filled(Repo, Filled) ->
    ?LOG_WARNING(#{
        msg => ~"kura: deprecated Postgres defaults filled in for the flat repo config",
        repo => Repo,
        filled => Filled,
        value => ~"postgres",
        action => ~"set these keys explicitly; kura 3.0 removes this fill"
    }),
    ok.

-doc false.
-spec start_pool(module(), atom(), map()) -> ok | {error, term()}.
start_pool(Repo, Pool, Config) ->
    PoolMod = pool_module(Repo, Config),
    case locked(Pool, fun() -> claimed(Repo, Pool, PoolMod, Config) end) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        {moved_from, Previous, PreviousMod} -> retire(Repo, Previous, PreviousMod)
    end.

-type claim() :: ok | {error, term()} | {moved_from, atom(), module()}.

%% Two repos starting one pool name at once must not both see it unowned.
%% Only pool start and stop take the lock; queries never do. The requester
%% is the caller, as global grants a lock again to the same requester.
-spec locked(atom(), fun(() -> claim())) -> claim().
locked(Pool, Run) ->
    case global:trans({{kura_pool_start, Pool}, self()}, Run, [node()]) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason};
        {moved_from, Previous, PreviousMod} when is_atom(Previous), is_atom(PreviousMod) ->
            {moved_from, Previous, PreviousMod}
    end.

%% A repo that moved to another pool name owns the pool it left until that
%% pool is stopped, so no other repo can adopt it while it still runs with
%% this repo's credentials. Its lock is taken after the new pool's is let go.
-spec retire(module(), atom(), module()) -> ok.
retire(Repo, Pool, PoolMod) ->
    ok = locked(Pool, fun() -> retired(Repo, Pool, PoolMod) end).

-spec retired(module(), atom(), module()) -> ok.
retired(Repo, Pool, PoolMod) ->
    case {owner(Pool), held(Repo)} of
        {Repo, {Pool, _MovedBack}} -> ok;
        {Repo, _HeldElsewhereOrStopped} -> stopped_owned(Repo, Pool, PoolMod);
        _NotOwned -> ok
    end.

%% A pool that will not stop is logged and stays owned, so the stop that
%% failed can be tried again with stop_pool/1 rather than failing the start
%% or stop that ran into it.
-spec stopped_owned(module(), atom(), module()) -> ok.
stopped_owned(Repo, Pool, PoolMod) ->
    try PoolMod:stop_pool(Pool) of
        _Stopped -> released(Repo, Pool)
    catch
        Class:Reason:Stack ->
            ?LOG_ERROR(#{
                msg => ~"kura: pool did not stop",
                repo => Repo,
                pool => Pool,
                pool_module => PoolMod,
                reason => {Class, shape(Reason)},
                stacktrace => arities(Stack)
            }),
            ok
    end.

%% Every pool whose owner record names the repo. A start that died after
%% it claimed its pool, or before it retired the one it left, leaves a pool
%% the repo owns but no longer holds, and stop_pool/1 still reaches it here.
-spec owned_by(module()) -> [{atom(), module()}].
owned_by(Repo) ->
    [
        {Pool, PoolMod}
     || {{?MODULE, pool_owner, Pool}, {R, PoolMod}} <- persistent_term:get(),
        R =:= Repo,
        is_atom(Pool),
        is_atom(PoolMod)
    ].

-spec claimed(module(), atom(), module(), map()) -> claim().
claimed(Repo, Pool, PoolMod, Config) ->
    case owner(Pool) of
        Other when Other =/= undefined, Other =/= Repo ->
            Refused = {pool_owned_by, Other},
            ok = kura_repo_settings:refused(Repo, Other),
            ok = not_started(Repo, PoolMod, #{reason => Refused, pool => Pool}),
            {error, Refused};
        Owner ->
            pool_started(Repo, Pool, PoolMod, Config, Owner =:= Repo)
    end.

-spec pool_started(module(), atom(), module(), map(), boolean()) -> claim().
pool_started(Repo, Pool, PoolMod, Config, Owned) ->
    Secrets = secrets(Config),
    try PoolMod:start_pool(Pool, maps:without(?REPO_KEYS, Config)) of
        {ok, _Pid} ->
            owned(Repo, Pool, PoolMod, Config);
        {error, {already_started, _Pid}} ->
            ok = adopted(Repo, Pool, PoolMod, Owned),
            owned(Repo, Pool, PoolMod, Config);
        {error, Reason} ->
            Redacted = redacted(Reason, Secrets),
            ok = not_running(Repo, Pool),
            ok = not_started(Repo, PoolMod, #{reason => Redacted}),
            {error, Redacted}
    catch
        Class:Reason:Stack ->
            Redacted = {Class, redacted(Reason, Secrets)},
            ok = not_running(Repo, Pool),
            ok = not_started(Repo, PoolMod, #{reason => Redacted, stacktrace => arities(Stack)}),
            error({pool_start_failed, Repo, Redacted})
    end.

%% A running pool kura did not start keeps the settings it was started with,
%% which may not be the repo's.
-spec adopted(module(), atom(), module(), boolean()) -> ok.
adopted(_Repo, _Pool, _PoolMod, true) ->
    ok;
adopted(Repo, Pool, PoolMod, false) ->
    ?LOG_WARNING(#{
        msg => ~"kura: adopted a running pool kura did not start",
        repo => Repo,
        pool => Pool,
        pool_module => PoolMod,
        action => ~"it keeps the connection settings it was started with"
    }),
    ok.

%% A repo moved to a pool that did not start must not keep querying the pool
%% it was on before, nor report the reason an earlier start failed. A failed
%% restart on the same pool keeps its settings.
-spec not_running(module(), atom()) -> ok.
not_running(Repo, Pool) ->
    case kura_repo_settings:lookup(Repo) of
        #{pool := Pool} -> ok;
        none -> ok;
        _OtherPoolOrMarker -> kura_repo_settings:not_started(Repo, Pool)
    end.

%% A repo holds the pool it started last; one it held before is retired.
%% The pool is claimed before the repo is recorded on it, so every pool a
%% repo's queries can reach is owned, and a start that dies between the two
%% leaves a pool owned_by/1 finds.
-spec owned(module(), atom(), module(), map()) -> claim().
owned(Repo, Pool, PoolMod, Config) ->
    ok = persistent_term:put({?MODULE, pool_owner, Pool}, {Repo, PoolMod}),
    case moved(Repo, Pool, PoolMod, Config) of
        {PreviousPool, PreviousMod} when PreviousPool =/= Pool ->
            {moved_from, PreviousPool, PreviousMod};
        _SamePoolOrNone ->
            ok
    end.

%% The pool locks of two starts of one repo on two pool names do not
%% exclude each other, so the read of the pool the repo was on and the
%% write of the new one happen under the repo's lock.
-spec moved(module(), atom(), module(), map()) -> {atom(), module()} | none.
moved(Repo, Pool, PoolMod, Config) ->
    case
        repo_locked(Repo, fun() ->
            Previous = held(Repo),
            ok = persistent_term:put({?MODULE, pool_held, Repo}, {Pool, PoolMod}),
            ok = kura_repo_settings:started(Repo, Config#{pool => Pool}),
            Previous
        end)
    of
        {PreviousPool, PreviousMod} when is_atom(PreviousPool), is_atom(PreviousMod) ->
            {PreviousPool, PreviousMod};
        none ->
            none
    end.

-spec repo_locked(module(), fun(() -> term())) -> term().
repo_locked(Repo, Run) ->
    global:trans({{kura_repo_pool, Repo}, self()}, Run, [node()]).

-spec released(module(), atom()) -> ok.
released(Repo, Pool) ->
    case owner(Pool) of
        Repo ->
            _ = persistent_term:erase({?MODULE, pool_owner, Pool}),
            ok;
        _NotOwned ->
            ok
    end.

-spec held(module()) -> {atom(), module()} | none.
held(Repo) ->
    case persistent_term:get({?MODULE, pool_held, Repo}, none) of
        {Pool, PoolMod} when is_atom(Pool), is_atom(PoolMod) -> {Pool, PoolMod};
        _None -> none
    end.

-spec owner(atom()) -> module() | undefined.
owner(Pool) ->
    case persistent_term:get({?MODULE, pool_owner, Pool}, undefined) of
        {Repo, PoolMod} when is_atom(Repo), is_atom(PoolMod) -> Repo;
        _Unclaimed -> undefined
    end.

-doc """
Stop the pool `Repo` last started through `start_pool/1` and release it.

The pool module's `stop_pool/1` stops the pool, the pool name is no longer
owned by `Repo`, so another repo can start it, and `Repo`'s stored settings
are dropped: its queries read its config on each call again, as for a repo
kura has not started. Any other pool `Repo` still owns, one it moved away from
whose stop failed or whose start died before stopping it, is stopped and
released too. A repo that does not own a running pool, one refused another
repo's pool among them, stops nothing and only has its stored state dropped.

A stop that raises is logged with only the shape of its reason, and that pool
stays owned, with the repo's settings kept when it is the pool the repo is
on, until a later `stop_pool/1` stops it. The other pools are still stopped.

A stopped repo should not keep issuing queries. If another repo starts the
same pool name, they raise `{pool_owned_by, Owner}`; otherwise they fail in
the pool module, as for any pool that is not running.
""".
-spec stop_pool(module()) -> ok.
stop_pool(Repo) ->
    case held(Repo) of
        {Pool, PoolMod} -> ok = locked(Pool, fun() -> stopped(Repo, Pool, PoolMod) end);
        none -> ok = kura_repo_settings:forget(Repo)
    end,
    lists:foreach(fun({Pool, PoolMod}) -> retire(Repo, Pool, PoolMod) end, owned_by(Repo)).

%% A start of the same repo can move it to another pool while this runs; the
%% pool it moved to stays held, so a later stop_pool/1 still reaches it.
-spec stopped(module(), atom(), module()) -> ok.
stopped(Repo, Pool, PoolMod) ->
    case {held(Repo), owner(Pool)} of
        {{Pool, PoolMod}, Repo} ->
            ok = stopped_owned(Repo, Pool, PoolMod),
            unheld_when_released(Repo, Pool, PoolMod);
        {{Pool, PoolMod}, _NotOwned} ->
            unheld(Repo, Pool, PoolMod);
        _Moved ->
            ok
    end.

-spec unheld_when_released(module(), atom(), module()) -> ok.
unheld_when_released(Repo, Pool, PoolMod) ->
    case owner(Pool) of
        Repo -> ok;
        _Released -> unheld(Repo, Pool, PoolMod)
    end.

-spec unheld(module(), atom(), module()) -> ok.
unheld(Repo, Pool, PoolMod) ->
    case
        repo_locked(Repo, fun() ->
            case held(Repo) of
                {Pool, PoolMod} ->
                    _ = persistent_term:erase({?MODULE, pool_held, Repo}),
                    kura_repo_settings:forget(Repo);
                _Moved ->
                    ok
            end
        end)
    of
        ok -> ok
    end.

-ifdef(TEST).
forget_pools() ->
    All = persistent_term:get(),
    _ = [persistent_term:erase(Key) || {{?MODULE, pool_owner, _} = Key, _} <- All],
    _ = [persistent_term:erase(Key) || {{?MODULE, pool_held, _} = Key, _} <- All],
    kura_repo_settings:forget().
-endif.

%% init/1 is the consumer's code, so nothing is known about what its reason
%% holds: only its tag is kept.
-spec shape(term()) -> atom() | {atom(), redacted}.
shape(Reason) when is_atom(Reason) -> Reason;
shape(Reason) when is_tuple(Reason) -> tagged(tuple_to_list(Reason));
shape(_Reason) -> redacted.

-spec tagged([term()]) -> {atom(), redacted} | redacted.
tagged([Tag | _]) when is_atom(Tag) -> {Tag, redacted};
tagged(_Untagged) -> redacted.

-spec not_started(module(), module(), map()) -> ok.
not_started(Repo, PoolMod, Report) ->
    ?LOG_ERROR(Report#{msg => ~"kura: pool did not start", repo => Repo, pool_module => PoolMod}),
    ok.

%% A start that raises carries the config in its stacktrace arguments.
-type frame() :: {module(), atom(), arity() | [term()], [term()]}.

-spec arities(erlang:stacktrace()) -> [frame()].
arities(Stack) ->
    [arity_frame(Frame) || {_, _, _, _} = Frame <- Stack].

-spec arity_frame(frame()) -> frame().
arity_frame({Module, Function, Args, Location}) -> {Module, Function, arity(Args), Location}.

-spec arity(arity() | [term()]) -> arity().
arity(Args) when is_list(Args) -> length(Args);
arity(Arity) -> Arity.

%% A supervisor reports a failed start with the child's start arguments, and
%% those hold the connection settings, password and TLS key included.
-spec secrets(map()) -> [term()].
secrets(Config) ->
    passwords(Config) ++ tls_keys(Config).

-spec tls_keys(map()) -> [term()].
tls_keys(#{ssl_options := Options}) when is_list(Options) ->
    [Form || {key, Key} <- Options, Form <- key_forms(Key)];
tls_keys(_Config) ->
    [].

-spec key_forms(term()) -> [term()].
key_forms({_Type, Der} = Key) when is_binary(Der) -> [Key, Der];
key_forms(Key) -> [Key].

-spec passwords(map()) -> [binary() | string()].
passwords(#{password := Password}) when is_binary(Password), Password =/= <<>> ->
    forms(Password);
passwords(#{password := Password}) when is_list(Password), Password =/= [] ->
    [
        Password
        | [
            Form
         || Binary <- [as_text(Password), as_bytes(Password)],
            is_binary(Binary),
            Form <- forms(Binary)
        ]
    ];
passwords(_Config) ->
    [].

-spec forms(binary()) -> [binary() | string()].
forms(Password) ->
    Text = [List || List <- [unicode:characters_to_list(Password)], is_list(List)],
    [Password, binary_to_list(Password) | Text].

-spec as_text(string()) -> binary() | undefined.
as_text(Password) ->
    try unicode:characters_to_binary(Password) of
        Binary when is_binary(Binary) -> Binary;
        _NotText -> undefined
    catch
        error:badarg -> undefined
    end.

%% A password given as a binary reaches some pools as a list of its bytes,
%% and is turned back into the binary by list_to_binary, not as text.
-spec as_bytes(string()) -> binary() | undefined.
as_bytes(Password) ->
    try
        list_to_binary(Password)
    catch
        error:badarg -> undefined
    end.

-spec redacted(term(), [term()]) -> term().
redacted(Term, Secrets) ->
    case lists:member(Term, Secrets) of
        true -> redacted;
        false -> redacted_parts(Term, Secrets)
    end.

-spec redacted_parts(term(), [term()]) -> term().
redacted_parts({Key, _}, _Secrets) when ?IS_SECRET_KEY(Key) ->
    {Key, redacted};
redacted_parts(Map, Secrets) when is_map(Map) ->
    #{redacted(K, Secrets) => redacted_value(K, V, Secrets) || K := V <- Map};
redacted_parts(Tuple, Secrets) when is_tuple(Tuple) ->
    list_to_tuple([redacted(E, Secrets) || E <- tuple_to_list(Tuple)]);
redacted_parts(List, Secrets) when is_list(List) ->
    try length(List) of
        _Proper -> [redacted(E, Secrets) || E <- List]
    catch
        error:badarg -> redacted
    end;
redacted_parts(Term, _Secrets) ->
    Term.

-spec redacted_value(term(), term(), [term()]) -> term().
redacted_value(Key, _Value, _Secrets) when ?IS_SECRET_KEY(Key) -> redacted;
redacted_value(_Key, Value, Secrets) -> redacted(Value, Secrets).

-doc "Resolve the `kura_driver` implementation module for a repo.".
-spec get_driver_module(module()) -> module().
get_driver_module(RepoMod) ->
    case maps:get(driver_module, kura_repo:settings(RepoMod), undefined) of
        M when is_atom(M), M =/= undefined -> M;
        _ -> error({no_driver_module_configured, RepoMod})
    end.

%%----------------------------------------------------------------------
%% Row loading
%%----------------------------------------------------------------------

-spec load_row(kura_dialect:table(), map()) -> map().
load_row(Table, Row) when is_binary(Table) ->
    Row;
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

%%----------------------------------------------------------------------
%% Telemetry
%%----------------------------------------------------------------------

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
    {ResultStatus, NumRows, ErrorReason} =
        case Result of
            #{rows := Rows} -> {ok, length(Rows), undefined};
            #{num_rows := N} -> {ok, N, undefined};
            {error, Reason} -> {error, 0, Reason};
            _ -> {ok, 0, undefined}
        end,
    Source = extract_source(SQLBin),
    #{
        query => SQLBin,
        params => Params,
        repo => RepoMod,
        result => ResultStatus,
        num_rows => NumRows,
        source => Source,
        tenant => kura_tenant:get_tenant(),
        error_reason => ErrorReason
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

default_logger() ->
    fun(Event) ->
        logger:info("Kura ~s ~pus", [
            maps:get(query, Event), maps:get(duration_us, Event)
        ])
    end.

%%----------------------------------------------------------------------
%% Type narrowing
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
