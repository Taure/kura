-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1, pool_config/0]).

start(_StartType, _StartArgs) ->
    resolve_backend(),
    maybe_configure_pg_types(),
    kura_query_cache:init(),
    ok = ensure_pool(),
    kura_sup:start_link().

stop(_State) ->
    ok.

%% If `{kura, [{backend, BackendMod}]}` is set, populate dialect,
%% pool_module, and driver_module from the aggregator. Explicit
%% per-key overrides win.
-spec resolve_backend() -> ok.
resolve_backend() ->
    case application:get_env(kura, backend) of
        {ok, Backend} when is_atom(Backend) ->
            ensure_set_env(dialect, fun() -> Backend:dialect() end),
            ensure_set_env(pool_module, fun() -> Backend:pool_module() end),
            ensure_set_env(driver_module, fun() -> Backend:driver_module() end),
            ok;
        _ ->
            ok
    end.

-spec ensure_set_env(atom(), fun(() -> term())) -> ok.
ensure_set_env(Key, Resolve) ->
    case application:get_env(kura, Key) of
        {ok, _} -> ok;
        _ -> application:set_env(kura, Key, Resolve())
    end.

-spec ensure_pool() -> ok.
ensure_pool() ->
    case application:get_env(kura, repo) of
        {ok, Repo} when is_atom(Repo) ->
            Config = pool_config(),
            PoolMod = kura_db:get_pool_module(Repo),
            case PoolMod:start_pool(Repo, Config) of
                {ok, _Pid} -> ok;
                {error, {already_started, _Pid}} -> ok;
                {error, _} -> ok
            end;
        _ ->
            ok
    end.

-spec pool_config() -> map().
pool_config() ->
    case application:get_env(kura, dialect) of
        {ok, kura_dialect_pg} -> pg_pool_config();
        _ -> generic_pool_config()
    end.

%% Pass-through map of user-set kura env keys, minus bookkeeping
%% (`repo`, `backend`, plugin-resolution keys, migrator tunables).
%% Backends pick the keys they understand from this map.
-spec generic_pool_config() -> map().
generic_pool_config() ->
    Excluded = [
        repo,
        backend,
        dialect,
        pool_module,
        driver_module,
        ensure_database,
        migration_pool_ready_timeout,
        migration_pool_ready_interval,
        pg_types
    ],
    All = application:get_all_env(kura),
    maps:from_list([{K, V} || {K, V} <- All, not lists:member(K, Excluded)]).

%% PG-shaped pool config with the historical defaults (port 5432,
%% user/database "postgres", etc.). Kept stable for back-compat.
-spec pg_pool_config() -> map().
pg_pool_config() ->
    Env = fun(Key, Default) -> application:get_env(kura, Key, Default) end,
    Base = #{
        host => Env(host, "localhost"),
        port => Env(port, 5432),
        database => Env(database, "postgres"),
        user => Env(user, "postgres"),
        password => Env(password, ""),
        pool_size => Env(pool_size, 10),
        decode_opts => [return_rows_as_maps, column_name_as_atom]
    },
    WithSocket =
        case Env(socket_options, []) of
            Opts when is_list(Opts), Opts =/= [] -> Base#{socket_options => Opts};
            _ -> Base
        end,
    WithSSL =
        case Env(ssl, false) of
            true -> WithSocket#{ssl => true};
            _ -> WithSocket
        end,
    case Env(ssl_options, []) of
        SSLOpts when is_list(SSLOpts), SSLOpts =/= [] -> WithSSL#{ssl_options => SSLOpts};
        _ -> WithSSL
    end.

%% Only set pg_types defaults when the configured dialect is the PG one.
-spec maybe_configure_pg_types() -> ok.
maybe_configure_pg_types() ->
    case application:get_env(kura, dialect) of
        {ok, kura_dialect_pg} -> configure_pg_types();
        _ -> ok
    end.

-spec configure_pg_types() -> ok.
configure_pg_types() ->
    Defaults = #{uuid_format => string},
    UserConfig =
        case application:get_env(kura, pg_types) of
            {ok, M} when is_map(M) -> M;
            _ -> #{}
        end,
    Merged = maps:merge(Defaults, UserConfig),
    maps:foreach(
        fun(Key, Val) -> application:set_env(pg_types, Key, Val) end,
        Merged
    ).
