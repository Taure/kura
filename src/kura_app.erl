-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1, pool_config/0, pool_config/1]).

-eqwalizer({nowarn_function, resolve_backends/0}).
-eqwalizer({nowarn_function, ensure_pools/0}).
-eqwalizer({nowarn_function, pool_config/1}).
-eqwalizer({nowarn_function, any_pg_repo/0}).

start(_StartType, _StartArgs) ->
    resolve_backends(),
    maybe_configure_pg_types(),
    ok = ensure_pools(),
    kura_sup:start_link().

stop(_State) ->
    ok.

%% Resolve backend aggregator(s) into concrete `dialect` / `pool_module`
%% / `driver_module` keys. Two paths:
%%
%% 1. Multi-repo: `{kura, [{repos, #{Name => Cfg}}]}`. Resolve per-repo
%%    inside the map and persist back.
%% 2. Single-repo legacy: `{kura, [{backend, BackendMod}, ...]}`.
%%    Populate kura's flat env with dialect/pool_module/driver_module.
-spec resolve_backends() -> ok.
resolve_backends() ->
    case application:get_env(kura, repos) of
        {ok, Map} when is_map(Map) ->
            Resolved = maps:map(fun(_R, Cfg) -> resolve_backend(Cfg) end, Map),
            application:set_env(kura, repos, Resolved),
            ok;
        _ ->
            case application:get_env(kura, backend) of
                {ok, Backend} when is_atom(Backend) ->
                    ensure_set_env(dialect, fun() -> Backend:dialect() end),
                    ensure_set_env(pool_module, fun() -> Backend:pool_module() end),
                    ensure_set_env(driver_module, fun() -> Backend:driver_module() end),
                    ok;
                _ ->
                    ok
            end
    end.

-spec resolve_backend(map()) -> map().
resolve_backend(Cfg = #{backend := Backend}) when is_atom(Backend) ->
    Cfg#{
        dialect => maps:get(dialect, Cfg, Backend:dialect()),
        pool_module => maps:get(pool_module, Cfg, Backend:pool_module()),
        driver_module => maps:get(driver_module, Cfg, Backend:driver_module())
    };
resolve_backend(Cfg) ->
    Cfg.

-spec ensure_set_env(atom(), fun(() -> term())) -> ok.
ensure_set_env(Key, Resolve) ->
    case application:get_env(kura, Key) of
        {ok, _} -> ok;
        _ -> application:set_env(kura, Key, Resolve())
    end.

-spec ensure_pools() -> ok.
ensure_pools() ->
    case application:get_env(kura, repos) of
        {ok, Map} when is_map(Map) ->
            maps:foreach(fun(R, _Cfg) -> start_repo_pool(R) end, Map);
        _ ->
            case application:get_env(kura, repo) of
                {ok, Repo} when is_atom(Repo) -> start_repo_pool(Repo);
                _ -> ok
            end
    end.

-spec start_repo_pool(module()) -> ok.
start_repo_pool(Repo) ->
    Config = pool_config(Repo),
    PoolMod = kura_db:get_pool_module(Repo),
    case PoolMod:start_pool(Repo, Config) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, _} -> ok
    end.

%% Per-repo pool config. Multi-repo: read from the repos map. Single-repo
%% legacy: pull the flat kura env keys.
-spec pool_config(module()) -> map().
pool_config(RepoMod) ->
    case application:get_env(kura, repos) of
        {ok, Map} when is_map(Map), is_map_key(RepoMod, Map) ->
            maps:get(RepoMod, Map);
        _ ->
            pool_config()
    end.

%% Legacy single-repo pool config.
-spec pool_config() -> map().
pool_config() ->
    case application:get_env(kura, dialect) of
        {ok, kura_dialect_pg} -> pg_pool_config();
        _ -> generic_pool_config()
    end.

%% Pass-through map of user-set kura env keys, minus bookkeeping.
%% Backends pick the keys they understand from this map.
-spec generic_pool_config() -> map().
generic_pool_config() ->
    Excluded = [
        repo,
        repos,
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
    #{K => V || {K, V} <- All, not lists:member(K, Excluded)}.

%% Postgres-shaped defaults (port 5432, user/database "postgres", etc.).
%% Kept stable for back-compat with pre-2.4 single-repo configs.
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

%% Configure pg_types defaults if any configured dialect is kura_dialect_pg.
-spec maybe_configure_pg_types() -> ok.
maybe_configure_pg_types() ->
    case any_pg_repo() of
        true -> configure_pg_types();
        false -> ok
    end.

-spec any_pg_repo() -> boolean().
any_pg_repo() ->
    case application:get_env(kura, repos) of
        {ok, Map} when is_map(Map) ->
            lists:any(
                fun(Cfg) -> maps:get(dialect, Cfg, undefined) =:= kura_dialect_pg end,
                maps:values(Map)
            );
        _ ->
            application:get_env(kura, dialect) =:= {ok, kura_dialect_pg}
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
