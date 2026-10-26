-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1]).

-ifdef(TEST).
-export([start_repo_pool/1]).
-endif.

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
                    ensure_set_env(dialect, fun Backend:dialect/0),
                    ensure_set_env(pool_module, fun Backend:pool_module/0),
                    ensure_set_env(driver_module, fun Backend:driver_module/0),
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
    case kura_db:start_pool(Repo) of
        ok -> ok;
        {error, _Logged} -> ok
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
