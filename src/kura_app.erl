-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1, pool_config/0]).

start(_StartType, _StartArgs) ->
    configure_pg_types(),
    kura_query_cache:init(),
    ok = ensure_pool(),
    kura_sup:start_link().

stop(_State) ->
    ok.

-spec ensure_pool() -> ok.
ensure_pool() ->
    case application:get_env(kura, repo) of
        {ok, Repo} ->
            Config = pool_config(),
            case pgo_sup:start_child(Repo, Config) of
                {ok, _Pid} -> ok;
                {error, {already_started, _Pid}} -> ok
            end;
        undefined ->
            ok
    end.

-spec pool_config() -> map().
pool_config() ->
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
