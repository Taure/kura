-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1, pool_config/0]).

start(_StartType, _StartArgs) ->
    kura_query_cache:init(),
    ok = ensure_pool(),
    kura_sup:start_link().

stop(_State) ->
    ok.

-spec ensure_pool() -> ok.
ensure_pool() ->
    case application:get_env(kura, repo) of
        {ok, Repo} when is_atom(Repo) ->
            Config = pool_config(),
            case kura_pool_hnc:start_pool(Repo, Config) of
                {ok, _Pid} -> ok;
                {error, {already_started, _Pid}} -> ok
            end;
        _ ->
            ok
    end.

-spec pool_config() -> map().
pool_config() ->
    Env = fun(Key, Default) -> application:get_env(kura, Key, Default) end,
    Connection0 = #{
        host => Env(host, "localhost"),
        port => Env(port, 5432),
        database => Env(database, "postgres"),
        username => Env(user, "postgres"),
        password => Env(password, "")
    },
    Connection1 =
        case Env(socket_options, []) of
            Opts when is_list(Opts), Opts =/= [] -> Connection0#{tcp_opts => Opts};
            _ -> Connection0
        end,
    Connection2 =
        case Env(ssl, false) of
            true -> Connection1#{ssl => true};
            _ -> Connection1
        end,
    Connection =
        case Env(ssl_options, []) of
            SSLOpts when is_list(SSLOpts), SSLOpts =/= [] -> Connection2#{ssl_opts => SSLOpts};
            _ -> Connection2
        end,
    PoolSize = Env(pool_size, 10),
    #{
        connection => Connection,
        pool_opts => #{size => {PoolSize, PoolSize}}
    }.
