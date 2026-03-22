-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    configure_pg_types(),
    kura_query_cache:init(),
    kura_sup:start_link().

stop(_State) ->
    ok.

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
