-module(kura_sup).
-moduledoc false.
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [
        #{
            id => kura_query_cache,
            start => {kura_query_cache, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [kura_query_cache]
        }
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
