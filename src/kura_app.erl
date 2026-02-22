-module(kura_app).
-moduledoc false.
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    kura_sup:start_link().

stop(_State) ->
    ok.
