-module(kura_db_counting_init_repo).
-behaviour(kura_repo).

-export([otp_app/0, init/1, calls/0, reset/0]).

otp_app() -> kura.

init(Config) ->
    Calls = calls() + 1,
    put({?MODULE, calls}, Calls),
    case Calls of
        1 -> Config;
        _ -> error({init_called_again, Calls})
    end.

calls() ->
    case get({?MODULE, calls}) of
        undefined -> 0;
        Calls -> Calls
    end.

reset() ->
    _ = erase({?MODULE, calls}),
    ok.
