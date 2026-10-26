-module(kura_db_raising_init_repo).
-behaviour(kura_repo).

-export([otp_app/0, init/1]).

otp_app() -> kura.

init(#{raise := function_clause} = Config) ->
    unmatched(Config);
init(#{password := Password} = Config) ->
    error({bad_config, Password, Config}).

unmatched(#{never := _}) ->
    #{}.
