-module(kura_coverage_init_repo).
-behaviour(kura_repo).

-export([otp_app/0, init/1]).

otp_app() -> kura.

init(Config) ->
    Config#{extra => true}.
