-module(kura_coverage_noinit_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura.
