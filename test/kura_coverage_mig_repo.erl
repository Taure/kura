-module(kura_coverage_mig_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura_coverage_mig_app.
