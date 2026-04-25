-module(kura_migrator_telemetry_test_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura_migrator_telemetry_test_app.
