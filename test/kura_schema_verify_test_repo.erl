-module(kura_schema_verify_test_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura_schema_verify_test_app.
