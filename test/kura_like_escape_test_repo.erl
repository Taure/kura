-module(kura_like_escape_test_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura.
