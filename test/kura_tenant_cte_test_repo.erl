-module(kura_tenant_cte_test_repo).
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura.
