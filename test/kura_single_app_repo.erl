-module(kura_single_app_repo).
-moduledoc """
Repo without `migration_apps/0` - the shape every consumer ships today.

Exists so the tests can prove multi-app discovery leaves the
single-application path untouched.
""".
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura_ma_core.
