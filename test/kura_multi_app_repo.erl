-module(kura_multi_app_repo).
-moduledoc """
Repo declaring extra migration applications, for multi-app discovery tests.

`migration_apps/0` reads the `kura` app env so a single module can stand
in for every declaration a test needs, including deliberately invalid
ones.
""".
-behaviour(kura_repo).

-export([otp_app/0, migration_apps/0]).

otp_app() -> kura_ma_core.

migration_apps() -> application:get_env(kura, kura_ma_declared_apps, []).
