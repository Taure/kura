-module(kura_nontx_repo).
-moduledoc """
Repo for the non-transactional migration tests. `migration_apps/0` reads
the `kura` app env so each test picks the migration set it runs.
""".
-behaviour(kura_repo).

-export([otp_app/0, migration_apps/0]).

otp_app() -> kura_nontx_base.

migration_apps() -> application:get_env(kura, kura_nontx_apps, []).
