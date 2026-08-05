-module(kura_denied_app_repo).
-moduledoc """
Repo on a pool whose role may not create tables.

Exists so `ensure_schema_migrations/1` can be tested against a database
that refuses the `CREATE TABLE`, which is the case whose result the
migrator used to discard.
""".
-behaviour(kura_repo).

-export([otp_app/0]).

otp_app() -> kura_ma_core.
