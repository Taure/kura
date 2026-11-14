-module(kura_test_limited_pool).
-moduledoc """
Declares a list of capabilities and no capability model, as a backend
released before kura checked a query does. Nothing is checked against it.
""".
-behaviour(kura_capabilities).

-export([capabilities/0]).

capabilities() -> [returning, transactions].
