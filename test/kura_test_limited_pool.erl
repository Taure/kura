-module(kura_test_limited_pool).
-moduledoc "Declares a backend with none of the capabilities a query can need.".
-behaviour(kura_capabilities).

-export([capabilities/0]).

capabilities() -> [returning, transactions].
