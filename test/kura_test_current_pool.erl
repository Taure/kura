-module(kura_test_current_pool).
-moduledoc """
Declares a backend written against the current capability model, with none
of the capabilities a query or a stream can need.
""".
-behaviour(kura_capabilities).

-export([capabilities/0, capability_model/0]).

capabilities() -> [returning, transactions].

capability_model() -> 2.
