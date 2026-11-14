-module(kura_test_misdated_pool).
-moduledoc "Declares a capability model this kura does not have.".
-behaviour(kura_capabilities).

-export([capabilities/0, capability_model/0]).

capabilities() -> [returning, transactions].

capability_model() -> 2.
