-module(m20250304100000_nontx_raise).
-behaviour(kura_migration).

-export([up/0, down/0, transaction/0]).

transaction() -> false.

up() -> error(nontx_test_boom).

down() -> [].
