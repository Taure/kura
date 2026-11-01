-module(m20250304100000_nontx_raise).
-behaviour(kura_migration).

-export([up/0, down/0, transactional/0]).

transactional() -> false.

up() -> error(nontx_test_boom).

down() -> [].
