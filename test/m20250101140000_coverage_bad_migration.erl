-module(m20250101140000_coverage_bad_migration).
-behaviour(kura_migration).

-export([up/0, down/0]).

up() ->
    [{execute, <<"CREATE TABLE this_will_fail_coverage (">>}].

down() ->
    [{execute, <<"DROP TABLE IF EXISTS this_will_fail_coverage">>}].
