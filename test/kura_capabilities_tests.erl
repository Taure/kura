-module(kura_capabilities_tests).
-include_lib("eunit/include/eunit.hrl").

%% Fake backends used by the tests below.

-export([capabilities/0]).

%%----------------------------------------------------------------------
%% kura_capabilities:supported/1
%%----------------------------------------------------------------------

supported_returns_declared_set_test() ->
    ?assertEqual([returning, jsonb, listen_notify], kura_capabilities:supported(?MODULE)).

supported_returns_empty_for_module_without_callback_test() ->
    %% lists doesn't export capabilities/0 - should return [], not crash.
    ?assertEqual([], kura_capabilities:supported(lists)).

%%----------------------------------------------------------------------
%% kura_capabilities:has/2
%%----------------------------------------------------------------------

has_true_when_declared_test() ->
    ?assert(kura_capabilities:has(?MODULE, returning)).

has_false_when_not_declared_test() ->
    ?assertNot(kura_capabilities:has(?MODULE, advisory_locks)).

has_false_for_module_without_callback_test() ->
    ?assertNot(kura_capabilities:has(lists, returning)).

%%----------------------------------------------------------------------
%% kura_capabilities:require/2
%%----------------------------------------------------------------------

require_ok_when_all_declared_test() ->
    ?assertEqual(
        ok,
        kura_capabilities:require(?MODULE, [returning, listen_notify])
    ).

require_ok_for_empty_required_test() ->
    ?assertEqual(ok, kura_capabilities:require(?MODULE, [])).

require_ok_for_empty_required_on_silent_module_test() ->
    %% A backend with no capabilities() callback still satisfies an empty require.
    ?assertEqual(ok, kura_capabilities:require(lists, [])).

require_reports_missing_test() ->
    ?assertEqual(
        {error, {missing_capabilities, [advisory_locks, partial_indexes]}},
        kura_capabilities:require(?MODULE, [returning, advisory_locks, partial_indexes])
    ).

require_preserves_missing_order_test() ->
    %% Missing list should preserve the order the consumer asked in, so the
    %% error message matches the consumer's mental model.
    ?assertEqual(
        {error, {missing_capabilities, [partial_indexes, advisory_locks]}},
        kura_capabilities:require(?MODULE, [partial_indexes, returning, advisory_locks])
    ).

require_reports_all_missing_for_silent_module_test() ->
    ?assertEqual(
        {error, {missing_capabilities, [returning, jsonb]}},
        kura_capabilities:require(lists, [returning, jsonb])
    ).

%%----------------------------------------------------------------------
%% Fake backend (this module) — declares a small capability set
%%----------------------------------------------------------------------

capabilities() ->
    [returning, jsonb, listen_notify].
