-module(kura_settings_tests).
-moduledoc "kura_settings: the kura application environment's settings, read and checked in one place.".

-include_lib("eunit/include/eunit.hrl").

-export([log/2]).

-define(FLAGS, [require_tenant, strict_assoc_cast, strict_locks, telemetry_error_detail]).
-define(INVALID, [
    {require_tenant, yes},
    {strict_assoc_cast, 1},
    {strict_locks, ~"true"},
    {telemetry_error_detail, on},
    {max_page_size, 0},
    {max_page_size, ~"100"},
    {encryption_write_format, v3}
]).

with_setting(Key, Value, Run) ->
    application:set_env(kura, Key, Value),
    try
        Run()
    after
        application:unset_env(kura, Key)
    end.

flag_test_() ->
    [
        [
            ?_assertEqual(unset, kura_settings:flag(Flag)),
            ?_assertEqual(true, with_setting(Flag, true, fun() -> kura_settings:flag(Flag) end)),
            ?_assertEqual(false, with_setting(Flag, false, fun() -> kura_settings:flag(Flag) end)),
            ?_assertError(
                {kura, {invalid_setting, Flag, 1}},
                with_setting(Flag, 1, fun() -> kura_settings:flag(Flag) end)
            )
        ]
     || Flag <- ?FLAGS
    ].

checked_flag_test_() ->
    [
        ?_assertEqual(true, kura_settings:checked_flag(require_tenant, true)),
        ?_assertError(
            {kura, {invalid_setting, require_tenant, ~"no"}},
            kura_settings:checked_flag(require_tenant, ~"no")
        )
    ].

max_page_size_test_() ->
    Read = fun kura_settings:max_page_size/0,
    [
        ?_assertEqual(unset, Read()),
        ?_assertEqual(50, with_setting(max_page_size, 50, Read)),
        ?_assertEqual(infinity, with_setting(max_page_size, infinity, Read))
        | [
            ?_assertError(
                {kura, {invalid_setting, max_page_size, Max}},
                with_setting(max_page_size, Max, Read)
            )
         || Max <- [0, -1, 1.5, ~"100"]
        ]
    ].

read_test_() ->
    Read = fun() -> kura_settings:read(encryption_write_format) end,
    [
        ?_assertEqual(unset, Read()),
        ?_assertEqual({ok, v2}, with_setting(encryption_write_format, v2, Read)),
        ?_assertEqual({invalid, v3}, with_setting(encryption_write_format, v3, Read))
    ].

validate_test_() ->
    [
        ?_assertEqual(ok, kura_settings:validate()),
        {"every valid value passes", fun() ->
            Valid =
                [{Flag, true} || Flag <- ?FLAGS] ++
                    [{max_page_size, infinity}, {encryption_write_format, v1}],
            [application:set_env(kura, Key, Value) || {Key, Value} <- Valid],
            try
                ?assertEqual(ok, kura_settings:validate())
            after
                [application:unset_env(kura, Key) || {Key, _} <- Valid]
            end
        end}
        | [
            {
                lists:flatten(io_lib:format("~p = ~p is refused and logged by name", [Key, Value])),
                fun() ->
                    {Result, Logged} = logged(fun() ->
                        with_setting(Key, Value, fun kura_settings:validate/0)
                    end),
                    ?assertEqual({error, {invalid_setting, Key, Value}}, Result),
                    ?assertMatch([#{setting := Key, value := Value, expected := _}], Logged)
                end
            }
         || {Key, Value} <- ?INVALID
        ]
    ].

boot_test_() ->
    [
        {lists:flatten(io_lib:format("kura does not start with ~p = ~p", [Key, Value])), fun() ->
            {Result, _Logged} = logged(fun() ->
                with_setting(Key, Value, fun() ->
                    try kura_app:start(normal, []) of
                        Started -> {started, Started}
                    catch
                        error:Reason -> Reason
                    end
                end)
            end),
            ?assertEqual(boot_reason(Key, Value), Result)
        end}
     || {Key, Value} <- ?INVALID
    ].

boot_reason(encryption_write_format, _Value) -> {kura_crypto, invalid_write_format};
boot_reason(Key, Value) -> {kura, {invalid_setting, Key, Value}}.

logged(Run) ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => error}),
    try
        Result = Run(),
        {Result, reports()}
    after
        logger:remove_handler(?MODULE)
    end.

reports() ->
    receive
        {logged, #{setting := _} = Report} -> [Report | reports()];
        {logged, _} -> reports()
    after 100 -> []
    end.

log(#{msg := {report, Report}}, #{config := Pid}) ->
    Pid ! {logged, Report},
    ok;
log(_Event, _Config) ->
    ok.
