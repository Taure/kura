-module(kura_boot_warning_tests).

-include_lib("eunit/include/eunit.hrl").

-export([log/2]).

-define(KEY, kura_boot_warning_tests_key).
-define(OTHER, kura_boot_warning_tests_other_key).

once_test_() ->
    [
        {"the first warning for a key is logged, at warning level", fun() ->
            Logged = logged_by(fun() ->
                ?assertEqual(ok, kura_boot_warning:once(?KEY, #{msg => ~"first"}))
            end),
            ?assertEqual([{warning, #{msg => ~"first"}}], Logged)
        end},
        {"a second warning for the key is not", fun() ->
            Logged = logged_by(fun() ->
                ok = kura_boot_warning:once(?KEY, #{msg => ~"first"}),
                ?assertEqual(ok, kura_boot_warning:once(?KEY, #{msg => ~"second"}))
            end),
            ?assertEqual([{warning, #{msg => ~"first"}}], Logged)
        end},
        {"each key has its own", fun() ->
            Logged = logged_by(fun() ->
                ok = kura_boot_warning:once(?KEY, #{msg => ~"one"}),
                ok = kura_boot_warning:once(?OTHER, #{msg => ~"other"}),
                ok = kura_boot_warning:once(?OTHER, #{msg => ~"other again"})
            end),
            ?assertEqual([{warning, #{msg => ~"one"}}, {warning, #{msg => ~"other"}}], Logged)
        end},
        {"a forgotten key is logged again, and only that key", fun() ->
            Logged = logged_by(fun() ->
                ok = kura_boot_warning:once(?KEY, #{msg => ~"one"}),
                ok = kura_boot_warning:once(?OTHER, #{msg => ~"other"}),
                ?assertEqual(ok, kura_boot_warning:forget(?KEY)),
                ok = kura_boot_warning:once(?KEY, #{msg => ~"again"}),
                ok = kura_boot_warning:once(?OTHER, #{msg => ~"other again"})
            end),
            ?assertEqual(
                [
                    {warning, #{msg => ~"one"}},
                    {warning, #{msg => ~"other"}},
                    {warning, #{msg => ~"again"}}
                ],
                Logged
            )
        end},
        {"forgetting a key that was never logged is fine", fun() ->
            Logged = logged_by(fun() -> ?assertEqual(ok, kura_boot_warning:forget(?KEY)) end),
            ?assertEqual([], Logged)
        end}
    ].

logged_by(Run) ->
    forget(),
    _ = logger:remove_handler(?MODULE),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        _ = Run(),
        logged()
    after
        logger:remove_handler(?MODULE),
        forget()
    end.

forget() ->
    ok = kura_boot_warning:forget(?KEY),
    ok = kura_boot_warning:forget(?OTHER).

logged() ->
    receive
        {logged, Level, Report} -> [{Level, Report} | logged()]
    after 100 -> []
    end.

log(#{level := Level, msg := {report, #{msg := _} = Report}}, #{config := Pid}) when
    map_size(Report) =:= 1
->
    Pid ! {logged, Level, Report},
    ok;
log(_Event, _Config) ->
    ok.
