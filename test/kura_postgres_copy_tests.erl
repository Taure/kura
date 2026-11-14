-module(kura_postgres_copy_tests).
-moduledoc """
`kura_driver_minato` and `kura_pool_minato` in this directory are the
modules of a released kura_postgres. kura cannot take kura_postgres as a
test dependency: rebar3 compiles a dependency before the project, so the
backend would be built before the kura behaviours it implements exist.

These tests fail when a copy is no longer the release named here. To move
to a newer release, copy its two modules over these, reapply the changes
listed in `local_changes/1` if they still apply, and update `?RELEASE` and
the hashes.
""".

-include_lib("eunit/include/eunit.hrl").

-define(RELEASE, "1.1.7").

released() ->
    [
        {"kura_driver_minato.erl",
            ~"d7385d1053778091127203c3738eff44e93357a47a303ce1174dee5abe89db49"},
        {"kura_pool_minato.erl",
            ~"07dfe30e2d1c58722929e4d91146afc9d43a753a750c91ba8ea3776486b15f1f"}
    ].

%% {What the copy says, what the release says}: two map lookups written
%% without the result tuple ELP reports.
local_changes("kura_driver_minato.erl") ->
    [
        {
            ~"""
                case Config of
                    #{database := Database} -> ensured(Config, binary(Database));
                    #{} -> ok
                end.
            """,
            ~"""
                case maps:find(database, Config) of
                    error -> ok;
                    {ok, Database} -> ensured(Config, binary(Database))
                end.
            """
        },
        {
            ~"""
                case Config of
                    #{password := Password} -> Base#{password => binary(Password)};
                    #{} -> Base
                end.
            """,
            ~"""
                case maps:find(password, Config) of
                    {ok, Password} -> Base#{password => binary(Password)};
                    error -> Base
                end.
            """
        }
    ];
local_changes(_File) ->
    [].

copies_are_the_released_modules_test_() ->
    [
        {File ++ " is kura_postgres " ++ ?RELEASE, fun() ->
            {ok, Copy} = file:read_file(filename:join(filename:dirname(?FILE), File)),
            Released = without_local_changes(Copy, local_changes(File)),
            ?assertEqual(Hash, binary:encode_hex(crypto:hash(sha256, Released), lowercase))
        end}
     || {File, Hash} <- released()
    ].

without_local_changes(Source, []) ->
    Source;
without_local_changes(Source, [{Ours, Theirs} | Rest]) ->
    ?assertMatch([_, _], binary:split(Source, Ours, [global])),
    without_local_changes(binary:replace(Source, Ours, Theirs), Rest).
