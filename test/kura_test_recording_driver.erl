-module(kura_test_recording_driver).
-moduledoc """
A driver that runs nothing: it sends each statement to the calling process
and answers with an empty result of the statement's kind, or with the next
result queued with `reply_with/1`. Lets a test read the SQL and parameters
kura_repo_worker builds without a database.
""".

-behaviour(kura_driver).

-export([query/5, query_on/4, transaction/4]).
-export([configure/1, configure/2, unconfigure/0, statements/0, reply_with/1]).

-define(REPLIES, {?MODULE, replies}).

-spec configure(module()) -> ok.
configure(Repo) ->
    configure(Repo, #{}).

-spec configure(module(), map()) -> ok.
configure(Repo, Extra) ->
    Config = #{
        dialect => kura_dialect_pg,
        pool => kura_test_recording_pool,
        pool_module => kura_pool_minato,
        driver_module => ?MODULE
    },
    application:set_env(kura, repos, #{Repo => maps:merge(Config, Extra)}).

-spec unconfigure() -> ok.
unconfigure() ->
    _ = erase(?REPLIES),
    _ = statements(),
    application:unset_env(kura, repos).

-spec reply_with([term()]) -> ok.
reply_with(Results) ->
    _ = put(?REPLIES, Results),
    ok.

-spec statements() -> [{binary(), [term()]}].
statements() ->
    receive
        {?MODULE, SQL, Params} -> [{SQL, Params} | statements()]
    after 0 -> []
    end.

query(_PoolMod, _Pool, SQL, Params, _Opts) ->
    recorded(SQL, Params).

query_on(_Conn, SQL, Params, _Opts) ->
    recorded(SQL, Params).

transaction(_PoolMod, _Pool, Fun, _Opts) ->
    Fun().

recorded(SQL, Params) ->
    Text = iolist_to_binary(SQL),
    self() ! {?MODULE, Text, Params},
    case get(?REPLIES) of
        [Result | Rest] ->
            _ = put(?REPLIES, Rest),
            Result;
        _ ->
            empty_result(Text)
    end.

empty_result(<<"INSERT", _/binary>>) -> #{command => insert, rows => [], num_rows => 0};
empty_result(<<"UPDATE", _/binary>>) -> #{command => update, rows => [], num_rows => 0};
empty_result(<<"DELETE", _/binary>>) -> #{command => delete, rows => [], num_rows => 0};
empty_result(_Select) -> #{command => select, rows => [], num_rows => 0}.
