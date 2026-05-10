-module(kura_pool_pgo).
-moduledoc """
Default `kura_pool` implementation. Wraps [pgo](https://hex.pm/packages/pgo)'s
pool, which uses an ETS-holder transfer pattern that keeps the per-query hot
path off the pool agent.

The connection handle returned from `checkout/2` is pgo's own `conn()` record
and can be passed straight to `pgo_handler:extended_query/4` etc.

The `give_away/3` callback transfers ownership of pgo's per-conn holder ETS
table to another process via `ets:give_away/3`. The new owner can then call
`kura_pool_pgo:checkin/2` legitimately. Used by `kura_sandbox`-style test
fixtures.

Pool options are passed through to pgo verbatim. Common keys:

```erlang
#{
    host => "localhost",
    port => 5432,
    database => "my_app",
    user => "postgres",
    password => "secret",
    pool_size => 10,
    socket_options => [],
    decode_opts => [return_rows_as_maps, column_name_as_atom]
}
```

See `pgo:pool_config/0` for the full list.
""".

-behaviour(kura_pool).
-behaviour(kura_capabilities).

-export([
    start_pool/2,
    stop_pool/1,
    checkout/2,
    checkin/2,
    give_away/3,
    capabilities/0
]).

-spec capabilities() -> kura_capabilities:capability_set().
capabilities() ->
    [
        returning,
        jsonb,
        arrays,
        advisory_locks,
        listen_notify,
        select_for_update_skip_locked,
        partial_indexes,
        transactions,
        savepoints,
        prepared_statements
    ].

-spec start_pool(kura_pool:name(), kura_pool:opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts) ->
    case pgo_sup:start_child(Name, Opts) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, _} = Err -> Err
    end.

-spec stop_pool(kura_pool:name()) -> ok.
stop_pool(Name) ->
    case erlang:whereis(Name) of
        undefined ->
            ok;
        Pid ->
            _ = supervisor:terminate_child(pgo_sup, Pid),
            ok
    end.

-spec checkout(kura_pool:name(), kura_pool:checkout_opts()) ->
    {ok, kura_pool:conn(), kura_pool:token()} | {error, term()}.
checkout(Name, Opts) ->
    PoolOpts = checkout_opts_to_list(Opts),
    case pgo_pool:checkout(Name, PoolOpts) of
        {ok, Ref, Conn} -> {ok, Conn, {Ref, Conn}};
        {error, _} = Err -> Err
    end.

-spec checkin(kura_pool:name(), kura_pool:token()) -> ok.
checkin(_Name, {Ref, Conn}) ->
    pgo_pool:checkin(Ref, Conn, []),
    ok.

-spec give_away(kura_pool:token(), pid(), term()) -> ok | {error, term()}.
give_away({Ref, _Conn}, NewOwner, GiftData) when is_pid(NewOwner) ->
    %% pgo's pool_ref is `{Pool, Ref0, Deadline, Holder}` — the per-conn
    %% holder ETS that backs ets:give_away. Transferring it lets `NewOwner`
    %% legitimately call kura_pool_pgo:checkin/2 with the same token later.
    case Ref of
        {_Pool, _Ref0, _Deadline, Holder} when Holder =/= undefined ->
            try
                true = ets:give_away(Holder, NewOwner, GiftData),
                ok
            catch
                error:badarg -> {error, badarg}
            end;
        _ ->
            {error, no_holder}
    end.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

checkout_opts_to_list(Opts) when is_map(Opts) ->
    maps:to_list(Opts);
checkout_opts_to_list(Opts) when is_list(Opts) ->
    Opts.
