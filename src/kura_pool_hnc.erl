-module(kura_pool_hnc).
-moduledoc """
Default `kura_pool` implementation backed by [hnc](https://github.com/hnc-agency/hnc).

Each pool slot runs a `kura_pg_conn` worker holding one epgsql connection.

## Pool options

```erlang
#{
    connection => #{                  %% passed verbatim to kura_pg_conn:start_link/1
        host => "localhost",
        port => 5432,
        username => "postgres",
        password => "",
        database => "my_app",
        ping_interval => 15000
    },
    pool_opts => #{                   %% passed to hnc:start_pool/4
        size => {5, 10},              %% {min, max}
        strategy => fifo
    }
}
```

See `hnc` docs for the full pool option set.
""".

-behaviour(kura_pool).

-export([
    start_pool/2,
    stop_pool/1,
    checkout/2,
    checkin/2,
    give_away/3,
    resize/2
]).

-spec start_pool(kura_pool:name(), kura_pool:pool_opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts) ->
    PoolOpts = maps:get(pool_opts, Opts, #{}),
    WorkerCfg = maps:get(connection, Opts, #{}),
    try
        hnc:start_pool(Name, PoolOpts, kura_pg_conn, WorkerCfg)
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

-spec stop_pool(kura_pool:name()) -> ok.
stop_pool(Name) ->
    hnc:stop_pool(Name).

-spec checkout(kura_pool:name(), kura_pool:checkout_opts()) ->
    {ok, pid(), kura_pool:token()} | {error, term()}.
checkout(Name, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    try
        WorkerRef = hnc:checkout(Name, Timeout),
        Worker = hnc:get_worker(WorkerRef),
        {ok, Worker, WorkerRef}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

-spec checkin(kura_pool:name(), kura_pool:token()) -> ok.
checkin(_Name, WorkerRef) ->
    _ = hnc:checkin(WorkerRef),
    ok.

-spec give_away(kura_pool:token(), pid(), term()) -> ok | {error, term()}.
give_away(WorkerRef, NewOwner, GiftData) ->
    hnc:give_away(WorkerRef, NewOwner, GiftData).

-spec resize(kura_pool:name(), pos_integer()) -> ok | {error, term()}.
resize(Name, Size) when is_integer(Size), Size > 0 ->
    try
        hnc:set_size(Name, {Size, Size}),
        ok
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.
