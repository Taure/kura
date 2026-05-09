-module(kura_pg_conn).
-moduledoc """
PostgreSQL connection worker. Wraps a single `epgsql` connection and is intended
to be spawned per pool slot by a `kura_pool` implementation.

Each worker:

* Holds one epgsql connection (monitored).
* Periodically pings the connection with `SELECT 1` to detect server-side closures.
* Stops when the underlying connection dies, signalling the pool to replace it.

Future commits will add a per-connection prepared-statement cache and
`LISTEN/NOTIFY` wiring on top of this skeleton.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([
    start_link/1,
    get_conn/1,
    ping/1,
    close/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([config/0]).

-define(DEFAULT_PING_INTERVAL, 15000).

-type config() :: #{
    host => string() | binary(),
    port => inet:port_number(),
    username => string() | binary(),
    password => string() | binary(),
    database => string() | binary(),
    ssl => boolean(),
    timeout => timeout(),
    ping_interval => pos_integer(),
    atom() => term()
}.

-record(state, {
    conn :: pid() | undefined,
    monitor :: reference() | undefined,
    ping_interval :: pos_integer(),
    ping_timer :: reference() | undefined
}).

-doc """
Start a connection worker linked to the caller. `Config` is passed through to
`epgsql:connect/1` after stripping kura-specific keys (`ping_interval`).
""".
-spec start_link(config()) -> gen_server:start_ret().
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-doc """
Return the underlying epgsql connection pid for direct use by adapter code.
""".
-spec get_conn(pid()) -> {ok, pid()} | {error, no_connection}.
get_conn(Pid) ->
    case gen_server:call(Pid, get_conn) of
        {ok, Conn} when is_pid(Conn) -> {ok, Conn};
        {error, no_connection} -> {error, no_connection}
    end.

-doc """
Run `SELECT 1` on the connection. Useful for health probes outside the
worker's automatic ping schedule.
""".
-spec ping(pid()) -> ok | {error, term()}.
ping(Pid) ->
    case gen_server:call(Pid, ping) of
        ok -> ok;
        {error, _} = Err -> Err
    end.

-doc """
Stop the worker and close the underlying connection.
""".
-spec close(pid()) -> ok.
close(Pid) ->
    gen_server:stop(Pid).

init(Config) when is_map(Config) ->
    case epgsql:connect(epgsql_config(Config)) of
        {ok, Conn} ->
            MRef = erlang:monitor(process, Conn),
            Interval = maps:get(ping_interval, Config, ?DEFAULT_PING_INTERVAL),
            State = #state{
                conn = Conn,
                monitor = MRef,
                ping_interval = Interval,
                ping_timer = schedule_ping(Interval)
            },
            {ok, State};
        {error, Reason} ->
            ?LOG_ERROR(#{event => kura_pg_conn_connect_failed, reason => Reason}),
            {stop, {connect_failed, Reason}}
    end.

handle_call(get_conn, _From, #state{conn = Conn} = S) when is_pid(Conn) ->
    {reply, {ok, Conn}, S};
handle_call(get_conn, _From, S) ->
    {reply, {error, no_connection}, S};
handle_call(ping, _From, #state{conn = Conn} = S) when is_pid(Conn) ->
    {reply, run_ping(Conn), S};
handle_call(ping, _From, S) ->
    {reply, {error, no_connection}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(ping_timer, #state{conn = Conn, ping_interval = Interval} = S) when is_pid(Conn) ->
    case run_ping(Conn) of
        ok ->
            {noreply, S#state{ping_timer = schedule_ping(Interval)}};
        {error, Reason} ->
            ?LOG_WARNING(#{event => kura_pg_conn_ping_failed, reason => Reason}),
            {stop, {ping_failed, Reason}, S}
    end;
handle_info({'DOWN', MRef, process, _Pid, Reason}, #state{monitor = MRef} = S) ->
    ?LOG_WARNING(#{event => kura_pg_conn_died, reason => Reason}),
    {stop, {connection_lost, Reason}, S#state{conn = undefined}};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #state{conn = Conn, ping_timer = Timer}) ->
    cancel_timer(Timer),
    close_conn(Conn),
    ok.

epgsql_config(Config) ->
    Defaults = #{host => "localhost", port => 5432, timeout => 5000},
    maps:merge(Defaults, maps:without([ping_interval], Config)).

run_ping(Conn) ->
    case epgsql:equery(Conn, ~"SELECT 1", []) of
        {ok, _Cols, _Rows} -> ok;
        {error, _} = Err -> Err
    end.

schedule_ping(Interval) ->
    erlang:send_after(Interval, self(), ping_timer).

cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    _ = erlang:cancel_timer(Ref),
    ok.

close_conn(undefined) ->
    ok;
close_conn(Conn) when is_pid(Conn) ->
    try
        epgsql:close(Conn)
    catch
        _:_ -> ok
    end,
    ok.
