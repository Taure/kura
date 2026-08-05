-module(kura_pool_minato).
-moduledoc """
`kura_pool` implementation over [minato](https://hex.pm/packages/minato).

minato hands a connection to the process that borrows it: the socket belongs to
that process, and a query returns the connection to carry on from rather than
mutating one in place. `kura_pool` is the other shape - an opaque handle that
several processes may use and that outlives the call that made it, which is what
`kura_sandbox` needs when it checks a connection out in one process and runs the
test in another.

A checkout therefore spawns a holder: a process that borrows the connection,
owns the socket for as long as the checkout lasts, and answers `run/4`. The
handle is that process. Nothing on the ordinary query path goes through it -
`kura_driver_minato:query/5` borrows and returns a connection inside one call -
so the holder is only paid for by callers who asked for a connection to keep.

The holder watches whoever owns the checkout and gives the connection back if
that process dies, so a test that crashes returns its connection instead of
taking it out of the pool. `give_away/3` moves the watch rather than the socket,
because the socket never left the holder.
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

-export([run/4, transaction_conn/1, in_transaction/2]).

-define(CALL_TIMEOUT, 30000).

-doc "What this backend supports. Read by `kura_capabilities`.".
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
        window_functions,
        full_text_search,
        vector,
        transactions,
        savepoints,
        prepared_statements
    ].

-doc """
Start a pool.

The minato application is started if it is not already, because a repo starting
its pool has not necessarily been told that it has to start something else
first, and a pool that cannot start for that reason is a confusing way to find
out.

kura's options are translated to minato's: `pool_size` becomes `size`, and the
connection settings are gathered under `connection`. `host`, `user`, `password`
and `database` are accepted as strings or binaries, because kura configuration
in the wild is written both ways.
""".
-spec start_pool(kura_pool:name(), kura_pool:opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts) ->
    {ok, _Started} = application:ensure_all_started(minato),
    case minato:start_pool(Name, translate(Opts)) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

-doc "Stop a pool. A pool that is not running is not an error.".
-spec stop_pool(kura_pool:name()) -> ok.
stop_pool(Name) ->
    _ = minato:stop_pool(Name),
    ok.

-doc """
Borrow a connection and keep it until `checkin/2`.

The handle is the holder process, and the token is what gives it back.
""".
-spec checkout(kura_pool:name(), kura_pool:checkout_opts()) ->
    {ok, kura_pool:conn(), kura_pool:token()} | {error, term()}.
checkout(Name, Opts) ->
    Owner = self(),
    Timeout = maps:get(timeout, Opts, ?CALL_TIMEOUT),
    Holder = spawn(fun() -> hold(Name, Owner, Timeout) end),
    Monitor = erlang:monitor(process, Holder),
    receive
        {holding, Holder} ->
            true = erlang:demonitor(Monitor, [flush]),
            {ok, Holder, Holder};
        {failed, Holder, Reason} ->
            true = erlang:demonitor(Monitor, [flush]),
            {error, Reason};
        {'DOWN', Monitor, process, Holder, Reason} ->
            {error, Reason}
    after Timeout + 1000 -> {error, checkout_timeout}
    end.

-doc "Give a borrowed connection back.".
-spec checkin(kura_pool:name(), kura_pool:token()) -> ok.
checkin(_Name, Holder) when is_pid(Holder) ->
    _ = call(Holder, checkin),
    ok.

-doc """
Hand the checkout to another process.

Only the watch moves: the socket belongs to the holder and stays there, so
nothing about the connection changes. The new owner is now the one whose death
returns the connection, and it is the one that should call `checkin/2`.
""".
-spec give_away(kura_pool:token(), pid(), term()) -> ok | {error, term()}.
give_away(Holder, NewOwner, GiftData) when is_pid(Holder), is_pid(NewOwner) ->
    case call(Holder, {give_away, NewOwner}) of
        ok ->
            NewOwner ! GiftData,
            ok;
        {error, Reason} ->
            {error, Reason}
    end;
give_away(_Token, _NotAPid, _GiftData) ->
    {error, badarg}.

-doc "Run a statement on a held connection. Used by `m:kura_driver_minato`.".
-spec run(pid(), iodata(), [term()], map()) -> dynamic().
run(Holder, Sql, Params, Opts) ->
    call(Holder, {run, Sql, Params, Opts}).

-doc "The holder of the transaction this process is inside, or `undefined`.".
-spec transaction_conn(kura_pool:name()) -> pid() | undefined.
transaction_conn(Pool) ->
    erlang:get({?MODULE, Pool}).

-doc """
Note that this process is inside a transaction on `Pool`, held by `Holder`.

`undefined` clears it. Kept in the process dictionary because that is what makes
a query written inside the transaction function find the transaction's
connection without being handed one.
""".
-spec in_transaction(kura_pool:name(), pid() | undefined) -> ok.
in_transaction(Pool, undefined) ->
    _ = erlang:erase({?MODULE, Pool}),
    ok;
in_transaction(Pool, Holder) ->
    _ = erlang:put({?MODULE, Pool}, Holder),
    ok.

%%----------------------------------------------------------------------
%% The holder
%%----------------------------------------------------------------------

-spec hold(kura_pool:name(), pid(), timeout()) -> ok.
hold(Pool, Owner, Timeout) ->
    case minato_pool:checkout(Pool, Timeout) of
        {ok, Conn} ->
            Owner ! {holding, self()},
            held(Pool, Conn, erlang:monitor(process, Owner));
        {error, Reason} ->
            Owner ! {failed, self(), Reason},
            ok
    end.

-spec held(kura_pool:name(), minato_conn:conn(), reference()) -> ok.
held(Pool, Conn, Monitor) ->
    receive
        {call, From, Tag, {run, Sql, Params, Opts}} ->
            {Answer, Next} = ran(Conn, Sql, Params, Opts),
            From ! {Tag, Answer},
            held(Pool, Next, Monitor);
        {call, From, Tag, checkin} ->
            ok = minato_pool:checkin(Pool, Conn),
            true = erlang:demonitor(Monitor, [flush]),
            From ! {Tag, ok},
            ok;
        {call, From, Tag, {give_away, NewOwner}} ->
            true = erlang:demonitor(Monitor, [flush]),
            From ! {Tag, ok},
            held(Pool, Conn, erlang:monitor(process, NewOwner));
        {'DOWN', Monitor, process, _Owner, _Reason} ->
            minato_pool:checkin(Pool, Conn)
    end.

-spec ran(minato_conn:conn(), iodata(), [term()], map()) ->
    {dynamic(), minato_conn:conn()}.
ran(Conn, Sql, Params, Opts) ->
    case minato_query:query(Conn, Sql, Params, Opts) of
        {ok, Result, Next} -> {Result, Next};
        {error, Reason, Next} -> {{error, Reason}, Next};
        {error, Reason} -> {{error, Reason}, Conn}
    end.

-spec call(pid(), term()) -> dynamic().
call(Holder, Message) ->
    Tag = make_ref(),
    Monitor = erlang:monitor(process, Holder),
    Holder ! {call, self(), Tag, Message},
    receive
        {Tag, Answer} ->
            true = erlang:demonitor(Monitor, [flush]),
            Answer;
        {'DOWN', Monitor, process, Holder, Reason} ->
            {error, {holder_died, Reason}}
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%%----------------------------------------------------------------------
%% kura options to minato options
%%----------------------------------------------------------------------

-spec translate(map()) -> minato_pool:opts().
translate(Opts) ->
    Connection = maps:fold(fun connection/3, #{}, Opts),
    #{size => maps:get(pool_size, Opts, 10), connection => Connection}.

-spec connection(term(), term(), map()) -> map().
connection(host, Value, Connection) -> Connection#{host => text(Value)};
connection(hostname, Value, Connection) -> Connection#{host => text(Value)};
connection(port, Value, Connection) -> Connection#{port => Value};
connection(database, Value, Connection) -> Connection#{database => binary(Value)};
connection(user, Value, Connection) -> Connection#{user => binary(Value)};
connection(username, Value, Connection) -> Connection#{user => binary(Value)};
connection(password, Value, Connection) -> Connection#{password => password(Value)};
connection(ssl, Value, Connection) -> Connection#{ssl => Value};
connection(ssl_options, Value, Connection) -> Connection#{ssl_options => Value};
connection(socket_options, Value, Connection) -> Connection#{socket_options => Value};
connection(queue_target, _Value, Connection) -> Connection;
connection(queue_interval, _Value, Connection) -> Connection;
connection(connect_timeout, Value, Connection) -> Connection#{connect_timeout => Value};
connection(timeout, Value, Connection) -> Connection#{timeout => Value};
connection(_Key, _Value, Connection) -> Connection.

-spec password(term()) -> binary() | fun(() -> binary()).
password(Value) when is_function(Value, 0) -> Value;
password(Value) -> binary(Value).

-spec text(term()) -> string().
text(Value) when is_binary(Value) -> binary_to_list(Value);
text(Value) when is_list(Value) -> Value;
text(Value) when is_atom(Value) -> atom_to_list(Value);
text(Value) -> Value.

-spec binary(term()) -> binary().
binary(Value) when is_list(Value) -> list_to_binary(Value);
binary(Value) when is_atom(Value) -> atom_to_binary(Value);
binary(Value) -> Value.
