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

The holder watches whoever owns the checkout and discards the connection if that
process dies, so the pool opens a fresh one in its place. Nothing the owner left
on the session survives into the next borrower: an advisory lock, a `SET`, a
temporary table or an open transaction all end with the connection. A
statement that fails without handing a connection back, or raises, ends the
holder the same way, since there is no connection left to answer from. A call
that times out ends it too, so a request still queued behind it cannot run
later. `give_away/3` moves the watch rather than the socket, because the
socket never left the holder.

`run_in_transaction/4` is `run/4` for a statement meant for an open
transaction: the holder refuses it once that transaction has ended, and answers
with what became of the transaction after it.

A `checkin/2` hands the connection to minato, which rolls back an open
transaction and resets the session before it lends the connection again.
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

-export([open_transaction/1, run/4, run_in_transaction/4, transaction_conn/1, in_transaction/2]).

-ifdef(TEST).
-export([marked/2]).
-endif.

-export_type([transaction_status/0]).

-doc """
What became of the transaction a statement was run in. Used by
`m:kura_driver_minato`.

`transaction` and `failed` are the status byte of the `ReadyForQuery` after it:
still open, or open with a statement already failed in it. `ended` is a
transaction the statement ended: the connection went back to `idle`, the
statement's tag was `COMMIT`, which `COMMIT AND CHAIN` answers, or its tag was
`ROLLBACK` and the transaction left open is not the one `open_transaction/1`
began, as after `ROLLBACK AND CHAIN`. `unknown` when no answer came back to read
it from.
""".
-type transaction_status() :: transaction | failed | ended | unknown.

-type within() :: opening | anywhere | transaction.

-define(CALL_TIMEOUT, 30000).

-define(OPEN_TRANSACTION, ~"BEGIN; SET LOCAL kura.txn = '1'").
-define(MARKER, ~"SELECT current_setting('kura.txn', true)").

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
        prepared_statements,
        concurrent_indexes
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

A pool needs `user` (or `username`). Without it `start_pool/2` returns
`{error, {missing_option, user}}` and starts nothing, where minato would
start a pool whose connections cannot log in.

`transport` is passed through. minato chooses one by default and documents the
other as the way back from that choice, which is only true from here if the
option survives the translation.

A name that is already running returns `{error, {already_started, Pid}}` with
the running pool's pid, and that pool keeps the options it was started with.
kura tells a fresh start from a running pool by this answer, so a second repo
naming the same pool is not told the pool was started for it.
""".
-spec start_pool(kura_pool:name(), kura_pool:opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts) when is_map_key(user, Opts); is_map_key(username, Opts) ->
    {ok, _Started} = application:ensure_all_started(minato),
    case minato:start_pool(Name, translate(Opts)) of
        {ok, Pid} when is_pid(Pid) -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end;
start_pool(_Name, _Opts) ->
    {error, {missing_option, user}}.

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
    after Timeout + 1000 ->
        exit(Holder, kill),
        receive
            {'DOWN', Monitor, process, Holder, _} -> ok
        end,
        receive
            {holding, Holder} -> ok;
            {failed, Holder, _} -> ok
        after 0 -> ok
        end,
        {error, checkout_timeout}
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
discards the connection, and it is the one that should call `checkin/2`.
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

-doc """
Run a statement on a held connection. Used by `m:kura_driver_minato`.

`timeout` in `Opts` is handed to minato, which cancels the statement on the
server when it expires. It can shorten a statement but not lengthen it: minato
still gives up on any single read after the connection's own read timeout. The
wait for the holder's answer is not bounded here, so whatever ends the statement
comes back as its error rather than the caller giving up first. A pool whose
`timeout` is `infinity` therefore waits as long as the socket does.
""".
-spec run(pid(), iodata(), [term()], map()) -> dynamic().
run(Holder, Sql, Params, Opts) ->
    reraised(call(Holder, {run, Sql, Params, Opts}, infinity)).

-doc """
Begin a transaction on a held connection. Used by `m:kura_driver_minato`.

Answers what the `BEGIN` answered. It goes as one simple query together with
`SET LOCAL kura.txn = '1'`, so it costs one round trip. `SET` takes no snapshot,
so the transaction's first statement may still be `SET TRANSACTION`. The setting
is what tells a statement tagged `ROLLBACK` that left the connection in a
transaction apart: `ROLLBACK TO SAVEPOINT` keeps it, while `ROLLBACK AND CHAIN`
and `ABORT AND CHAIN` start a new transaction without it. So does `RESET ALL`,
after which a savepoint rollback reads as a replaced transaction.
""".
-spec open_transaction(pid()) -> dynamic().
open_transaction(Holder) ->
    reraised(call(Holder, {run, opening, ?OPEN_TRANSACTION, [], #{}}, infinity)).

-doc """
Run a statement on a held connection that is inside a transaction, and say what
became of the transaction after it. Used by `m:kura_driver_minato`.

Answers `ended` without running the statement when the connection is idle, or
once a call to this function has found the transaction ended, so a statement
meant for that transaction never runs outside it. The transaction is expected to
have been begun by `open_transaction/1`: a statement tagged `ROLLBACK` that
leaves the connection in a transaction without its setting has replaced it, and
counts as having ended it. Otherwise
`{Answer, Status}`, where `Answer` is what `run/4` would answer and `Status` lets
the caller see a statement that ended the transaction itself.
""".
-spec run_in_transaction(pid(), iodata(), [term()], map()) ->
    ended | {dynamic(), transaction_status()}.
run_in_transaction(Holder, Sql, Params, Opts) ->
    case reraised(call(Holder, {run_in_transaction, Sql, Params, Opts}, infinity)) of
        {reported, Answer, Status} -> {Answer, Status};
        ended -> ended;
        Unanswered -> {Unanswered, unknown}
    end.

-doc "The holder of the transaction this process is inside, or `undefined`.".
-spec transaction_conn(kura_pool:name()) -> pid() | undefined.
transaction_conn(Pool) ->
    case erlang:get({?MODULE, Pool}) of
        Holder when is_pid(Holder) -> Holder;
        undefined -> undefined
    end.

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
            held(Pool, Conn, erlang:monitor(process, Owner), open);
        {error, Reason} ->
            Owner ! {failed, self(), Reason},
            ok
    end.

-spec held(kura_pool:name(), minato_conn:conn(), reference(), open | ended) -> ok.
held(Pool, Conn, Monitor, Transaction) ->
    receive
        {call, Alias, {run, opening, Sql, Params, Opts}} ->
            answered(Pool, {Conn, Monitor, Transaction}, Alias, {opening, Sql, Params, Opts});
        {call, Alias, {run, Sql, Params, Opts}} ->
            answered(Pool, {Conn, Monitor, Transaction}, Alias, {anywhere, Sql, Params, Opts});
        {call, Alias, {run_in_transaction, Sql, Params, Opts}} ->
            case {Transaction, minato_conn:transaction_status(Conn)} of
                {open, Status} when Status =/= idle ->
                    answered(
                        Pool, {Conn, Monitor, Transaction}, Alias, {transaction, Sql, Params, Opts}
                    );
                _Ended ->
                    Alias ! {Alias, ended},
                    held(Pool, Conn, Monitor, ended)
            end;
        {call, Alias, checkin} ->
            ok = minato_pool:checkin(Pool, Conn),
            true = erlang:demonitor(Monitor, [flush]),
            Alias ! {Alias, ok},
            ok;
        {call, Alias, {give_away, NewOwner}} ->
            true = erlang:demonitor(Monitor, [flush]),
            Alias ! {Alias, ok},
            held(Pool, Conn, erlang:monitor(process, NewOwner), Transaction);
        {'DOWN', Monitor, process, _Owner, _Reason} ->
            minato_pool:discard(Pool, Conn)
    end.

-spec answered(
    kura_pool:name(),
    {minato_conn:conn(), reference(), open | ended},
    reference(),
    {within(), iodata(), [term()], map()}
) -> ok.
answered(Pool, {Conn, Monitor, Transaction}, Alias, {Within, Sql, Params, Opts}) ->
    try ran(Within, Conn, Sql, Params, Opts) of
        {lost, Answer} ->
            minato_pool:discard(Pool, Conn),
            Alias ! {Alias, reported(Within, Answer, unknown)},
            ok;
        {continue, Answer, Next} ->
            case became(Within, Answer, Next, Opts) of
                {lost, Lost} ->
                    minato_pool:discard(Pool, Next),
                    Alias ! {Alias, reported(Within, Lost, unknown)},
                    ok;
                {Status, Checked} ->
                    Alias ! {Alias, reported(Within, Answer, Status)},
                    held(Pool, Checked, Monitor, still(Within, Status, Transaction))
            end
    catch
        Class:Reason:Stack ->
            minato_pool:discard(Pool, Conn),
            Alias ! {Alias, {raised, Class, Reason, Stack}},
            ok
    end.

-spec became(within(), dynamic(), minato_conn:conn(), map()) ->
    {transaction_status(), minato_conn:conn()} | {lost, {error, term()}}.
became(_Within, #{command := commit}, Conn, _Opts) ->
    {ended, Conn};
became(transaction, #{command := rollback}, Conn, Opts) ->
    case minato_conn:transaction_status(Conn) of
        idle -> {ended, Conn};
        _Chained -> marked(Conn, Opts)
    end;
became(_Within, _Answer, Conn, _Opts) ->
    case minato_conn:transaction_status(Conn) of
        idle -> {ended, Conn};
        Status -> {Status, Conn}
    end.

-spec marked(minato_conn:conn(), map()) ->
    {transaction_status(), minato_conn:conn()} | {lost, {error, term()}}.
marked(Conn, Opts) ->
    case minato_query:simple(Conn, ?MARKER, maps:with([timeout], Opts)) of
        {ok, [#{rows := [{~"1"}]}], Next} -> {transaction, Next};
        {ok, _Replaced, Next} -> {ended, Next};
        {error, _Reason, Next} -> {ended, Next};
        {error, Reason} -> {lost, {error, Reason}}
    end.

-spec still(within(), transaction_status(), open | ended) -> open | ended.
still(transaction, ended, _Transaction) -> ended;
still(_Within, _Status, Transaction) -> Transaction.

-spec reported(within(), dynamic(), transaction_status()) -> dynamic().
reported(transaction, Answer, Status) -> {reported, Answer, Status};
reported(_Within, Answer, _Status) -> Answer.

-spec ran(within(), minato_conn:conn(), iodata(), [term()], map()) ->
    {continue, dynamic(), minato_conn:conn()} | {lost, dynamic()}.
ran(opening, Conn, Sql, _Params, _Opts) ->
    case minato_query:simple(Conn, Sql) of
        {ok, [Began | _Marked], Next} -> {continue, Began, Next};
        {error, Reason, Next} -> {continue, {error, Reason}, Next};
        {error, Reason} -> {lost, {error, Reason}}
    end;
ran(_Within, Conn, Sql, Params, Opts) ->
    case minato_query:query(Conn, Sql, Params, Opts) of
        {ok, Result, Next} -> {continue, Result, Next};
        {error, Reason, Next} -> {continue, {error, Reason}, Next};
        {error, Reason} -> {lost, {error, Reason}}
    end.

-spec reraised(dynamic()) -> dynamic().
reraised({raised, Class, Reason, Stack}) -> erlang:raise(Class, Reason, Stack);
reraised(Answer) -> Answer.

-spec call(pid(), term()) -> dynamic().
call(Holder, Message) ->
    call(Holder, Message, ?CALL_TIMEOUT).

-spec call(pid(), term(), timeout()) -> dynamic().
call(Holder, Message, Timeout) ->
    Alias = erlang:monitor(process, Holder, [{alias, demonitor}]),
    Holder ! {call, Alias, Message},
    receive
        {Alias, Answer} ->
            true = erlang:demonitor(Alias, [flush]),
            Answer;
        {'DOWN', Alias, process, Holder, Reason} ->
            {error, {holder_died, Reason}}
    after Timeout ->
        exit(Holder, kill),
        true = erlang:demonitor(Alias, [flush]),
        {error, timeout}
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
connection(transport, Value, Connection) -> Connection#{transport => Value};
connection(_Key, _Value, Connection) -> Connection.

-spec password(dynamic()) -> binary() | fun(() -> binary()).
password(Value) when is_function(Value, 0) -> Value;
password(Value) -> binary(Value).

-spec text(dynamic()) -> string().
text(Value) when is_binary(Value) -> binary_to_list(Value);
text(Value) when is_list(Value) -> Value;
text(Value) when is_atom(Value) -> atom_to_list(Value);
text(Value) -> Value.

-spec binary(dynamic()) -> binary().
binary(Value) when is_list(Value) -> list_to_binary(Value);
binary(Value) when is_atom(Value) -> atom_to_binary(Value);
binary(Value) -> Value.
