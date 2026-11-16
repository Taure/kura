-module(kura_driver_minato).
-moduledoc """
`kura_driver` implementation over [minato](https://hex.pm/packages/minato).

## Where a query goes

Outside a transaction it goes to the pool: `minato:query/4` borrows a
connection, runs the statement and gives it back, all inside the call. Nothing
is held and nothing is threaded through kura.

Inside a transaction it goes to the connection the transaction is running on,
which `m:kura_pool_minato` holds and this module finds in the process
dictionary. The reason is that a query written inside `transaction/4`'s
function is handed nothing, so the connection has to be found rather than
passed.

## Results and errors

minato answers `#{command := atom(), num_rows := integer(), rows := [row()]}`,
which is the shape kura already reads, and a failing statement answers `{error,
{pgsql_error, Fields}}` with `code` and `constraint` as binaries - which is what
`kura_repo_worker` turns into a changeset error. Nothing in kura had to change
for either.

## numeric

`numeric` decodes to a float here, which is not minato's default: minato hands
back the exact decimal text, because that is what the type is for. kura has
always given callers a float for it, through `pg_types`, and a driver swap is
the wrong moment to change what `avg/1` returns for everybody.

A repo that wants the exact form says `numeric_format => binary` in its driver
options, and should, for anything that is money.

## Transaction control in user SQL

A `COMMIT`, `END` or `ROLLBACK` run through `query/5` inside `transaction/4`
ends the transaction behind kura's back. Every statement there goes to the
transaction's holder, which reads the transaction status the server sends in
`ReadyForQuery` after it, and the statement's command tag. A statement that
leaves the connection `idle`, or answers with the tag `COMMIT`, raises
`error({kura_driver_minato, transaction_ended_by_statement})`. Whatever it
committed stays committed. From then on the holder refuses every statement meant
for the transaction without sending it to the server: a statement run by a
function that caught the error raises the same, so do the `COMMIT`, `SAVEPOINT`
and `RELEASE` that `transaction/4` would send, and the `ROLLBACK` and
`ROLLBACK TO SAVEPOINT` it would clean up with are skipped. No work meant for
the transaction runs outside it.

The status and the tag are read rather than the SQL text, so a statement is
caught however it is spelled. The tag is what catches `COMMIT AND CHAIN`, which
leaves a new transaction open, so the connection never reads as `idle`; that
transaction is rolled back when the connection is checked in.
`ROLLBACK AND CHAIN` and `ABORT AND CHAIN` answer with the tag `ROLLBACK`, as
`ROLLBACK TO SAVEPOINT` does. `transaction/4` begins with a transaction-local
setting sent in the same round trip as its `BEGIN`, and only a statement tagged
`ROLLBACK` that leaves the connection in a transaction is followed by one more
query to read it back, under the statement's `timeout`: a savepoint rollback
keeps the setting, a chained rollback has replaced the transaction and lost it,
and raises the same error. A `RESET ALL` inside the transaction clears the
setting too, so a later `ROLLBACK TO SAVEPOINT`, including the one a nested
`transaction/4` cleans up with, raises `transaction_ended_by_statement` while the
transaction is still open. Avoid `RESET ALL` inside `transaction/4`.

This covers transactions `transaction/4` opens, and a sandboxed test's: kura's
sandbox begins its transaction with `query_on/4` and `transaction => starts`,
which this driver opens and marks the way `transaction/4` does, and runs every
statement in it with `transaction => inside`, which goes through the same
check. The `RESET ALL` caveat above applies there too.

## Timeouts

`timeout` in the driver options is a deadline on the statement rather than on
the read. minato cancels a statement that outlives it on the server and reads
the cancellation through, so a slow query comes back as SQLSTATE `57014` and the
connection goes back to the pool rather than being closed.
""".

-behaviour(kura_driver).

-export([
    query/5,
    query_on/4,
    transaction/4,
    ensure_database/1,
    probe_pool/1
]).

-define(DEFAULT_DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

-doc """
Run a statement on a pool, or on the transaction this process is inside.

Inside a transaction, a statement that ends it, or one run after it has ended,
raises `error({kura_driver_minato, transaction_ended_by_statement})`.
""".
-spec query(module(), kura_pool:name(), iodata(), [term()], map()) -> dynamic().
query(_PoolMod, Pool, SQL, Params, Opts) ->
    case kura_pool_minato:transaction_conn(Pool) of
        undefined ->
            answered(minato:query(Pool, SQL, Params, decoding(Opts)));
        Holder ->
            still_open(kura_pool_minato:run_in_transaction(Holder, SQL, Params, decoding(Opts)))
    end.

-doc """
Run a statement on a connection the caller checked out.

With `transaction => starts`, `SQL` is a plain `BEGIN`: the driver opens the
transaction with its own, marked as `transaction/4` marks its own, and does not
send `SQL`. With `transaction => inside`, a statement that
ends that transaction, or one run after it has ended, raises
`error({kura_driver_minato, transaction_ended_by_statement})`.
""".
-spec query_on(kura_pool:conn(), iodata(), [term()], map()) -> dynamic().
query_on(Holder, SQL, Params, Opts) when is_pid(Holder) ->
    case maps:get(transaction, Opts, none) of
        starts ->
            kura_pool_minato:open_transaction(Holder);
        inside ->
            still_open(kura_pool_minato:run_in_transaction(Holder, SQL, Params, decoding(Opts)));
        _None ->
            kura_pool_minato:run(Holder, SQL, Params, decoding(Opts))
    end.

-doc """
Run `Fun` inside a transaction.

`BEGIN` first, `COMMIT` on a normal return, `ROLLBACK` on an exception, which is
then re-raised. Queries inside `Fun` find the connection through the process
dictionary.

Raises `error(transaction_rolled_back)` when the server answers the `COMMIT`
with `ROLLBACK`, which it does when the transaction had already failed. Nothing
was written, and returning the function's value there would report success for
work the server threw away.

Called again from the same process inside a transaction on the same pool, it
runs `Fun` on that transaction's connection under a `SAVEPOINT`: a normal return
releases it, and an exception rolls back to it and is re-raised, leaving the
work before it in place. A nested transaction therefore commits only with the
outer one. Here `transaction_rolled_back` means a statement inside `Fun` failed
and only the nested work was undone: the outer transaction is still open and
may catch it and carry on. A process spawned from `Fun` is outside the
transaction, and its own `transaction/4` is an independent outermost one.

Other failures raise `error({kura_driver_minato, {Failure, Reason}})`:

- `begin_failed` or `savepoint_failed`: the transaction or savepoint could not
  start, and `Fun` did not run.
- `commit_failed` or `release_failed`: the server refused it, so nothing in
  that transaction or savepoint was kept.
- `commit_unknown`: no answer came back, for example on a timeout or a lost
  connection, so the server may or may not have committed.
- `rollback_to_failed`: a nested `Fun` raised and the server refused the
  `ROLLBACK TO SAVEPOINT`, for example because `Fun` released the savepoint
  itself. The whole transaction is rolled back rather than keep work the nested
  call was meant to undo, and anything run in it afterwards raises
  `transaction_ended_by_statement`.
- `no_connection`: no connection could be checked out, and `Fun` did not run.

`error({kura_driver_minato, transaction_ended_by_statement})` when a statement
run inside `Fun`, such as a raw `COMMIT`, `END` or `ROLLBACK`, ended the
transaction. Whatever it committed stays committed and nothing after it runs.
Nested, it is raised the same way: the statement ended the outermost
transaction, not just the savepoint.

A server error is reduced to its `code`, `constraint`, `severity` and
`severity_non_localised`, since its message and detail can carry row values.
""".
-spec transaction(module(), kura_pool:name(), fun(() -> term()), map()) -> term().
transaction(_PoolMod, Pool, Fun, _Opts) ->
    case kura_pool_minato:transaction_conn(Pool) of
        undefined -> outermost(Pool, Fun);
        Holder -> nested(Holder, Fun)
    end.

-doc "Create the database named in `Config` if it is not there.".
-spec ensure_database(map()) -> ok | {error, term()}.
ensure_database(Config) ->
    case Config of
        #{database := Database} -> ensured(Config, binary(Database));
        #{} -> ok
    end.

-doc "Round trip a trivial statement against `Pool`.".
-spec probe_pool(kura_pool:name()) -> ok | {error, term()}.
probe_pool(Pool) ->
    minato:health(Pool).

%%----------------------------------------------------------------------
%% Transactions
%%----------------------------------------------------------------------

-spec outermost(kura_pool:name(), fun(() -> term())) -> term().
outermost(Pool, Fun) ->
    case kura_pool_minato:checkout(Pool, #{}) of
        {ok, Holder, _Token} -> inside(Pool, Holder, Fun);
        {error, Reason} -> error({kura_driver_minato, {no_connection, Reason}})
    end.

-spec inside(kura_pool:name(), pid(), fun(() -> term())) -> term().
inside(Pool, Holder, Fun) ->
    ok = kura_pool_minato:in_transaction(Pool, Holder),
    try
        ok = expect(opened(Holder), 'begin', begin_failed),
        Value = Fun(),
        committed(Holder, Value)
    catch
        Class:Reason:Stacktrace ->
            _ = within(Holder, ~"ROLLBACK"),
            erlang:raise(Class, Reason, Stacktrace)
    after
        ok = kura_pool_minato:in_transaction(Pool, undefined),
        ok = kura_pool_minato:checkin(Pool, Holder)
    end.

-spec committed(pid(), term()) -> term().
committed(Holder, Value) ->
    case checked(Holder, ~"COMMIT") of
        #{command := commit} ->
            Value;
        #{command := rollback} ->
            error(transaction_rolled_back);
        {error, {pgsql_error, _}} = Refused ->
            failed(commit_failed, Refused);
        Unknown ->
            failed(commit_unknown, Unknown)
    end.

-spec nested(pid(), fun(() -> term())) -> term().
nested(Holder, Fun) ->
    Savepoint = savepoint_name(),
    ok = expect(checked(Holder, [~"SAVEPOINT ", Savepoint]), savepoint, savepoint_failed),
    try
        Value = Fun(),
        released(Holder, Savepoint, Value)
    catch
        Class:Reason:Stacktrace ->
            ok = rolled_back_to(Holder, Savepoint),
            erlang:raise(Class, Reason, Stacktrace)
    end.

%% A transaction that has ended, or a connection that is gone, leaves nothing to
%% roll back to, and the exception being cleaned up after says more.
-spec rolled_back_to(pid(), binary()) -> ok.
rolled_back_to(Holder, Savepoint) ->
    case within(Holder, [~"ROLLBACK TO SAVEPOINT ", Savepoint]) of
        #{command := rollback} ->
            _ = within(Holder, [~"RELEASE SAVEPOINT ", Savepoint]),
            ok;
        {error, {pgsql_error, _}} = Refused ->
            _ = within(Holder, ~"ROLLBACK"),
            failed(rollback_to_failed, Refused);
        _EndedOrGone ->
            ok
    end.

-spec released(pid(), binary(), term()) -> term().
released(Holder, Savepoint, Value) ->
    case checked(Holder, [~"RELEASE SAVEPOINT ", Savepoint]) of
        #{command := release} -> Value;
        {error, {pgsql_error, #{code := ~"25P02"}}} -> error(transaction_rolled_back);
        Failed -> failed(release_failed, Failed)
    end.

-spec savepoint_name() -> binary().
savepoint_name() ->
    iolist_to_binary([~"kura_savepoint_", integer_to_binary(erlang:unique_integer([positive]))]).

-spec expect(dynamic(), atom(), atom()) -> ok.
expect(#{command := Command}, Command, _Failure) -> ok;
expect(Answer, _Command, Failure) -> failed(Failure, Answer).

-spec failed(atom(), dynamic()) -> no_return().
failed(Failure, {error, {pgsql_error, Fields}}) when is_map(Fields) ->
    error(
        {kura_driver_minato,
            {Failure,
                {pgsql_error,
                    maps:with([code, constraint, severity, severity_non_localised], Fields)}}}
    );
failed(Failure, {error, Reason}) ->
    error({kura_driver_minato, {Failure, Reason}});
failed(Failure, #{command := Command}) ->
    error({kura_driver_minato, {Failure, {unexpected, Command}}});
failed(Failure, Answer) ->
    error({kura_driver_minato, {Failure, Answer}}).

-spec still_open(ended | {dynamic(), kura_pool_minato:transaction_status()}) -> dynamic().
still_open({_Answer, ended}) -> ended();
still_open({Answer, _Open}) -> Answer;
still_open(ended) -> ended().

-spec ended() -> no_return().
ended() ->
    error({kura_driver_minato, transaction_ended_by_statement}).

-spec checked(pid(), iodata()) -> dynamic().
checked(Holder, SQL) ->
    case within(Holder, SQL) of
        ended -> ended();
        Answer -> Answer
    end.

%% A raise here would replace the exception being cleaned up after, so it becomes
%% an answer that the caller names like any other failure.
-spec within(pid(), iodata()) -> dynamic().
within(Holder, SQL) ->
    try kura_pool_minato:run_in_transaction(Holder, SQL, [], #{}) of
        ended -> ended;
        {Answer, _Status} -> Answer
    catch
        Class:_ -> {raised, Class}
    end.

-spec opened(pid()) -> dynamic().
opened(Holder) ->
    try
        kura_pool_minato:open_transaction(Holder)
    catch
        Class:_ -> {raised, Class}
    end.

%%----------------------------------------------------------------------
%% Creating the database
%%----------------------------------------------------------------------

-spec ensured(map(), binary()) -> ok | {error, term()}.
ensured(Config, Database) ->
    case minato_conn:connect(connection(Config, Database)) of
        {ok, Conn} -> minato_conn:close(Conn);
        {error, _Missing} -> created(Config, Database)
    end.

-spec created(map(), binary()) -> ok | {error, term()}.
created(Config, Database) ->
    case minato_conn:connect(connection(Config, ~"postgres")) of
        {ok, Conn} -> creating(Conn, Database);
        {error, Reason} -> {error, Reason}
    end.

-spec creating(minato_conn:conn(), binary()) -> ok | {error, term()}.
creating(Conn, Database) ->
    SQL = <<"CREATE DATABASE ", (quoted(Database))/binary>>,
    Answer = minato_query:simple(Conn, SQL),
    ok = close(Answer, Conn),
    case Answer of
        {ok, _Results, _Made} ->
            logger:info(
                #{event => database_created, database => Database}, #{domain => [kura]}
            );
        {error, Reason, _Failed} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

-spec close(dynamic(), minato_conn:conn()) -> ok.
close({ok, _Results, Conn}, _Fallback) -> minato_conn:close(Conn);
close({error, _Reason, Conn}, _Fallback) -> minato_conn:close(Conn);
close({error, _Reason}, Fallback) -> minato_conn:close(Fallback).

-spec connection(map(), binary()) -> minato_conn:opts().
connection(Config, Database) ->
    Base = #{
        host => host(Config),
        port => maps:get(port, Config, 5432),
        user => binary(first([user, username], Config, ~"postgres")),
        database => Database,
        connect_timeout => 5000
    },
    secured(passworded(Base, Config)).

-spec passworded(map(), map()) -> map().
passworded(Base, Config) ->
    case Config of
        #{password := Password} -> Base#{password => binary(Password)};
        #{} -> Base
    end.

-spec secured(map()) -> map().
secured(Base) ->
    case application:get_env(kura, ssl, false) of
        true -> Base#{ssl => true, ssl_options => application:get_env(kura, ssl_options, [])};
        _Plain -> Base
    end.

-spec host(map()) -> string().
host(Config) ->
    case first([host, hostname], Config, ~"localhost") of
        Value when is_binary(Value) -> binary_to_list(Value);
        Value when is_list(Value) -> Value;
        Value when is_atom(Value) -> atom_to_list(Value)
    end.

%% Config values come from the application environment.
-spec first([atom()], map(), term()) -> dynamic().
first([], _Config, Default) ->
    Default;
first([Key | Rest], Config, Default) ->
    case maps:find(Key, Config) of
        {ok, Value} when is_binary(Value); is_list(Value); is_atom(Value) -> Value;
        _Absent -> first(Rest, Config, Default)
    end.

%%----------------------------------------------------------------------
%% Options and shapes
%%----------------------------------------------------------------------

-spec answered({ok, term()} | {error, term()}) -> dynamic().
answered({ok, Result}) -> Result;
answered({error, Reason}) -> {error, Reason}.

-spec decoding(map()) -> map().
decoding(Opts) ->
    Decode = maps:get(decode_opts, Opts, ?DEFAULT_DECODE_OPTS),
    Carried = maps:with([timeout, uuid_format, datetime_format, numeric_format], Opts),
    Carried#{
        return_rows_as_maps => lists:member(return_rows_as_maps, Decode),
        column_name_as_atom => lists:member(column_name_as_atom, Decode),
        numeric_format => maps:get(numeric_format, Opts, float)
    }.

-spec quoted(binary()) -> binary().
quoted(Name) ->
    Escaped = binary:replace(Name, ~"\"", ~"\"\"", [global]),
    <<$", Escaped/binary, $">>.

-spec binary(dynamic()) -> binary().
binary(Value) when is_list(Value) -> list_to_binary(Value);
binary(Value) when is_atom(Value) -> atom_to_binary(Value);
binary(Value) -> Value.
