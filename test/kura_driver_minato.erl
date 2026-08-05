-module(kura_driver_minato).
-moduledoc """
`kura_driver` implementation over [minato](https://hex.pm/packages/minato).

## Where a query goes

Outside a transaction it goes to the pool: `minato:query/4` borrows a
connection, runs the statement and gives it back, all inside the call. Nothing
is held and nothing is threaded through kura.

Inside a transaction it goes to the connection the transaction is running on,
which `m:kura_pool_minato` holds and this module finds in the process
dictionary. That is the same convention the pgo driver used, for the same
reason: a query written inside `transaction/4`'s function is handed nothing, so
the connection has to be found rather than passed.

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

-doc "Run a statement on a pool, or on the transaction this process is inside.".
-spec query(module(), kura_pool:name(), iodata(), [term()], map()) -> dynamic().
query(_PoolMod, Pool, SQL, Params, Opts) ->
    case kura_pool_minato:transaction_conn(Pool) of
        undefined -> answered(minato:query(Pool, SQL, Params, decoding(Opts)));
        Holder -> kura_pool_minato:run(Holder, SQL, Params, decoding(Opts))
    end.

-doc "Run a statement on a connection the caller checked out.".
-spec query_on(kura_pool:conn(), iodata(), [term()], map()) -> dynamic().
query_on(Holder, SQL, Params, Opts) when is_pid(Holder) ->
    kura_pool_minato:run(Holder, SQL, Params, decoding(Opts)).

-doc """
Run `Fun` inside a transaction.

`BEGIN` first, `COMMIT` on a normal return, `ROLLBACK` on an exception, which is
then re-raised. Queries inside `Fun` find the connection through the process
dictionary.

Raises `error(transaction_rolled_back)` when the server answers the `COMMIT`
with `ROLLBACK`, which it does when the transaction had already failed. Nothing
was written, and returning the function's value there would report success for
work the server threw away.
""".
-spec transaction(module(), kura_pool:name(), fun(() -> term()), map()) -> term().
transaction(_PoolMod, Pool, Fun, _Opts) ->
    case kura_pool_minato:checkout(Pool, #{}) of
        {ok, Holder, _Token} -> inside(Pool, Holder, Fun);
        {error, Reason} -> error({kura_driver_minato, {no_connection, Reason}})
    end.

-doc "Create the database named in `Config` if it is not there.".
-spec ensure_database(map()) -> ok | {error, term()}.
ensure_database(Config) ->
    case maps:find(database, Config) of
        error -> ok;
        {ok, Database} -> ensured(Config, binary(Database))
    end.

-doc "Round trip a trivial statement against `Pool`.".
-spec probe_pool(kura_pool:name()) -> ok | {error, term()}.
probe_pool(Pool) ->
    minato:health(Pool).

%%----------------------------------------------------------------------
%% Transactions
%%----------------------------------------------------------------------

-spec inside(kura_pool:name(), pid(), fun(() -> term())) -> term().
inside(Pool, Holder, Fun) ->
    ok = kura_pool_minato:in_transaction(Pool, Holder),
    try
        _ = statement(Holder, ~"BEGIN"),
        Value = Fun(),
        committed(Holder, Value)
    catch
        Class:Reason:Stacktrace ->
            _ = statement(Holder, ~"ROLLBACK"),
            erlang:raise(Class, Reason, Stacktrace)
    after
        ok = kura_pool_minato:in_transaction(Pool, undefined),
        ok = kura_pool_minato:checkin(Pool, Holder)
    end.

-spec committed(pid(), term()) -> term().
committed(Holder, Value) ->
    case statement(Holder, ~"COMMIT") of
        #{command := rollback} -> error(transaction_rolled_back);
        _Committed -> Value
    end.

-spec statement(pid(), binary()) -> dynamic().
statement(Holder, SQL) ->
    kura_pool_minato:run(Holder, SQL, [], #{}).

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
    case maps:find(password, Config) of
        {ok, Password} -> Base#{password => binary(Password)};
        error -> Base
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

-spec first([atom()], map(), term()) -> term().
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

-spec binary(term()) -> binary().
binary(Value) when is_list(Value) -> list_to_binary(Value);
binary(Value) when is_atom(Value) -> atom_to_binary(Value);
binary(Value) -> Value.
