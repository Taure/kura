-module(kura_driver_pgo).
-moduledoc """
PostgreSQL driver impl using [pgo](https://hex.pm/packages/pgo).

Wraps pgo's `query/3` and `query/4` and pgo's `transaction/2` behind
the `kura_driver` behaviour so kura code does not need to know which
client library is in use.

## Transaction context

pgo stashes the in-flight transaction conn in the process dict
(`pgo_transaction_connection`). `query/5` checks for it: if present,
the query routes to the transaction conn via `pgo:query/3` (pool
key); if absent, the query leases a fresh conn via `kura_pool` and
runs it caller-driven via `pgo:query/4`. Same shape as the previous
hardcoded path in `kura_db:query/3` — extracted into a driver impl
so non-pgo drivers can substitute their own transaction conventions.
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

-spec query(module(), kura_pool:name(), iodata(), [term()], map()) -> dynamic().
query(PoolMod, Pool, SQL, Params, Opts) ->
    Decode = maps:get(decode_opts, Opts, ?DEFAULT_DECODE_OPTS),
    case erlang:get(pgo_transaction_connection) of
        undefined ->
            kura_pool:with_conn(PoolMod, Pool, fun(Conn) ->
                pgo:query(SQL, Params, #{decode_opts => Decode}, Conn)
            end);
        _TxConn ->
            %% Inside a pgo transaction; let pgo route to the tx conn
            %% from its process dict.
            pgo:query(SQL, Params, #{pool => Pool, decode_opts => Decode})
    end.

-spec query_on(kura_pool:conn(), iodata(), [term()], map()) -> dynamic().
query_on(Conn, SQL, Params, Opts) ->
    Decode = maps:get(decode_opts, Opts, ?DEFAULT_DECODE_OPTS),
    pgo:query(SQL, Params, #{decode_opts => Decode}, Conn).

-spec transaction(module(), kura_pool:name(), fun(() -> term()), map()) -> term().
transaction(_PoolMod, Pool, Fun, Opts) ->
    PgoOpts = maps:with([pool_options], Opts),
    pgo:transaction(Pool, Fun, PgoOpts).

-spec ensure_database(map()) -> ok | {error, term()}.
ensure_database(Config) ->
    case maps:find(database, Config) of
        error ->
            ok;
        {ok, DbName} ->
            do_ensure_database(Config, binary_to_list(DbName))
    end.

-spec probe_pool(kura_pool:name()) -> ok | {error, term()}.
probe_pool(Pool) ->
    try pgo:query(~"SELECT 1", [], #{pool => Pool}) of
        #{rows := _} -> ok;
        {error, Reason} -> {error, Reason}
    catch
        Class:CatchReason -> {error, {Class, CatchReason}}
    end.

%%----------------------------------------------------------------------
%% Internal: bootstrap the maintenance DB to CREATE DATABASE if missing.
%%----------------------------------------------------------------------

do_ensure_database(Config, Database) ->
    TmpPool = kura_migrator_tmp_pool,
    TmpConfig = apply_pool_extras(base_pool_config(Config, Database)),
    case try_connect(TmpPool, TmpConfig) of
        ok ->
            stop_tmp_pool(TmpPool);
        {error, _} ->
            stop_tmp_pool(TmpPool),
            create_database(Config, Database)
    end.

base_pool_config(Config, Database) ->
    #{
        host => binary_to_list(maps:get(hostname, Config, ~"localhost")),
        port => maps:get(port, Config, 5432),
        database => Database,
        user => binary_to_list(maps:get(username, Config, ~"postgres")),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => 1,
        decode_opts => ?DEFAULT_DECODE_OPTS
    }.

apply_pool_extras(Base) ->
    WithSocket =
        case application:get_env(kura, socket_options, []) of
            Opts when is_list(Opts), Opts =/= [] -> Base#{socket_options => Opts};
            _ -> Base
        end,
    WithSSL =
        case application:get_env(kura, ssl, false) of
            true -> WithSocket#{ssl => true};
            _ -> WithSocket
        end,
    case application:get_env(kura, ssl_options, []) of
        SSLOpts when is_list(SSLOpts), SSLOpts =/= [] ->
            WithSSL#{ssl_options => SSLOpts};
        _ ->
            WithSSL
    end.

try_connect(TmpPool, TmpConfig) ->
    case pgo_sup:start_child(TmpPool, TmpConfig) of
        {ok, _} ->
            case pgo:query(~"SELECT 1", [], #{pool => TmpPool}) of
                #{rows := _} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

create_database(Config, Database) ->
    TmpPool = kura_migrator_create_pool,
    TmpConfig = apply_pool_extras(base_pool_config(Config, "postgres")),
    case pgo_sup:start_child(TmpPool, TmpConfig) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    DbBin = list_to_binary(Database),
    QuotedDb = iolist_to_binary([<<"\"">>, DbBin, <<"\"">>]),
    SQL = iolist_to_binary([~"CREATE DATABASE ", QuotedDb]),
    _ = pgo:query(SQL, [], #{pool => TmpPool}),
    logger:info(#{msg => ~"kura: created database", database => Database}),
    stop_tmp_pool(TmpPool).

stop_tmp_pool(TmpPool) ->
    _ = supervisor:terminate_child(pgo_sup, TmpPool),
    _ = supervisor:delete_child(pgo_sup, TmpPool),
    ok.
