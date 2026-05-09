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
    transaction/3
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

-spec transaction(module(), kura_pool:name(), fun(() -> term())) -> term().
transaction(_PoolMod, Pool, Fun) ->
    pgo:transaction(Fun, #{pool => Pool}).
