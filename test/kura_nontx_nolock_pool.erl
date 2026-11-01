-module(kura_nontx_nolock_pool).
-moduledoc """
A Postgres pool that declares no capabilities, so the migrator treats it
as a backend without `advisory_locks` and takes no lock.
""".

-export([checkout/2, checkin/2]).

checkout(Pool, Opts) ->
    kura_pool_minato:checkout(Pool, Opts).

checkin(Pool, Token) ->
    kura_pool_minato:checkin(Pool, Token).
