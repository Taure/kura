-module(kura_sandbox).
-moduledoc """
Test sandbox that wraps each test in a rolled-back transaction.

Usage in Common Test or EUnit:

```erlang
init_per_testcase(_TestCase, Config) ->
    kura_sandbox:checkout(my_repo),
    Config.

end_per_testcase(_TestCase, _Config) ->
    kura_sandbox:checkin(my_repo),
    ok.
```

All database operations within the test use the sandboxed connection.
On checkin, the transaction is rolled back so no test data persists.
""".

-include("kura.hrl").

-export([checkout/1, checkin/1]).

-doc "Begin a sandboxed transaction for the repo's connection pool.".
-spec checkout(module()) -> ok.
checkout(RepoMod) ->
    Pool = get_pool(RepoMod),
    {ok, Ref, Conn} = pgo:checkout(Pool),
    erlang:put({kura_sandbox, Pool}, {Ref, Conn}),
    pgo:with_conn(Conn, fun() ->
        pgo:query(<<"BEGIN">>, [])
    end),
    ok.

-doc "Roll back the sandboxed transaction and return the connection to the pool.".
-spec checkin(module()) -> ok.
checkin(RepoMod) ->
    Pool = get_pool(RepoMod),
    case erlang:erase({kura_sandbox, Pool}) of
        {Ref, Conn} ->
            pgo:with_conn(Conn, fun() ->
                pgo:query(<<"ROLLBACK">>, [])
            end),
            pgo:checkin(Ref, Conn),
            ok;
        undefined ->
            ok
    end.

get_pool(RepoMod) ->
    Config = RepoMod:config(),
    maps:get(pool, Config, RepoMod).
