-module(kura_repo).
-moduledoc """
Behaviour for defining a repository (database connection).

Implement `config/0` to return connection parameters. The `pool` key defaults
to the module name.

```erlang
-module(my_repo).
-behaviour(kura_repo).

config() ->
    #{database => <<"my_db">>,
      hostname => <<"localhost">>,
      port => 5432,
      username => <<"postgres">>,
      password => <<"secret">>,
      pool_size => 10}.
```
""".

-callback config() ->
    #{
        pool => atom(),
        database => binary(),
        hostname => binary(),
        port => integer(),
        username => binary(),
        password => binary(),
        pool_size => integer()
    }.
