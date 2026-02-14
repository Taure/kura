-module(kura_repo).

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
