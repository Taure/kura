-module(kura_repo).
-moduledoc """
Behaviour for defining a repository (database connection).

Implement `otp_app/0` to tell Kura which application owns the repo's
migrations.

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0]).

otp_app() -> my_app.
```

Configure the database connection under the `kura` application env.
Kura starts the pgo pool automatically during application startup:

```erlang
[{kura, [
    {repo, my_repo},
    {host, "localhost"},
    {port, 5432},
    {database, "my_db"},
    {user, "postgres"},
    {password, "secret"},
    {pool_size, 10}
]}].
```

For backward compatibility, Kura also supports per-app config via
`application:get_env(OtpApp, RepoModule)`. The kura app env is checked
first.

Optionally implement `init/1` to modify config at runtime — useful for reading
secrets from files, environment variables, or external services:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0, init/1]).

otp_app() -> my_app.

init(Config) ->
    Config#{
        password => list_to_binary(os:getenv("DB_PASSWORD", "postgres"))
    }.
```
""".

-export([config/1]).

-callback otp_app() -> atom().

-callback init(Config :: map()) -> map().
-optional_callbacks([init/1]).

-doc "Read the repo configuration from application environment.".
-spec config(module()) -> map().
config(RepoMod) ->
    Config = read_config(RepoMod),
    case erlang:function_exported(RepoMod, init, 1) of
        true -> RepoMod:init(Config);
        false -> Config
    end.

-spec read_config(module()) -> map().
read_config(RepoMod) ->
    %% Try kura app env first, fall back to repo's otp_app env for
    %% backward compatibility.
    case application:get_env(kura, repo) of
        {ok, RepoMod} ->
            Env = fun(Key, Default) ->
                application:get_env(kura, Key, Default)
            end,
            #{
                hostname => list_to_binary(Env(host, "localhost")),
                port => Env(port, 5432),
                database => list_to_binary(Env(database, "postgres")),
                username => list_to_binary(Env(user, "postgres")),
                password => list_to_binary(Env(password, "")),
                pool_size => Env(pool_size, 10)
            };
        _ ->
            App = RepoMod:otp_app(),
            case application:get_env(App, RepoMod) of
                {ok, C} when is_map(C) -> C;
                _ -> #{}
            end
    end.
