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
Repos go in a `{repos, #{Name => Cfg}}` map; Kura starts the pool for
each repo automatically during application startup.

```erlang
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            port => 5432,
            database => "my_db",
            user => "postgres",
            password => "secret",
            pool_size => 10
        }
    }}
]}].
```

The same form scales to multiple repos (e.g. a Postgres primary plus a
SQLite analytics store):

```erlang
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            database => "my_db",
            pool_size => 10
        },
        analytics_repo => #{
            backend => kura_backend_sqlite,
            database => <<":memory:">>
        }
    }}
]}].
```

Each repo's dialect, pool, and driver are resolved from its `backend`
key. Queries through different repos use their own dialects; the query
cache is keyed per repo.

For backward compatibility, Kura also accepts the flat single-repo
form (`{repo, _}`/`{backend, _}`/`{host, _}` at the kura env level) and
the per-app form (`application:get_env(OtpApp, RepoModule)`). The
`{repos, ...}` map is checked first, then the flat env, then the per-app
form. New projects should prefer the map form.

Optionally implement `init/1` to modify config at runtime - useful for reading
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

-eqwalizer({nowarn_function, read_config/1}).

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
    case application:get_env(kura, repos) of
        {ok, Map} when is_map(Map), is_map_key(RepoMod, Map) ->
            maps:get(RepoMod, Map);
        _ ->
            case application:get_env(kura, repo) of
                {ok, RepoMod} ->
                    kura_env_to_config();
                _ ->
                    per_app_config(RepoMod)
            end
    end.

-spec per_app_config(module()) -> map().
per_app_config(RepoMod) ->
    case
        is_atom(RepoMod) andalso
            code:ensure_loaded(RepoMod) =/= {error, nofile} andalso
            erlang:function_exported(RepoMod, otp_app, 0)
    of
        true ->
            App = RepoMod:otp_app(),
            case application:get_env(App, RepoMod) of
                {ok, C} when is_map(C) -> C;
                _ -> #{}
            end;
        false ->
            #{}
    end.

%% Map kura app env keys into a config map. Excludes bookkeeping keys
%% that aren't connection config.
-spec kura_env_to_config() -> map().
kura_env_to_config() ->
    Excluded = [
        repo,
        backend,
        ensure_database,
        migration_pool_ready_timeout,
        migration_pool_ready_interval,
        pg_types
    ],
    All = application:get_all_env(kura),
    Filtered = [{K, V} || {K, V} <- All, not lists:member(K, Excluded)],
    maps:from_list(Filtered).
