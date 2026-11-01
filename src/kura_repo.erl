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
secrets from files, environment variables, or external services. It runs when
kura starts the repo's pool, not per query. For a repo in the `{repos, ...}` map
or the flat form that is during kura's own application start, before the
application that owns the repo has started, so it must not call that
application's processes:

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

Optionally implement `migration_apps/0` to run migrations shipped by other
applications against the same repo - an extension that owns part of the
schema, for instance. The application owning the repo module is always
included, so this lists only the extras:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0, migration_apps/0]).

otp_app() -> my_app.

migration_apps() -> [my_gdpr_extension].
```

`kura_migrator` orders them by their OTP `applications` lists, so a
dependency's migrations run first. See `kura_migrator:migration_apps/1`.
""".

-export([config/1, settings/1, read_only/1, replica/1]).

-export_type([settings/0]).

-doc "Kura's own settings for a repo, as configured. See `settings/1`.".
-type settings() :: #{
    dialect => module(),
    pool => atom(),
    pool_module => module(),
    driver_module => module(),
    read_only => boolean(),
    replicas => [module()],
    uuid_version => v4 | v7
}.

-callback otp_app() -> atom().

-doc """
Adjust the repo's config at runtime. Optional.

It runs when kura starts the repo's pool (`kura_db:start_pool/1`, used by
application boot and `kura_repo_worker:start/1`), and on each call to
`config/1`. For a repo in the `{repos, ...}` map or the flat form, pool start
includes kura's own application start. The consuming application has not
started then, so `init/1` must not call processes it owns. One that raises
while the pool is started does not stop kura: the pool is not started, and
only the shape of the reason is logged and returned (see
`kura_db:start_pool/1`).

Queries read the repo's settings (`settings/1`) as they were when the pool
started, so `init/1` does not run per query. For a repo whose pool kura did
not start, they are read through `config/1` on each lookup, `init/1` included,
and a raise there is neither caught nor redacted.
""".
-callback init(Config :: map()) -> map().

-callback migration_apps() -> [atom()].

-optional_callbacks([init/1, migration_apps/0]).

-doc """
Read the repo configuration from application environment, with `init/1`
applied. Each call reads it afresh and runs `init/1`.
""".
-spec config(module()) -> map().
config(RepoMod) ->
    Config = read_config(RepoMod),
    case erlang:function_exported(RepoMod, init, 1) of
        true -> RepoMod:init(Config);
        false -> Config
    end.

-doc """
Kura's own settings for the repo: `dialect`, `pool`, `pool_module`,
`driver_module`, `read_only`, `replicas` and `uuid_version`, those it has.
Connection settings and secrets are not among them.

Once `kura_db:start_pool/1` has started the repo's pool, these are the
settings it was started with, read without calling `init/1`; starting the
pool again re-reads them. For a repo whose pool kura has not started, they
come from `config/1` on each call. A repo refused a pool that another repo
owns raises `{pool_owned_by, Owner}` until its own pool starts. A repo whose
config moved it to a pool that then failed to start raises
`{pool_not_started, Pool}` until a start succeeds.
""".
-spec settings(module()) -> settings().
settings(RepoMod) ->
    case kura_repo_settings:lookup(RepoMod) of
        #{} = Settings -> Settings;
        {pool_owned_by, Owner} -> error({pool_owned_by, Owner});
        {pool_not_started, Pool} -> error({pool_not_started, Pool});
        none -> kura_repo_settings:from_config(config(RepoMod))
    end.

-doc """
Return `true` if the repo is configured `read_only => true`.

A read-only repo rejects writes (insert/update/delete and the bulk and
soft-delete variants return `{error, read_only}`). Reads are unaffected.
Intended for a replica repo pointed at a read replica.
""".
-spec read_only(module()) -> boolean().
read_only(RepoMod) ->
    maps:get(read_only, settings(RepoMod), false) =:= true.

-doc """
Pick a replica repo for `RepoMod` from its `replicas => [Repo]` config.

Returns a randomly-chosen replica, or `RepoMod` itself when none are
configured. Routing is explicit: the caller runs reads through the
returned repo (`(kura_repo:replica(MyRepo)):all(Q)`), so the caller owns
read-after-write consistency. There is no automatic write/read split.
""".
-spec replica(module()) -> module().
replica(RepoMod) ->
    case maps:get(replicas, settings(RepoMod), []) of
        [] -> RepoMod;
        [Only] -> Only;
        Replicas when is_list(Replicas) -> lists:nth(rand:uniform(length(Replicas)), Replicas)
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
        repos,
        backend,
        ensure_database,
        migration_pool_ready_timeout,
        migration_pool_ready_interval,
        pg_types
    ],
    All = application:get_all_env(kura),
    Filtered = [{K, V} || {K, V} <- All, not lists:member(K, Excluded)],
    maps:from_list(Filtered).
