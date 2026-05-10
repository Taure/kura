# Migrating to kura 2.x

kura 2.x splits the Postgres backend into a separate package so the core
can host other backends (SQLite, MySQL, ...). Apps that used 1.x need a
new dependency and one config change.

## 1. Add a backend dependency

Pick the backend you use and add it to `rebar.config`:

```erlang
{deps, [
    {kura, "~> 2.0"},
    {kura_postgres, "~> 0.4"}    %% or kura_sqlite, "~> 0.2"
]}.
```

`pgo` is no longer a transitive dependency of `kura` itself.

## 2. Point kura at the backend

Configure repos in a `{repos, #{Name => Cfg}}` map under the kura app
env. kura's app start populates `dialect`, `pool_module`, and
`driver_module` from each repo's `backend` key automatically.

```erlang
%% single Postgres repo
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            port => 5432,
            database => "my_app",
            user => "postgres",
            password => "postgres",
            pool_size => 10
        }
    }}
]}].
```

```erlang
%% single SQLite repo
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_sqlite,
            database => <<"my_app.db">>,
            pool_size => 4
        }
    }}
]}].
```

```erlang
%% Postgres primary + SQLite analytics
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            database => "my_app",
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
cache is keyed per repo so they never share entries.

If you prefer per-key wiring instead of `{backend, ...}`, set
`pool_module`, `driver_module`, and `dialect` explicitly inside the
repo's map. They override the aggregator defaults when present.

The 1.x flat form (`{repo, _}/{host, _}/{user, _}` directly at the
kura env level) and the per-app form (`application:get_env(OtpApp,
RepoModule)`) still work as fallbacks. New projects should prefer the
map form.

## 3. Module locations

Backend modules live in their backend package. Public API names are
unchanged — only the OTP application providing them differs.

| 1.x location           | 2.x location                             |
| ---------------------- | ---------------------------------------- |
| `kura/kura_pool_pgo`   | `kura_postgres/kura_pool_pgo`            |
| `kura/kura_driver_pgo` | `kura_postgres/kura_driver_pgo`          |
| `kura/kura_dialect_pg` | `kura/kura_dialect_pg` (kept in core)    |

`kura_dialect_pg` is the default ANSI-ish SQL emitter. It lives in kura
core so non-PG dialects (kura_dialect_sqlite, ...) can delegate to it
without forcing a kura_postgres install.
