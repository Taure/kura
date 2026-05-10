# Migrating to kura 2.x

kura 2.x splits the Postgres backend into a separate package so the core
can host other backends (SQLite, MySQL, ...). Apps that used 1.x need a
new dependency and one config change.

## 1. Add a backend dependency

Pick the backend you use and add it to `rebar.config`:

```erlang
{deps, [
    {kura, "~> 2.4"},
    {kura_postgres, "~> 0.4"}    %% or kura_sqlite, "~> 0.2"
]}.
```

`pgo` is no longer a transitive dependency of `kura` itself.

## 2. Point kura at the backend

Set the backend in `sys.config`. kura's app start populates `dialect`,
`pool_module`, and `driver_module` from the aggregator automatically.

```erlang
%% Postgres
[{kura, [
    {repo, my_repo},
    {backend, kura_backend_postgres},
    {host, "localhost"},
    {port, 5432},
    {database, "my_app"},
    {user, "postgres"},
    {password, "postgres"},
    {pool_size, 10}
]}].
```

```erlang
%% SQLite
[{kura, [
    {repo, my_repo},
    {backend, kura_backend_sqlite},
    {database, <<"my_app.db">>},
    {pool_size, 4}
]}].
```

If you prefer per-key wiring instead of `{backend, ...}`, set
`pool_module`, `driver_module`, and `dialect` explicitly. They override
the aggregator defaults when present.

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
