# Migrating to kura 2.0

`kura` 2.0 splits the Postgres backend into a separate package so the
core can host other backends (SQLite, MySQL, ...). Apps that used 1.x
need a one-line config change and a new dependency.

## 1. Add a backend dependency

Pick the backend you use and add it to `rebar.config`:

```erlang
{deps, [
    {kura, "~> 2.0"},
    {kura_postgres, "~> 0.1"}    %% or kura_sqlite
]}.
```

`pgo` is no longer a transitive dependency of `kura` itself.

## 2. Configure the dialect

Set the dialect once on app start (or in `sys.config`):

```erlang
application:set_env(kura, dialect, kura_dialect_pg).   %% or kura_dialect_sqlite
```

Previously this defaulted to `kura_dialect_pg`. It now errors if unset.

## 3. Module renames

The Postgres modules moved to `kura_postgres`. Public API names are
unchanged — only the OTP application providing them differs.

| 1.x location           | 2.0 location                             |
| ---------------------- | ---------------------------------------- |
| `kura/kura_pool_pgo`   | `kura_postgres/kura_pool_pgo`            |
| `kura/kura_driver_pgo` | `kura_postgres/kura_driver_pgo`          |
| `kura/kura_dialect_pg` | `kura_postgres/kura_dialect_pg`          |

If you referenced these modules directly, the references keep working
once `kura_postgres` is on the path.
