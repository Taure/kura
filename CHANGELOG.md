# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [2.0.0] - 2026-05-10

The 2.0 line introduces pluggable backends and multi-repo support. The
intermediate 2.1-2.5 dev tags were rolled into this release; nothing
shipped to Hex during that window. See
[MIGRATION-2.0.md](MIGRATION-2.0.md) for upgrade notes.

### Added

- **Pluggable backends.** Pick a backend package and point kura at it
  with `{backend, ...}`. Kura auto-populates `dialect`, `pool_module`,
  and `driver_module` from the aggregator at app start.
  - `kura_pool` behaviour - connection-pool contract.
  - `kura_capabilities` behaviour - backend feature flags
    (e.g. `advisory_locks`, `returning`, `arrays`).
  - `kura_dialect` behaviour - SQL dialect contract, with optional
    `column_type/1` and `format_default/1` callbacks for
    backend-specific DDL.
  - `kura_driver` behaviour - query/transaction driver contract.
  - `kura_pool_ets` - in-memory pool for tests that don't need a real DB.
- **Multi-repo configuration.** Run two or more repos in the same app,
  each with its own backend, dialect, and pool. Configure with a map of
  maps under `{repos, ...}`:

  ```erlang
  {kura, [
      {repos, #{
          primary => #{
              backend => kura_backend_postgres,
              host => "primary.example.com",
              database => "main",
              pool_size => 10
          },
          analytics => #{
              backend => kura_backend_sqlite,
              database => <<":memory:">>
          }
      }}
  ]}
  ```

  Kura starts each repo's pool on app start. The dialect resolves
  per-repo, so `primary` emits Postgres SQL while `analytics` emits
  SQLite SQL. The query cache is keyed per repo so dialects never share
  entries.
- `kura_app:pool_config/1` returns the config map for a specific repo.
- `kura_types:cast(boolean, 0|1)` so SQLite (booleans stored as INTEGER)
  round-trips cleanly.

### Changed

- `kura_query_compiler:to_sql/2`, `to_sql_cached/2`, `to_sql_from/3`,
  `insert/4`, `insert/5`, `update/5`, `delete/4`, `update_all/3`,
  `delete_all/2`, `insert_all/4`, `insert_all/5`, and `dialect/1`
  take `RepoMod` as the first argument so they can resolve the
  per-repo dialect. Internal callers (`kura_repo_worker`,
  `kura_stream`, `kura_migrator`) thread the repo through automatically.
- `kura_query_cache` keys are tuples `{RepoMod, QueryHash}`. The cache
  ETS table is now owned by a supervised gen_server so it survives any
  caller exiting.
- `kura_db:query/3` and the rest of the query path route through
  `kura_pool` / `kura_driver`, so backends are swappable.
- `kura_app:pool_config/0` is dialect-aware. PG-shaped defaults
  (port 5432, user/database `"postgres"`, decode_opts) only apply when
  `dialect` is `kura_dialect_pg`. Other dialects get a pass-through
  map of user-set kura env keys.
- `kura_app:configure_pg_types/0` only runs when `dialect` is
  `kura_dialect_pg`.
- `kura_repo:read_config/1` returns a pass-through map of all kura env
  keys (minus bookkeeping). Previously hardcoded the PG-shaped subset
  and silently dropped `pool_module`, `driver_module`, `backend`.
- `kura_migrator` skips `pg_advisory_xact_lock` when the configured
  pool doesn't declare the `advisory_locks` capability. The migration
  transaction alone serializes runs on backends that lack advisory
  locks (SQLite, etc.).
- Single-repo legacy config (flat `kura` env keys) and the per-app form
  (`application:get_env(OtpApp, RepoMod)`) keep working unchanged. The
  global `application:get_env(kura, dialect)` is consulted as a fallback
  when a repo's config map omits `dialect`.

### Removed (BREAKING)

- `kura_pool_pgo`, `kura_driver_pgo` no longer ship in `kura`. Install
  [`kura_postgres`](https://github.com/Taure/kura_postgres) for Postgres
  or [`kura_sqlite`](https://github.com/Taure/kura_sqlite) for SQLite.
  `pgo` is no longer a runtime dependency of `kura`. `kura_dialect_pg`
  remains in core as the default ANSI-ish SQL emitter that other
  dialects delegate to.
- `kura_query_compiler:dialect/0` no longer defaults to `kura_dialect_pg`.
  Use `dialect/1` and configure the dialect per repo (recommended via
  `{backend, ...}`), or set the global `application:set_env(kura,
  dialect, Module)` for legacy single-dialect setups.
- `kura_db:get_pool_module/1` and `get_driver_module/1` error with
  `{no_pool_module_configured, _}` / `{no_driver_module_configured, _}`
  when not configured. Set `{backend, ...}` (recommended) or
  `pool_module`/`driver_module` explicitly.

## [1.8.0] - 2026-03-06

### Changed

- **Breaking**: `kura_repo` behaviour now requires `otp_app/0` callback instead of `config/0` - database configuration is read from application environment via `application:get_env(OtpApp, RepoModule)`
- Optional `init/1` callback on `kura_repo` - modify config at runtime (read secrets from env vars, files, external services)
- `kura_repo:config/1` function reads config from app env, then calls `init/1` if defined
- All internal `get_pool/1` calls now use `kura_repo:config/1` instead of `RepoMod:config()`

## [1.7.0] - 2026-03-06

### Added

- `telemetry` library integration - every query emits a `[kura, repo, query]` telemetry event with measurements (`duration`, `duration_us`) and metadata (`query`, `params`, `repo`, `result`, `num_rows`, `source`)
- `build_telemetry_metadata/4` and `extract_source/1` exported from `kura_repo_worker` for testing
- `source` field in telemetry metadata - table name extracted from the SQL query

### Changed

- Legacy `{kura, [{log, ...}]}` config still works but now runs alongside telemetry events (not instead of)

## [1.6.0] - 2026-03-06

### Added

- Ecto-style index support - `indexes/0` optional callback on `kura_schema` for declaring indexes at the schema level
- Map-based `{create_index, Table, Cols, Opts}` migration operation with auto-generated index names (`{table}_{cols}_index`)
- `kura_migration:index_name/2` helper for generating Ecto-style index names
- Unique indexes declared via `indexes/0` auto-register changeset constraints - no manual `unique_constraint/3` calls needed
- New types: `index_def()`, `index_opts_map()` in `kura_migration`

## [1.5.0] - 2026-03-05

### Added

- Table-level constraints in migrations - `{create_table, Table, Columns, Constraints}` 4-tuple variant supporting `{unique, [atom()]}` and `{check, binary()}` inline constraints
- `constraints/0` optional callback on `kura_schema` behaviour - declares table-level constraints on the schema
- Auto-registration of schema constraints on `kura_changeset:cast/4` - duplicate inserts get friendly error messages without manual `unique_constraint/3` calls

## [1.4.0] - 2026-03-05

### Changed

- **Breaking:** Minimum OTP version is now OTP 28 (sigil syntax, ELP fixes)
- Binary string literals converted to sigil syntax (`~"..."`) across changeset modules
- Eliminated unnecessary intermediate list and tuple allocations in association casting

## [1.3.2] - 2026-03-05

### Fixed

- Dialyzer warnings in `get_field/2,3` - `maps:find/2` returns `{ok, V} | error`, not map patterns
- Atom exhaustion risk in enum `load/2` - now validates against declared values list instead of `binary_to_atom`
- Atom exhaustion risk in `kura_repo_worker` error handlers - NOT NULL and constraint errors fall back to `base` atom for unknown columns
- CI running resilience tests that require docker compose control - explicitly list test modules

### Added

- Production readiness test suites: `kura_stress_tests` (7 concurrency tests), `kura_bench_tests` (6 throughput benchmarks), `kura_migration_stress_tests` (4 migration safety tests), `kura_resilience_tests` (4 failure/recovery tests)
- Makefile targets: `test-stress`, `test-bench`, `test-migration-stress`, `test-resilience`, `test-production`

## [1.3.1] - 2026-03-04

### Fixed

- Advisory lock query wrapped to avoid pgo void type decode error (`pg_advisory_xact_lock` returns void)

## [1.3.0] - 2026-03-03

### Changed

- Primary key derived from field flag instead of separate callback

## [1.2.0] - 2026-03-01

### Added

- Changeset validators: `validate_exclusion`, `validate_subset`, `traverse_errors`, `prepare_changes`, `optimistic_lock`
- Query extensions: subqueries in WHERE, CTEs, UNION/INTERSECT/EXCEPT, window functions (`select_expr`), query scopes
- `kura_sandbox` - test transaction rollback for isolated concurrent tests
- `kura_stream` - server-side cursor batching for large result sets
- `insert_all` with RETURNING support

### Changed

- `to_sql_from/2` refactored as keystone for all query compilation

## [1.1.0] - 2026-02-27

### Added

- Foreign key constraints in migrations - `references`, `on_delete`, `on_update` fields on `#kura_column{}` generate inline `REFERENCES "table"("col") ON DELETE CASCADE` etc.
- `kura_changeset:validate_confirmation/2,3` - validates a field has a matching `<field>_confirmation` in params (for password/email confirmation flows)
- `kura_repo_worker:exists/2` - checks if any record matches a query, returns `{ok, true | false}`
- `kura_repo_worker:reload/3` - re-fetches a record from the database by its primary key

## [1.0.4] - 2026-02-26

### Fixed

- ExDoc build failure - `doc/architecture.md` was wiped by edoc before ex_doc could read it; moved to `guides/architecture.md`
- Removed `doc` from hex include_files (generated output, not source)

### Added

- ExDoc build step in CI to catch documentation errors early

## [1.0.3] - 2026-02-26

### Added

- Rolling deployment safety warnings - detect dangerous migration operations (`drop_column`, `rename_column`, `modify_column`, `drop_table`, `add_column NOT NULL` without default) and log warnings before execution
- Optional `safe/0` callback on `kura_migration` behaviour to suppress warnings when the expand-contract pattern has been followed
- Rolling deployments guide (`guides/rolling_deployments.md`) documenting the expand-contract pattern with step-by-step examples

## [1.0.2] - 2026-02-24

### Added

- `kura_preloader` module - extracted preload logic from `kura_repo_worker` into a dedicated module
- `kura_changeset_assoc` module - extracted `cast_assoc`, `put_assoc`, and association coercion from `kura_changeset`
- Comprehensive integration tests (356 total) covering all query operators, aggregates, joins, bulk ops, multi pipelines, on_conflict upserts, constraint errors, type round-trips, and all association/preload types

### Fixed

- Constraint error handling (`unique_constraint`, `foreign_key_constraint`, `check_constraint`) now correctly unwraps pgo `{pgsql_error, Map}` tuples
- `put_assoc` for many_to_many now preserves primary key in changeset data

## [1.0.1] - 2026-02-22

### Added

- Embedded schemas guide (`guides/embedded-schemas.md`)
- Architecture doc linked in hexdocs extras
- Logo displayed in hexdocs
- Navigation groups: Getting Started, Guides, Reference

### Changed

- Hidden internal modules (`kura_app`, `kura_sup`) from docs

## [1.0.0] - 2026-02-22

### Changed

- Consolidated CI and release into a single GitHub Actions workflow
- Updated README features list to reflect all capabilities added since 0.2.0
- Fixed stale `log_queries` config reference in README (now `log`)
- Fixed stale migration discovery docs in README (module-based since 0.3.0)

## [0.5.0] - 2026-02-14

### Added

- Embedded schemas (`embeds_one`, `embeds_many`) stored as JSONB with `cast_embed/2,3`
- Many-to-many associations with `join_through` / `join_keys`, preloading, and persistence via `cast_assoc` / `put_assoc`
- Schemaless changesets - `cast/4` accepts a types map for validation-only workflows
- `Makefile` for local test lifecycle management (`make test` with Docker Compose)
- Schemaless changesets guide (`guides/schemaless-changesets.md`)

## [0.4.1] - 2026-02-14

### Added

- Hex documentation guides for enums, telemetry, and cast_assoc

## [0.4.0] - 2026-02-14

### Added

- Enum type (`{enum, [atom()]}`) - stored as `VARCHAR(255)`, cast/dump/load between atoms and binaries
- Query telemetry via `sys.config` - `{kura, [{log, true | {M,F}}]}` with timing on every query
- `kura_changeset:cast_assoc/2,3` - nested changeset casting for `has_many` and `has_one` associations
- `kura_changeset:put_assoc/3` - directly attach maps or pre-built changesets as association changes
- `assoc_changes` field on `#kura_changeset{}` record
- Nested insert/update - automatic transaction wrapping with FK propagation when `assoc_changes` present
- `kura_repo_worker:build_log_event/5` and `default_logger/0` exported for testability
- Architecture documentation (`guides/architecture.md`)
- Feature guides: enums, telemetry, cast_assoc (`guides/`)

### Changed

- Telemetry reads from `application:get_env(kura, log)` instead of repo `config/0` map
- Removed `log_queries` application env default (replaced by `log`)
- `kura_repo_worker:insert/2` and `update/2` refactored to support nested association persistence

### Removed

- `maybe_log/2` internal function (replaced by centralized `emit_log/5`)

## [0.3.0] - 2026-02-14

### Added

- Module-based migration discovery (no filesystem scanning)
- Xref checks in CI
- rebar3_kura plugin reference in README

### Removed

- `migration_paths` configuration (migrations now discovered automatically from compiled modules)

## [0.2.0] - 2025-02-14

### Added

- Constraint declarations (`unique_constraint`, `foreign_key_constraint`, `check_constraint`) with automatic PostgreSQL error mapping
- `kura_multi` for atomic transaction pipelines with insert/update/delete/run steps
- Association support (`belongs_to`, `has_one`, `has_many`) with preloading
- Nested preloads
- Standalone preloading via `kura_repo_worker:preload/4`
- Query preloads via `kura_query:preload/2`
- Upsert support (`insert/3` with `on_conflict` option)
- Bulk operations: `insert_all/3`, `update_all/3`, `delete_all/2`
- `kura_changeset:validate_change/3` for custom validation functions
- `kura_changeset:apply_action/2`
- Module documentation (`-moduledoc` / `-doc`) for all public modules
- Usage guides: Getting Started, Changesets, Query Builder, Associations, Multi, Migrations
- CI/CD with GitHub Actions
- CHANGELOG

## [0.1.0] - 2025-01-15

### Added

- `kura_schema` behaviour for defining database-backed schemas
- `kura_changeset` for casting, validation, and change tracking
- `kura_query` composable query builder
- `kura_query_compiler` for parameterized SQL generation
- `kura_repo` behaviour and `kura_repo_worker` for CRUD operations
- `kura_types` type system (id, integer, float, string, text, boolean, date, utc_datetime, uuid, jsonb, arrays)
- `kura_migration` behaviour and `kura_migrator` for DDL migrations
- Automatic timestamps (`inserted_at`, `updated_at`)
- Query logging via application env
