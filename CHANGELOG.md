# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-27

### Added

- Foreign key constraints in migrations — `references`, `on_delete`, `on_update` fields on `#kura_column{}` generate inline `REFERENCES "table"("col") ON DELETE CASCADE` etc.
- `kura_changeset:validate_confirmation/2,3` — validates a field has a matching `<field>_confirmation` in params (for password/email confirmation flows)
- `kura_repo_worker:exists/2` — checks if any record matches a query, returns `{ok, true | false}`
- `kura_repo_worker:reload/3` — re-fetches a record from the database by its primary key

## [1.0.4] - 2026-02-26

### Fixed

- ExDoc build failure — `doc/architecture.md` was wiped by edoc before ex_doc could read it; moved to `guides/architecture.md`
- Removed `doc` from hex include_files (generated output, not source)

### Added

- ExDoc build step in CI to catch documentation errors early

## [1.0.3] - 2026-02-26

### Added

- Rolling deployment safety warnings — detect dangerous migration operations (`drop_column`, `rename_column`, `modify_column`, `drop_table`, `add_column NOT NULL` without default) and log warnings before execution
- Optional `safe/0` callback on `kura_migration` behaviour to suppress warnings when the expand-contract pattern has been followed
- Rolling deployments guide (`guides/rolling_deployments.md`) documenting the expand-contract pattern with step-by-step examples

## [1.0.2] - 2026-02-24

### Added

- `kura_preloader` module — extracted preload logic from `kura_repo_worker` into a dedicated module
- `kura_changeset_assoc` module — extracted `cast_assoc`, `put_assoc`, and association coercion from `kura_changeset`
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
- Schemaless changesets — `cast/4` accepts a types map for validation-only workflows
- `Makefile` for local test lifecycle management (`make test` with Docker Compose)
- Schemaless changesets guide (`guides/schemaless-changesets.md`)

## [0.4.1] - 2026-02-14

### Added

- Hex documentation guides for enums, telemetry, and cast_assoc

## [0.4.0] - 2026-02-14

### Added

- Enum type (`{enum, [atom()]}`) — stored as `VARCHAR(255)`, cast/dump/load between atoms and binaries
- Query telemetry via `sys.config` — `{kura, [{log, true | {M,F}}]}` with timing on every query
- `kura_changeset:cast_assoc/2,3` — nested changeset casting for `has_many` and `has_one` associations
- `kura_changeset:put_assoc/3` — directly attach maps or pre-built changesets as association changes
- `assoc_changes` field on `#kura_changeset{}` record
- Nested insert/update — automatic transaction wrapping with FK propagation when `assoc_changes` present
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
