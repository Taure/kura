# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
