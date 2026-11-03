# Kura

<p align="center">
  <img src="priv/kura.png" alt="Kura" width="600" />
</p>

Database layer for Erlang - Ecto-equivalent abstractions in pure Erlang. Pluggable backends: [`kura_postgres`](https://github.com/Taure/kura_postgres) (PostgreSQL via minato), [`kura_sqlite`](https://github.com/Taure/kura_sqlite) (SQLite via esqlite).

## Features

- **Schema** - behaviour-based schema definitions with type metadata
- **Changeset** - cast external params, validate, track changes and errors
- **Query Builder** - composable, functional query construction
- **SQL Compiler** - parameterized SQL generation (no string interpolation)
- **Repo** - CRUD operations with automatic type conversion and PG error mapping
- **Associations** - `belongs_to`, `has_one`, `has_many`, `many_to_many` with preloading
- **Composite keys** - N-column primary keys (`key/0`) and composite foreign keys (`#kura_ref{}`)
- **Embedded Schemas** - `embeds_one`, `embeds_many` stored as JSONB
- **Multi** - atomic transaction pipelines
- **Migrations** - DDL operations with automatic module-based discovery
- **Enums** - atom-backed enum types stored as `VARCHAR`
- **Telemetry** - query logging with timing
- **Lifecycle Hooks** - before/after callbacks for insert, update, delete
- **Audit Trail** - automatic change tracking with actor context
- **Pagination** - offset-based and cursor-based pagination
- **Streaming** - server-side cursor streaming for large result sets
- **Multitenancy** - schema prefix and attribute-based tenant isolation
- **Optimistic Locking** - concurrent update conflict detection
- **Encryption at rest** - `{encrypted, Type}` fields, AES-256-GCM, key rotation, deterministic equality lookups

## Quick Start

### Define a Schema

```erlang
-module(user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).

table() -> ~"users".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = email, type = string, nullable = false},
        #kura_field{name = age, type = integer},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].
```

### Define a Repo

```erlang
-module(my_repo).
-behaviour(kura_repo).

-export([otp_app/0, start/0, all/1, get/2, insert/1, update/1, delete/1]).

otp_app() -> my_app.

start() -> kura_repo_worker:start(?MODULE).
all(Q) -> kura_repo_worker:all(?MODULE, Q).
get(Schema, Id) -> kura_repo_worker:get(?MODULE, Schema, Id).
insert(CS) -> kura_repo_worker:insert(?MODULE, CS).
update(CS) -> kura_repo_worker:update(?MODULE, CS).
delete(CS) -> kura_repo_worker:delete(?MODULE, CS).
```

Configure the database connection in `sys.config`:

```erlang
[{my_app, [
    {my_repo, #{
        database => ~"myapp",
        hostname => ~"localhost",
        port => 5432,
        username => ~"postgres",
        password => <<>>,
        pool_size => 10
    }}
]}].
```

### Changesets

```erlang
%% Cast and validate external params
CS = kura_changeset:cast(user, #{}, #{~"name" => ~"Alice", ~"email" => ~"alice@example.com"}, [name, email, age]),
CS1 = kura_changeset:validate_required(CS, [name, email]),
CS2 = kura_changeset:validate_format(CS1, email, ~"@"),
CS3 = kura_changeset:validate_length(CS2, name, [{min, 1}, {max, 100}]),

%% Insert
{ok, User} = my_repo:insert(CS3).
```

### Query Builder

```erlang
Q = kura_query:from(user),
Q1 = kura_query:where(Q, {age, '>', 18}),
Q2 = kura_query:where(Q1, {'or', [{role, ~"admin"}, {role, ~"moderator"}]}),
Q3 = kura_query:select(Q2, [name, email]),
Q4 = kura_query:order_by(Q3, [{name, asc}]),
Q5 = kura_query:limit(Q4, 10),

{ok, Users} = my_repo:all(Q5).
```

Supported conditions: `=`, `!=`, `<`, `>`, `<=`, `>=`, `like`, `ilike`, `matches`, `in`, `not_in`, `is_nil`, `is_not_nil`, `between`, `{'and', [...]}`, `{'or', [...]}`, `{'not', ...}`, `{fragment, SQL, Params}`.

#### Full-text search

`{Field, matches, Query}` is a full-text search: on PostgreSQL it compiles
to `to_tsvector(Field) @@ plainto_tsquery($n)`, with the query text bound
as a parameter (`plainto_tsquery` treats it as plain words, so no query
syntax is needed or trusted). Requires a backend declaring the
`full_text_search` capability.

```erlang
Q = kura_query:where(kura_query:from(article), {body, matches, ~"erlang database"}),
{ok, Hits} = my_repo:all(Q).
```

Matching uses the server's `default_text_search_config` for stemming and
stop-words. For production, add a GIN expression index so the search
doesn't recompute a `tsvector` per row:

```sql
CREATE INDEX articles_body_fts ON articles USING gin (to_tsvector('english', body));
```

#### Window functions

Use `over/2` inside `select_expr/2` for `OVER (PARTITION BY ... ORDER BY ...)`
expressions. The window function is an aggregate (`{count, '*'}`, `{sum, Field}`,
`{avg, Field}`, `{min, Field}`, `{max, Field}`) or a ranking function
(`row_number`, `rank`, `dense_rank`).

```erlang
Q = kura_query:select_expr(kura_query:from(sales), [
    {category, category},
    {row_num, kura_query:over(row_number, #{partition_by => [category], order_by => [{amount, desc}]})},
    {running_total, kura_query:over({sum, amount}, #{partition_by => [category], order_by => [{day, asc}]})}
]),

{ok, Rows} = my_repo:all(Q).
```

Requires a backend declaring the `window_functions` capability — PostgreSQL today (SQLite window rendering is planned).

### Migrations

```erlang
-module(m20240115120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"users", [
        #kura_column{name = id, type = id, primary_key = true, nullable = false},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false},
        #kura_column{name = age, type = integer},
        #kura_column{name = inserted_at, type = utc_datetime},
        #kura_column{name = updated_at, type = utc_datetime}
    ]},
    {create_index, ~"users", [email], #{unique => true}}].

down() ->
    [{drop_index, ~"users_email_index"},
     {drop_table, ~"users"}].
```

Run migrations:

```erlang
kura_migrator:migrate(my_repo).
kura_migrator:rollback(my_repo).
kura_migrator:status(my_repo).
```

## Type Mapping

| Kura | PostgreSQL | SQLite | Erlang |
|---|---|---|---|
| `id` | `BIGSERIAL` | `INTEGER PRIMARY KEY` | `integer()` |
| `integer` | `INTEGER` | `INTEGER` | `integer()` |
| `float` | `DOUBLE PRECISION` | `REAL` | `float()` |
| `string` | `VARCHAR(255)` | `TEXT` | `binary()` |
| `text` | `TEXT` | `TEXT` | `binary()` |
| `boolean` | `BOOLEAN` | `INTEGER` (0/1) | `boolean()` |
| `date` | `DATE` | `TEXT` (ISO 8601) | `{Y, M, D}` |
| `utc_datetime` | `TIMESTAMPTZ` | `TEXT` (ISO 8601) | `calendar:datetime()` |
| `uuid` | `UUID` | `TEXT` | `binary()` |
| `jsonb` | `JSONB` | `TEXT` | `map()` |
| `{array, T}` | `T[]` | unsupported | `list()` |
| `{encrypted, T}` | `BYTEA` | `BLOB` | `T` (encrypted at rest) |
| `{encrypted, T, Opts}` | `BYTEA` | see below | `T` (encrypted at rest) |
| `{vector, N}` | `VECTOR(N)` | unsupported | `[float()]` |

SQLite values round-trip transparently via `kura_types:cast/2` (booleans 0/1 → `true`/`false`, ISO 8601 → datetime tuples, JSON text → maps).

`{vector, N}` (and dimensionless `vector`) map to [pgvector](https://github.com/pgvector/pgvector)
columns for embeddings; requires `CREATE EXTENSION vector` and a backend
declaring the `vector` capability. A value is a list of numbers, dimension-checked
on cast. `kura_query:distance/3` orders and selects by `<->`, `<=>` or `<#>`
distance; see the vector search section of the query builder guide.
Elements are stored as `float4`, so a value read back from a column is the
single-precision approximation of what was written, not the exact double.

## Encryption at rest

Wrap a field type in `{encrypted, Type}` to store it AES-256-GCM-encrypted
in a `BYTEA` column. Values are encrypted on write and decrypted on read;
casting and validation still run on the plaintext.

```erlang
fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = string},
        #kura_field{name = ssn, type = {encrypted, string}, nullable = false}
    ].
```

Supported inner types: `string`, `text`, `binary`, `uuid`, `jsonb`,
`integer`, `bigint`, `smallint`, `boolean`. Configure keys under the
`kura` app env - each a base64-encoded 32-byte key, with `active`
selecting the one used for new writes:

```erlang
[{kura, [
    {encryption, #{
        active => 1,
        keys => [{1, <<"base64-encoded-32-byte-key">>}]
    }}
]}].
```

**Rotation:** add a new key and move `active` to it. Each ciphertext
carries its key id, so existing rows keep decrypting under their original
key - no backfill. A custom keyring (KMS/Vault) can replace the default by
implementing the `kura_keyring` behaviour and setting `{kura, [{keyring, my_keyring}]}`.

**Frame versions and column binding:** every ciphertext starts with a
version byte. By default reads accept every version, so stored data never
needs a backfill to stay readable.

| Version | Layout | Bound to |
|---|---|---|
| v1 | `<<1, KeyId:8, Nonce:12, Tag:16, CT>>` | nothing: encrypted under the root key |
| v2 | `<<2, Mode:8, KeyId:16, IV:12, Tag:16, CT>>` | its `{Table, Column}` |

A v2 ciphertext is encrypted under a per-column subkey derived with
HKDF-SHA256 from the root key and the column context `{Table, Column}`
(the schema's unprefixed `table()` and the column name), and the context,
mode and key id are authenticated as AAD. A v2 value copied by a
database-level writer into another encrypted column or table fails to
decrypt (`{kura_crypto, {bad_tag, KeyId}}`) instead of being shown as that
column's value. The binding uses the table and column name, so renaming
either makes the stored v2 values undecryptable, unless the field pins its
`context` (see Deterministic encryption). Mode is `0` (randomized) or `1`
(deterministic).

What v2 does not cover yet:

- **v1 is still accepted everywhere.** Every encrypted column accepts a v1
  frame on read, including a column that only ever had v2 written to it,
  and a v1 frame is bound to nothing. A database-level writer can copy a
  v1 ciphertext (from a backup, or from a column not yet rewritten) into
  any encrypted column and it decrypts there. Column binding is complete
  only once v1 reads are refused, which no release does yet.
- **Rows and tenants are not bound**, in either tenancy mode (schema
  prefix or tenant attribute). A writer with database access can swap
  ciphertexts of the same column between rows or tenants, and reorder or
  splice the elements of an encrypted array within the column.
- **Only the repo read path decrypts v2**: rows loaded for a schema
  (`kura_db:load_row/2`, which every repo read, stream and preload uses).
  An encrypted column of a joined schema is loaded by the `from` schema's
  types, so it comes back undecrypted. A join without a `select` reads
  `SELECT *`, so when a joined column shares its name with an encrypted
  field of the `from` schema, the joined value may be read under that
  field: it raises for a v2 value; a v1 value decrypts and is returned
  under the `from` field. Use an explicit `select` on joins.

**Write format:** kura writes v1 by default and reads both. Opt in to v2
with:

```erlang
[{kura, [{encryption_write_format, v2}]}].
```

Any other value fails `kura` application start. Turn it on only once every
node runs a kura that reads v2, since an older node cannot read a v2 value
(see the rolling deployments guide). A later release makes v2 the default.
An existing v1 value stays v1 until that field is written again; there is
no in-place re-encryption yet.

**Constraints:** randomized encrypted columns are **not queryable** and
cannot carry a unique constraint (each write has a random nonce, so equal
plaintexts produce different ciphertexts). Deterministic columns can be
looked up by equality (below). `kura_query:where/2` and `having/2` raise
on any condition that compares an encrypted field to a value, however it is
nested, including inside a subquery or over a CTE that selects an encrypted
field; `{Field, is_nil}` and `{Field, is_not_nil}` still work. Ordering,
grouping, `DISTINCT ON` or joining on an encrypted field (or on a
`select_expr/2` alias of one), partitioning or ordering a window by one,
and `sum`, `avg`, `min` or `max` of one raise too; `count` works. A raw
`{fragment, SQL, Params}` is not inspected. The check runs again when a
query is compiled, so a `#kura_query{}` record built by hand is covered
too. Primary keys, association keys and fields of embedded schemas cannot
be encrypted: `kura_schema` raises when it first reads such a schema. An
`{array, {encrypted, Inner}}` field encrypts each element, is stored as
`BYTEA[]`, and is an encrypted field for all of the above. `NULL` stays
`NULL` (unencrypted), so presence still leaks.
Schema introspection (`rebar3 kura gen_schemas`) cannot detect that a
`BYTEA` column was encrypted - annotate it by hand. The audit trail
redacts encrypted fields to `[encrypted]` (it keeps the "changed" signal
but never logs the value).

### Options and deterministic encryption

`{encrypted, Inner, Opts}` takes a closed set of options, checked when
the schema is first read (an unknown key or a bad value raises
`{kura, {unknown_encryption_option | invalid_encryption_option, Mod, Field, Key}}`):

| Option | Values | Default |
|---|---|---|
| `mode` | `randomized`, `deterministic` | `randomized` |
| `normalize` | `none`, `casefold`, `nfc`, `nfc_casefold` (deterministic `string`/`text` only) | `none` |
| `context` | `{Table, Column}`, binaries | the schema's `table()` and the column name |
| `legacy` | `accept`, `reject` | `reject` |

`casefold` is Unicode full case folding (`string:casefold/1`) without the
Turkic mappings, so dotless `ı` and `I` fold to different values.
`nfc_casefold` decomposes, folds and recomposes, so canonically equivalent
forms of a value (precomposed or not) compare equal.

The options form is always written as a v2 frame, whatever
`encryption_write_format` says. With `legacy => reject` a column refuses a
v1 frame (`{kura_crypto, {refused_version, 1}}`) and a v2 frame of the
other mode (`{kura_crypto, {refused_mode, Mode}}`); `accept` reads both,
which is how a column moves between modes. Moving a column is complete only
once every row has been rewritten: until a reencrypt tool rewrites them, an
old randomized row never matches a lookup, `!=` and `not_in` return it, and
a unique index does not cover it. `{encrypted, Inner}` keeps its
behaviour: randomized, `legacy => accept`, and the write format decides
the frame.

A **deterministic** field encrypts a value with a synthetic IV
(HMAC-SHA-256 over the value and the column, then AES-256-GCM, with keys
derived per column), so equal values of one column encrypt to equal
ciphertexts under one key:

```erlang
#kura_field{name = email,
            type = {encrypted, string, #{mode => deterministic, normalize => casefold}}}

kura_query:where(kura_query:from(user), {email, ~"Ada@Example.com"}).
```

`where/2` and `having/2` cast and normalise the value as a write would,
encrypt it under every lookup key and replace the condition with
`{email, encrypted_in, Ciphertexts}` before the query is stored, so the
plaintext never reaches the query, the cache, telemetry or logs. `=`,
`in`, `!=` and `not_in` work, as do `is_nil` and `is_not_nil`; every
other operator, ordering, grouping and aggregates still raise. A `unique`
index works, and `normalize => casefold` makes it case-insensitive. A
`uuid` is compared in its lowercase form. For a `{fragment, SQL, Params}`
use `kura_encryption:lookup_params(Schema, Field, Value)`. Inner types:
`string`, `text`, `binary`, `uuid`, `integer`, `bigint`, `smallint`
(`boolean` and `jsonb` are refused), and not in arrays.

Deterministic writes use a separate key, `deterministic_active`, and fail
closed without one (`{kura_crypto, no_deterministic_key}`):

```erlang
{encryption, #{active => 1, deterministic_active => 2, keys => [...]}}
```

A lookup is encrypted under `deterministic_active` and every id in
`deterministic_keys` (default `[]`; at most 8 ids in all, beyond which a
lookup raises `{kura_crypto, {too_many_lookup_keys, N}}`). An `in` of N
values binds N x K ciphertexts for K lookup keys, so PostgreSQL's limit of
65535 parameters is reached at N > 65535 / K. Keys used only
by `active` never enter a lookup. To rotate a deterministic key, add the
new key to `keys`, set `deterministic_active` to it and keep the old id in
`deterministic_keys` until no stored value is encrypted under it, then drop
it:

```erlang
{encryption, #{active => 1, deterministic_active => 3, deterministic_keys => [2],
               keys => [...]}}
```

`!=` and `not_in` are correct only while every key a stored value is
encrypted under is in `deterministic_keys` or is `deterministic_active`: a
row under any other key never equals the excluded value, so it is returned.

What deterministic mode gives away, compared with randomized:

| | Randomized | Deterministic |
|---|---|---|
| Which rows hold equal values | hidden | **revealed** (frequency analysis) |
| Equal values across tenants | hidden | **revealed** in both tenancy modes: the context is the unprefixed table |
| A looked-up value in telemetry and logged query parameters | never bound | its ciphertext, a stable pseudonym |
| Length of a value, and whether it is `NULL` | revealed | revealed |
| Personal data under the GDPR | yes | yes (pseudonymised, not anonymised) |
| Unique index | never violated | works within one key generation |

Use it only where an equality lookup or uniqueness is needed. Keep a
deterministic column under 2^32 distinct values per key. The same value
written under two deterministic keys is two ciphertexts, so a unique index
does not catch a duplicate across a key rotation until the old rows are
rewritten. Renaming the table or column without pinning `context` makes the
stored values undecryptable. A shared `context` gives two columns equal
ciphertexts for equal values, which raw SQL can compare (`join/5` still
refuses an encrypted column), at the cost of a value being movable between
them. On SQLite, `{encrypted, Inner, Opts}` fields are unsupported until a
kura_sqlite release after 1.1.0, which reads the options form through
`kura_types:encryption/1` and binds the `encrypted_in` condition as bytes.

## Configuration

Configure repos under the `kura` app env. Each repo is a map keyed by
its module name; pick a backend package and Kura starts the configured
pool at app boot, populating `dialect`, `pool_module`, and `driver_module`
from the aggregator automatically.

```erlang
%% sys.config — single Postgres repo
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            port => 5432,
            database => "my_app_dev",
            user => "postgres",
            password => "postgres",
            pool_size => 10
        }
    }}
]}].
```

```erlang
%% sys.config — single SQLite repo
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_sqlite,
            database => <<"my_app.db">>,   %% or <<":memory:">>
            pool_size => 4
        }
    }}
]}].
```

```erlang
%% sys.config — Postgres primary + SQLite analytics
[{kura, [
    {repos, #{
        my_repo => #{
            backend => kura_backend_postgres,
            host => "localhost",
            database => "main",
            user => "postgres",
            pool_size => 10
        },
        analytics_repo => #{
            backend => kura_backend_sqlite,
            database => <<":memory:">>
        }
    }}
]}].
```

Each repo module declares itself in code:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0]).
otp_app() -> my_app.
```

Queries through `my_repo` emit Postgres SQL; queries through
`analytics_repo` emit SQLite SQL. The query cache is keyed on
`{Repo, Tenant, Shape}` so neither dialects nor tenants share entries.
The shape is the query with its bound values taken out, so
`get(User, Id)` holds one entry however many ids it is called with. An
`in` list is one shape per list length. Two settings bound the table,
both counted in words:

| Setting | Default | Effect |
| --- | --- | --- |
| `query_cache_max_memory` | `8000000` (~64 MB) | cache is dropped wholesale once the table exceeds this |
| `query_cache_max_entry_size` | `4096` | a single entry larger than this is never cached |

Lower `query_cache_max_entry_size` if callers build `in` filters from
user-supplied lists and you want them kept out of the cache entirely.
`kura_query_cache:flush/0` drops every entry; call it if you swap a
repo's dialect at runtime, since the dialect is not part of the key.

UUID primary keys are auto-generated on insert when no value is
provided. The default is **UUIDv4** (random) - secure by default, since
a v4 key discloses nothing about the row. Set `uuid_version => v7` in a
repo's config to use time-ordered UUIDv7 for better index locality; note
that a v7 key embeds its creation timestamp, so avoid it for keys exposed
in URLs or public APIs. A schema's own `generate_id/0` callback still
takes precedence over both.

### Read replicas

A replica is just another repo pointed at a read replica. Mark it
`read_only => true` so writes are refused, and list it under the
primary's `replicas` so the primary can hand it out:

```erlang
{repos, #{
    my_repo => #{backend => kura_backend_postgres, host => "primary", replicas => [my_repo_ro]},
    my_repo_ro => #{backend => kura_backend_postgres, host => "replica", read_only => true}
}}
```

`read_only => true` makes every write (insert/update/delete and the
bulk and soft-delete variants) return `{error, read_only}`. Routing is
explicit - pick a replica per read and own read-after-write yourself:

```erlang
Replica = kura_repo:replica(my_repo),   %% a replica repo, or my_repo if none
{ok, Rows} = Replica:all(Q).
```

There is no automatic write/read split - reads go to whichever repo you
call, so a read that must see a just-written row should use the primary.
The guard covers the changeset write API; raw SQL via `query/3` is not
guarded, so a raw write on a replica is rejected by PostgreSQL itself.

`read_only` and `replicas` are read when the repo's pool starts. Changing
them at runtime takes effect after `kura_repo_worker:start/1` is called
again for that repo.

<details>
<summary>Legacy v1.x config forms (still supported)</summary>

The flat single-repo form:

```erlang
[{kura, [
    {repo, my_repo},
    {backend, kura_backend_postgres},
    {host, "localhost"},
    {port, 5432},
    {database, "my_app_dev"},
    {user, "postgres"},
    {password, "postgres"},
    {pool_size, 10}
]}].
```

The per-app form:

```erlang
[{my_app, [
    {my_repo, #{
        backend => kura_backend_postgres,
        database => ~"my_app_dev",
        hostname => ~"localhost",
        port => 5432,
        username => ~"postgres",
        password => ~"postgres",
        pool_size => 10
    }}
]}].
```

In the per-app form the settings go to the backend's pool as written, and
the pool applies its own defaults. kura_postgres needs `user`, and defaults
`database` to it. The flat form on Postgres still gets `user` and `database`
set to `"postgres"` when it leaves them out, with a deprecation warning;
kura 3.0 drops that, so set both.

The per-app form requires the consuming app to call `my_repo:start()`
manually. The `{repos, #{...}}` form (above) is preferred for new
projects - single-repo today, no rewrite when you add a second backend.
</details>

Migrations are discovered automatically from compiled modules implementing the `kura_migration` behaviour.

Optional telemetry/logging config:

```erlang
[{kura, [
    {log, true}  %% true | {M, F} | false (default)
]}].
```

## Plugins

- [rebar3_kura](https://github.com/Taure/rebar3_kura) - Rebar3 plugin that auto-generates migration files from schema changes. Add a field to your schema, run `rebar3 compile`, and the migration is created for you.
- [opentelemetry_kura](https://github.com/Taure/opentelemetry_kura) - OpenTelemetry instrumentation. Subscribes to Kura's telemetry events and creates spans for every database query.

## Examples

- [pet_store](https://github.com/Taure/pet_store) - A sample REST API built with Kura and Nova demonstrating schemas, changesets, queries, migrations, and associations in practice.

## Requirements

- Erlang/OTP 28+
- One backend: PostgreSQL 14+ (via [kura_postgres](https://github.com/Taure/kura_postgres)) or SQLite 3.35+ (via [kura_sqlite](https://github.com/Taure/kura_sqlite))
