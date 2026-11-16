# Multitenancy

`kura_tenant` provides process dictionary-based multitenancy with two strategies: schema prefix and attribute-based. Once a tenant is set, the queries and writes kura builds in that process are scoped to it. Raw SQL is not; see [What is not scoped](#what-is-not-scoped).

> **Backend note:** the schema-prefix strategy uses PostgreSQL schemas. SQLite has no schema concept; for multi-tenant SQLite use one database file per tenant or `ATTACH DATABASE`. The attribute-based strategy works on any backend.

## Strategies

### Schema Prefix

Uses PostgreSQL schemas to isolate tenant data. Each tenant has its own schema (e.g., `tenant_acme.users` instead of `public.users`).

```erlang
kura_tenant:put_tenant({prefix, ~"tenant_acme"}).

%% All queries now target the "tenant_acme" schema
{ok, Users} = kura_repo_worker:all(my_repo, kura_query:from(my_user)).
%% SELECT * FROM "tenant_acme"."users"
```

### Attribute-Based

Uses a shared table with a tenant identifier column. Queries automatically add a `WHERE` clause and inserts automatically include the tenant value.

```erlang
kura_tenant:put_tenant({attribute, {org_id, 42}}).

%% Queries are scoped
{ok, Users} = kura_repo_worker:all(my_repo, kura_query:from(my_user)).
%% SELECT * FROM "users" WHERE "org_id" = 42

%% Inserts include the tenant attribute automatically
CS = kura_changeset:cast(my_user, #{}, #{~"name" => ~"Alice"}, [name]),
{ok, User} = kura_repo_worker:insert(my_repo, CS).
%% The inserted row has org_id = 42
```

## API

### Setting a Tenant

```erlang
kura_tenant:put_tenant({prefix, ~"tenant_acme"}).
kura_tenant:put_tenant({attribute, {org_id, 42}}).
```

Returns the previously set tenant (or `undefined`). Anything that is not one of the two strategies raises `{invalid_tenant, Tenant}`, as does an attribute value of `is_nil`, `is_not_nil`, `undefined`, `null` or `nil`: a tenant built from a claim that was missing is refused, not run as a tenant that owns nothing. An attribute tenant's field is an atom that names a field: `and`, `or`, `not`, `exists`, `not_exists` and `fragment`, which `kura_query` reads as the head of a condition, are refused the same way. A field that a schema does not declare is a different matter; see [A schema that does not declare the tenant field](#a-schema-that-does-not-declare-the-tenant-field).

`put_tenant/1` sets the tenant for the rest of the process's life. That fits a process that serves one request and ends. In a process that outlives its work (a pool worker, a job runner, a `gen_server`) use [`with_tenant/2`](#temporary-tenant-scope), or the next piece of work runs under the last one's tenant.

### Getting the Current Tenant

```erlang
case kura_tenant:get_tenant() of
    {prefix, Prefix} -> Prefix;
    {attribute, {Field, Value}} -> {Field, Value};
    undefined -> no_tenant
end.
```

### Clearing the Tenant

```erlang
kura_tenant:clear_tenant().
```

Returns the tenant that was cleared (or `undefined`).

### Temporary Tenant Scope

`with_tenant/2` sets a tenant for the duration of a function call, then restores the previous tenant:

```erlang
kura_tenant:put_tenant({prefix, ~"tenant_acme"}).

Result = kura_tenant:with_tenant({prefix, ~"tenant_other"}, fun() ->
    %% Inside here, tenant is "tenant_other"
    kura_repo_worker:all(my_repo, kura_query:from(my_user))
end).

%% Tenant is back to "tenant_acme" here
```

The process is put back as it was however the function ends: on a return, an error, a throw or an exit. A tenant set inside it with `put_tenant/1` ends with it too. The work can also be given as `{Module, Function, Args}`:

```erlang
kura_tenant:with_tenant({attribute, {org_id, OrgId}}, {my_jobs, run, [Job]}).
```

This is the form for any process that is reused, and for temporarily reading another tenant's data without affecting the surrounding context.

## How Scoping Works

Tenant scoping is applied automatically by `kura_repo_worker` and the query compiler.

Under the attribute strategy:

- **Queries** (`all`, `one`, `get`, `exists`, aggregates, `stream`, preloads): the tenant condition is added to the query, to every CTE and union, and to every `in` and `exists` subquery, however deeply nested.
- **Record writes** (`update`, `delete`, including optimistic locking): the record is addressed by its key and the tenant, so another tenant's row is not found rather than written.
- **Bulk writes** (`update_all`, `delete_all`): the tenant condition is added to the statement's conditions, as for a query. A change to the tenant field is pinned to the current tenant, so a row cannot be moved to another tenant.
- **Fragments**: a `{fragment, SQL, Params}` condition is written inside parentheses and the tenant condition is joined to it with `AND`, so an `OR` in the fragment's text does not reach past the tenant: `WHERE (title ILIKE $1 OR body ILIKE $2) AND "org_id" = $3`. That holds in a query, an `update_all` and a `delete_all`.
- **Inserts** (`insert`, `insert_all`): the tenant field and value are set on every row.
- **Upserts** (`on_conflict` with a `DO UPDATE` action): the update only applies when the conflicting row belongs to the same tenant. A conflict with another tenant's row leaves it untouched.

The tenant condition names the field unqualified. A query, CTE or union branch over a table without it fails rather than running unscoped. Inside an `in` or `exists` subquery it does not fail: the database resolves the field to the outer query's column, so the inner table is not scoped. Give every tenant table the same tenant column name.

### A schema that does not declare the tenant field

kura adds the tenant condition whatever the schema declares. A schema that leaves out a tenant column its table has is scoped all the same, in a query, an `update_all` and a `delete_all`. Over a table without the column the database refuses the statement. Either way kura logs one warning per boot for each such schema, naming the schema and the field:

```
kura: a query under an attribute tenant reads a schema that does not declare the tenant's field, ...
    schema: my_report
    field: org_id
```

On a repo with [`require_tenant => true`](#requiring-a-tenant) the query raises `{invalid_tenant, Tenant}` instead, before it is sent to the database. kura 3.0 raises on every repo, so declare the tenant field in every schema you read under a tenant.

kura judges a query only where it can tell: the query, or a query combined with it, is from a schema and has no join and no CTE, either of which can bring the column in. Any other query is left to the database, without a warning.

### A schema every tenant shares

A table of plans or countries has no tenant column and belongs to no tenant. Its schema says so:

```erlang
-module(plan).
-behaviour(kura_schema).
-export([table/0, fields/0, tenant_scoped/0]).

tenant_scoped() -> false.
```

Under an attribute tenant a query that reads only such schemas gets no tenant condition and no warning, in `all`, `get`, aggregates, a preload of the schema, `update_all` and `delete_all`: every tenant reads the same rows, and a bulk write under a tenant reaches all of them. That is judged for each query on its own, so a shared subquery, CTE body or union branch is left alone inside a query that is scoped.

A query that joins a shared schema to anything else keeps the tenant condition, whichever of the two it is from: the condition names the tenant table's column. Only a schema module counts. A table named by a binary or an atom is scoped, and so is a CTE's name, whatever its body reads.

Writes of one record are not exempt. `update/2`, `delete/2` and an upsert of a shared schema's changeset are still matched on the tenant, which its table does not have, so the database refuses them. A plain insert writes the fields the schema declares. Change shared rows where no tenant is set, or inside `kura_tenant:without_tenant/1`.

Upserts that hit another tenant's row behave like `DO NOTHING`. Under attribute tenancy, unique indexes should include the tenant column; a unique index across tenants still tells one tenant that a value exists in another.

Under a tenant context, writes of a changeset to a table without the tenant field differ: `update/2`, `delete/2` and an upsert fail in the database, and a plain insert succeeds without the tenant. The same goes for a schema that does not declare the field: an insert writes the fields the schema declares, so the row is written without the tenant, with or without `require_tenant`.

kura does not check that a foreign key points at a row of the same tenant. A tenant can still set `post_id` to another tenant's post: reads stay scoped, but the insert tells it the id exists, and the row it plants can block the other tenant's delete under `ON DELETE RESTRICT`. Make tenant foreign keys composite, `FOREIGN KEY (org_id, post_id) REFERENCES posts (org_id, id)` over a unique `(org_id, id)`, so the database refuses them.

Under the schema prefix strategy, the dialect qualifies each table with the tenant's schema. If the query already has an explicit prefix set, the tenant prefix is not overridden. Many-to-many join tables are always qualified with the tenant's schema, whatever prefix the query sets.

### What is not scoped

- `query/3` with raw SQL. kura cannot see into SQL text; add the tenant condition yourself.
- What the SQL text of a `{fragment, SQL, Params}` reads on its own: a table in a subquery written there is not scoped, and neither is one a select-expression fragment reads. Add the tenant condition in the text. The query the fragment is a condition of is scoped as any other.
- Many-to-many join tables. The join rows are read, deleted and inserted without the tenant condition, and an insert does not stamp the tenant, so a join table with its own `NOT NULL` tenant column makes every `put_assoc` or `cast_assoc` on the association fail. The rows a join links are read and checked under the tenant; keep the tenant column on those tables, not on the join table.
- Joins between two tenant tables. Both carry the tenant field, so the unqualified condition is ambiguous and the query fails. A join to a table without the field works, and that table is not scoped, which is right for global lookup tables. After a join kura qualifies the `from` schema's other columns, but the tenant field stays bare under a tenant, so selecting, filtering, ordering or grouping by it in a join of two tenant tables fails as ambiguous too.

## Requiring a Tenant

Without a tenant in the process, nothing is scoped: the query runs
against every tenant's rows. That is the case in a process you spawned
and did not hand the tenant to, in a background job, and in a request
whose middleware did not run.

Turn a missing tenant into an error, for a repo:

```erlang
%% sys.config
{kura, [
    {repos, #{
        my_repo => #{backend => kura_backend_postgres, require_tenant => true}
    }}
]}
```

or for every repo that does not say otherwise:

```erlang
{kura, [{require_tenant, true}]}
```

A repo's own `require_tenant` wins, so a repo that holds no tenant data,
such as a control plane's beside the tenants' repos, sets
`require_tenant => false` and is left alone.

A query, or an insert, update or delete of a changeset, in a process
without a tenant then raises `{kura, {tenant_required, Source}}`, where
`Source` is the schema or table. The same goes for `insert_all`,
`update_all` and `delete_all`. A query is judged by everything it reads:
what it is from and what it joins, in the query itself and in every CTE,
union branch and subquery inside it. `kura_query:sources/1` lists them.
A source that names a CTE is judged by the CTE's body, but only where the
database sees the CTE: inside a CTE's body its own name, and the name of a
CTE listed after it, are tables.

Two things are exempt.

A schema every tenant shares, such as plans or countries, says so with
`tenant_scoped() -> false`; see
[A schema every tenant shares](#a-schema-every-tenant-shares).

A path that crosses tenants on purpose, such as an admin report or a
maintenance job, says so where it runs:

```erlang
kura_tenant:without_tenant(fun() ->
    my_repo:all(kura_query:from(invoice))
end).
```

Inside the fun the process has no tenant, and the one it had is restored
afterwards, however the fun ends. `without_tenant/1` takes
`{Module, Function, Args}` as well. Keep these calls few and easy to find:
each is a place where tenants are not isolated.

Raw SQL through `query/3` is not checked, as it is not scoped.

With `require_tenant` set, a query from a schema that does not declare
the attribute tenant's field raises `{invalid_tenant, Tenant}`; see
[A schema that does not declare the tenant field](#a-schema-that-does-not-declare-the-tenant-field).

`require_tenant` checks that a tenant is set, not which schema a query
reads. Under the prefix strategy a query with its own `kura_query:prefix/2`
reads that schema whatever the tenant, so never build a prefix from a
request.

`require_tenant` is one of a repo's settings (`kura_repo:settings/1`): it
is fixed when the repo's pool starts, the application environment's value
included, and starting the pool again re-reads it. A value that is not a
boolean keeps kura, or the repo's pool, from starting. Left unset in both
places, kura logs one warning per boot at the first query or write under a
tenant, naming the repo. Set it to `false` to keep unscoped queries
without the warning.

An application that never sets a tenant sees no warning and has nothing to
set. The plan for kura 3.0 is to require a tenant by default only on a repo
that declares a tenancy strategy; kura 2.x has no such declaration.

## When to Use Which

**Schema prefix** is a good fit when:

- You need strong data isolation between tenants
- Each tenant may have schema-level customizations
- You are willing to manage per-tenant PostgreSQL schemas (migrations run per schema)

**Attribute-based** is a good fit when:

- You want simpler infrastructure (single schema, single set of migrations)
- Data isolation at the application level is sufficient
- You have many tenants and creating individual schemas is impractical

## Usage in Web Applications

In a Nova handler or middleware, set the tenant early in the request lifecycle:

```erlang
pre_request(Req, State) ->
    TenantId = get_tenant_from_request(Req),
    kura_tenant:put_tenant({attribute, {org_id, TenantId}}),
    {ok, Req, State}.
```

Since Cowboy spawns a process per request, the tenant is naturally isolated between concurrent requests. A process that is reused is different: a pool worker or a job runner that called `put_tenant/1` keeps that tenant for whatever it does next. Run each piece of work there inside `with_tenant/2`, which ends the tenant with the work.

The tenant lives in the process dictionary, so a process you spawn does not have it. kura hands the caller's scope to the processes it starts for a preload of several associations; for your own, read it with `kura_tenant:current_scope/0` and call `kura_tenant:adopt/1` in the new process before it touches the repo. `adopt/1` is for a process that ends with that work: the scope, including "no tenant on purpose" when it was read inside `without_tenant/1`, stays for the rest of the process's life. In a pool worker, a job runner or a `gen_server`, run the work in `with_tenant/2` instead. With [`require_tenant`](#requiring-a-tenant) set, forgetting to is an error instead of an unscoped query.
