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

Returns the previously set tenant (or `undefined`). Anything that is not one of the two strategies raises `{invalid_tenant, Tenant}`, as does an attribute value of `is_nil` or `is_not_nil`.

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

This is useful when you need to temporarily access a different tenant's data without affecting the surrounding context.

## How Scoping Works

Tenant scoping is applied automatically by `kura_repo_worker` and the query compiler.

Under the attribute strategy:

- **Queries** (`all`, `one`, `get`, `exists`, aggregates, `stream`, preloads): the tenant condition is added to the query, to every CTE and union, and to every `in` and `exists` subquery, however deeply nested.
- **Record writes** (`update`, `delete`, including optimistic locking): the record is addressed by its key and the tenant, so another tenant's row is not found rather than written.
- **Bulk writes** (`update_all`, `delete_all`): scoped like queries. A change to the tenant field is pinned to the current tenant, so a row cannot be moved to another tenant.
- **Inserts** (`insert`, `insert_all`): the tenant field and value are set on every row.
- **Upserts** (`on_conflict` with a `DO UPDATE` action): the update only applies when the conflicting row belongs to the same tenant. A conflict with another tenant's row leaves it untouched.

The tenant condition names the field unqualified. A query, CTE or union branch over a table without it fails rather than running unscoped. Inside an `in` or `exists` subquery it does not fail: the database resolves the field to the outer query's column, so the inner table is not scoped. Give every tenant table the same tenant column name.

Upserts that hit another tenant's row behave like `DO NOTHING`. Under attribute tenancy, unique indexes should include the tenant column; a unique index across tenants still tells one tenant that a value exists in another.

Under a tenant context, writes to a table without the tenant field differ: `update/2`, `delete/2` and an upsert fail, and a plain insert succeeds without the tenant.

kura does not check that a foreign key points at a row of the same tenant. A tenant can still set `post_id` to another tenant's post: reads stay scoped, but the insert tells it the id exists, and the row it plants can block the other tenant's delete under `ON DELETE RESTRICT`. Make tenant foreign keys composite, `FOREIGN KEY (org_id, post_id) REFERENCES posts (org_id, id)` over a unique `(org_id, id)`, so the database refuses them.

Under the schema prefix strategy, the dialect qualifies each table with the tenant's schema. If the query already has an explicit prefix set, the tenant prefix is not overridden. Many-to-many join tables are always qualified with the tenant's schema, whatever prefix the query sets.

### What is not scoped

- `query/3` with raw SQL, and `{fragment, SQL, Params}` conditions. kura cannot see into SQL text; add the tenant condition yourself.
- Joins between two tenant tables. Both carry the tenant field, so the unqualified condition is ambiguous and the query fails. A join to a table without the field works, and that table is not scoped, which is right for global lookup tables.

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

Since Cowboy spawns a process per request, the tenant is naturally isolated between concurrent requests. Remember to clear the tenant if the process is reused.

The tenant lives in the process dictionary, so a process you spawn does not have it. kura hands it to the processes it starts for a preload of several associations; for your own, read it with `get_tenant/0` and set it in the new process before it touches the repo.
