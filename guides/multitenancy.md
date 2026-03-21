# Multitenancy

`kura_tenant` provides process dictionary-based multitenancy with two strategies: schema prefix and attribute-based. Once a tenant is set, all Kura repo operations in that process are automatically scoped.

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

Returns the previously set tenant (or `undefined`).

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

Tenant scoping is applied automatically by `kura_repo_worker`:

- **Queries** (`all`, `one`, `update_all`, `delete_all`): the tenant filter is applied before the query is compiled to SQL.
- **Inserts**: for attribute-based tenancy, the tenant field and value are merged into the changeset's changes before the INSERT is executed.
- **Schema prefix**: if the query already has an explicit prefix set, the tenant prefix is not overridden.

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
