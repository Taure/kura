# Embedded Schemas

Embedded schemas let you nest structured data inside a parent schema, stored as JSONB in PostgreSQL. They're useful for semi-structured data that doesn't need its own table — addresses, settings, metadata, tags, etc.

Kura supports two embed types:

- `embeds_one` — a single nested map
- `embeds_many` — a list of nested maps

## Defining Embedded Schemas

An embedded schema is a regular `kura_schema` module. By convention, set its table to `<<"_embedded">>` since it won't have its own database table:

```erlang
-module(address).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").
-export([fields/0]).

fields() ->
    [
        #kura_field{name = street, type = string},
        #kura_field{name = city, type = string},
        #kura_field{name = zip, type = string}
    ].
```

```erlang
-module(tag).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").
-export([fields/0]).

fields() ->
    [
        #kura_field{name = label, type = string},
        #kura_field{name = weight, type = integer}
    ].
```

## Adding Embeds to a Parent Schema

Export an `embeds/0` callback from the parent schema returning a list of `#kura_embed{}` records:

```erlang
-module(profile).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").
-export([table/0, fields/0, embeds/0]).

table() -> <<"profiles">>.

fields() ->
    [
        #kura_field{name = id, type = id},
        #kura_field{name = name, type = string},
        #kura_field{name = bio, type = text}
    ].

embeds() ->
    [
        #kura_embed{name = address, type = embeds_one, schema = address},
        #kura_embed{name = tags, type = embeds_many, schema = tag}
    ].
```

The corresponding migration would define these columns as JSONB:

```erlang
up(Pool) ->
    kura_migrator:execute(Pool,
        "CREATE TABLE profiles ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  bio TEXT,"
        "  address JSONB,"
        "  tags JSONB DEFAULT '[]'::jsonb,"
        "  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
        "  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
        ")"
    ).
```

## Casting Embeds

Use `kura_changeset:cast_embed/2` to cast nested params through the embedded schema's fields. Validation errors from the embedded schema bubble up to the parent changeset.

### embeds_one

Pass a map of params for the embedded field:

```erlang
Params = #{
    name => <<"Alice">>,
    bio => <<"Developer">>,
    address => #{street => <<"123 Main St">>, city => <<"Portland">>, zip => <<"97201">>}
},
CS = kura_changeset:cast(profile, #{}, Params, [name, bio]),
CS1 = kura_changeset:cast_embed(CS, address),
%% CS1#kura_changeset.changes contains:
%%   #{name => <<"Alice">>,
%%     bio => <<"Developer">>,
%%     address => #{street => <<"123 Main St">>, city => <<"Portland">>, zip => <<"97201">>}}
```

### embeds_many

Pass a list of maps:

```erlang
Params = #{
    name => <<"Alice">>,
    tags => [
        #{label => <<"important">>, weight => 10},
        #{label => <<"urgent">>, weight => 5}
    ]
},
CS = kura_changeset:cast(profile, #{}, Params, [name]),
CS1 = kura_changeset:cast_embed(CS, tags),
%% CS1#kura_changeset.changes contains:
%%   #{name => <<"Alice">>,
%%     tags => [
%%       #{label => <<"important">>, weight => 10},
%%       #{label => <<"urgent">>, weight => 5}
%%     ]}
```

## Custom Validation with `with`

By default, `cast_embed` casts all non-virtual fields of the embedded schema. To apply custom validation or restrict fields, pass a `with` option — a function that takes existing data and params, and returns a changeset:

```erlang
WithFun = fun(_Data, EmbedParams) ->
    ChildCS = kura_changeset:cast(address, #{}, EmbedParams, [street, city]),
    kura_changeset:validate_required(ChildCS, [street, city])
end,

CS1 = kura_changeset:cast_embed(CS, address, #{with => WithFun})
```

This is useful when you want to:

- Require specific fields on the embed
- Only allow a subset of fields to be cast
- Add custom validations (length, format, etc.)

## Error Handling

Type mismatches are caught automatically:

```erlang
%% Passing a list where a map is expected
Params = #{name => <<"Alice">>, address => [1, 2, 3]},
CS = kura_changeset:cast(profile, #{}, Params, [name]),
CS1 = kura_changeset:cast_embed(CS, address),
%% CS1#kura_changeset.valid =:= false
%% CS1#kura_changeset.errors =:= [{address, <<"expected a map">>}]
```

Validation errors from embedded schemas are merged into the parent:

```erlang
WithFun = fun(_Data, EmbedParams) ->
    ChildCS = kura_changeset:cast(address, #{}, EmbedParams, [street, city, zip]),
    kura_changeset:validate_required(ChildCS, [street, city, zip])
end,

Params = #{name => <<"Alice">>, address => #{street => <<"123 Main St">>}},
CS = kura_changeset:cast(profile, #{}, Params, [name]),
CS1 = kura_changeset:cast_embed(CS, address, #{with => WithFun}),
%% CS1#kura_changeset.valid =:= false
%% Errors include missing city and zip from the embedded changeset
```

## Storage

Embedded schemas are stored as JSONB in PostgreSQL. Kura handles serialization automatically:

- **On insert/update**: Erlang maps are JSON-encoded before writing
- **On load**: JSONB values are decoded back into Erlang maps with atom keys

This means you can query embedded data using PostgreSQL's JSONB operators in raw SQL, while working with regular Erlang maps in application code.

## When to Use Embedded Schemas

**Good fit:**
- Data that always belongs to the parent (addresses, settings, metadata)
- Semi-structured or variable data (form responses, config)
- Data that doesn't need independent querying or foreign keys

**Consider a separate table instead when:**
- The data needs its own associations
- You need to query or index the nested data frequently
- The data is shared across multiple parents
