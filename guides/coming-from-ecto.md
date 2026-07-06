# Coming from Ecto

If you've used Ecto in Elixir, Kura will feel familiar. The mental model is the same. The syntax is Erlang.

This guide is a side-by-side translation, not a tutorial. Each section shows the Ecto code you know, then the Kura equivalent.

## Mental model: what's the same, what's different

Same:
- Schemas describe tables. Changesets validate input. Queries are values you build up. Repos execute them.
- Migrations are versioned modules with `up`/`down`.
- Multi-step transactions compose into a pipeline you run atomically.

Different:
- **Records and functions, no macros.** Schemas are behaviours. Fields are `#kura_field{}` records. Queries are built by passing the query value through functions, not a `from u in User, where: ...` macro.
- **The repo is an argument, not a dispatcher.** Where Ecto has `MyRepo.insert(cs)`, Kura has `kura_repo_worker:insert(MyRepo, CS)`. The repo module exists to hold config; it does not dispatch CRUD.
- **WHERE conditions are data, not lambdas or quoted ASTs.** `kura_query:where(Q, {age, '>', 18})` instead of `where(q, [u], u.age > 18)`. There is no `Ecto.Query.API`-style sigil.
- **Field access uses atom column names.** Returned rows are maps with atom keys (configured via pgo's `column_name_as_atom`), not structs.

## Schemas

### Ecto

```elixir
defmodule MyApp.User do
  use Ecto.Schema

  schema "users" do
    field :name, :string
    field :email, :string
    field :age, :integer
    has_many :posts, MyApp.Post
    timestamps()
  end
end
```

### Kura

```erlang
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, associations/0]).

table() -> ~"users".

fields() ->
    [#kura_field{name = id, type = id, primary_key = true, nullable = false},
     #kura_field{name = name, type = string, nullable = false},
     #kura_field{name = email, type = string, nullable = false},
     #kura_field{name = age, type = integer},
     #kura_field{name = inserted_at, type = utc_datetime},
     #kura_field{name = updated_at, type = utc_datetime}].

associations() ->
    [#kura_assoc{name = posts, type = has_many,
                 schema = my_post, foreign_key = user_id}].
```

### Field types

| Ecto                 | Kura              |
|----------------------|-------------------|
| `:id`                | `id`              |
| `:integer`           | `integer`         |
| `:float`             | `float`           |
| `:string`            | `string`          |
| `:text` (varchar)    | `text`            |
| `:boolean`           | `boolean`         |
| `:date`              | `date`            |
| `:utc_datetime`      | `utc_datetime`    |
| `:naive_datetime`    | `naive_datetime`  |
| `Ecto.UUID`          | `uuid`            |
| `:map` / `:json`     | `jsonb`           |
| `{:array, T}`        | `{array, T}`      |
| `Ecto.Enum`          | `{enum, [a, b]}`  |
| `:decimal`           | `decimal`         |
| `:binary`            | `binary`          |

### Schema callbacks (all optional except `table/0` and `fields/0`)

| Purpose                | Ecto                          | Kura callback                        |
|------------------------|-------------------------------|--------------------------------------|
| Table name             | `schema "users" do`           | `table() -> ~"users".`               |
| Fields                 | `field :name, :string`        | `fields() -> [#kura_field{...}].`    |
| Timestamps             | `timestamps()`                | `timestamps() -> [{inserted_at, utc_datetime}, ...].` |
| Associations           | `has_many :posts, ...`        | `associations() -> [#kura_assoc{...}].` |
| Embeds                 | `embeds_many :tags, ...`      | `embeds() -> [#kura_embed{...}].`    |
| Indexes                | (in migrations only)          | `indexes() -> [...].`                |
| Constraints            | (in migrations only)          | `constraints() -> [...].`            |
| Custom PK generator    | `@primary_key {:id, :binary_id, autogenerate: true}` | `generate_id() -> ... .` |
| Composite primary key  | `@primary_key false` + `field :a, ..., primary_key: true` (x2) | `key() -> [a, b].` |
| Lifecycle hooks        | (none - use changesets)       | `before_insert/1`, `after_insert/1`, etc. |

## Changesets

### Cast and validate

#### Ecto

```elixir
def changeset(user, params) do
  user
  |> cast(params, [:name, :email, :age])
  |> validate_required([:name, :email])
  |> validate_format(:email, ~r/@/)
  |> validate_length(:name, min: 2, max: 100)
  |> unique_constraint(:email)
end
```

#### Kura

```erlang
changeset(User, Params) ->
    CS  = kura_changeset:cast(my_user, User, Params, [name, email, age]),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:validate_format(CS1, email, ~"@"),
    CS3 = kura_changeset:validate_length(CS2, name, [{min, 2}, {max, 100}]),
    kura_changeset:unique_constraint(CS3, email).
```

The `cast/4` signature: `cast(SchemaMod, Data, Params, AllowedFields)`.
- `SchemaMod` is the schema module (or a `#{Field => Type}` map for schemaless changesets).
- `Data` is the existing record as a map (use `#{}` for inserts).
- `Params` is the input map (string or atom keys both work).
- `AllowedFields` is a list of atom field names.

### Validation translation

| Ecto                                      | Kura                                                       |
|-------------------------------------------|------------------------------------------------------------|
| `validate_required([:a, :b])`             | `validate_required(CS, [a, b])`                            |
| `validate_format(:email, regex)`          | `validate_format(CS, email, Pattern)`                      |
| `validate_length(:name, min: 2, max: 50)` | `validate_length(CS, name, [{min, 2}, {max, 50}])`         |
| `validate_number(:age, greater_than: 0)`  | `validate_number(CS, age, [{greater_than, 0}])`            |
| `validate_inclusion(:role, [:a, :b])`     | `validate_inclusion(CS, role, [a, b])`                     |
| `validate_exclusion(:name, ["bad"])`      | `validate_exclusion(CS, name, [~"bad"])`                   |
| `validate_subset(:tags, [...])`           | `validate_subset(CS, tags, [...])`                         |
| `validate_confirmation(:password)`        | `validate_confirmation(CS, password)`                      |
| `validate_change(:f, fn _f, val -> ... end)` | `validate_change(CS, F, fun(V) -> ok \| {error, Msg} end)` |
| `unique_constraint(:email)`               | `unique_constraint(CS, email)`                             |
| `foreign_key_constraint(:user_id)`        | `foreign_key_constraint(CS, user_id)`                      |
| `check_constraint(:age, name: "...")`     | `check_constraint(CS, ~"constraint_name", age)`            |

### Inspecting and modifying

| Ecto                                | Kura                                |
|-------------------------------------|-------------------------------------|
| `Changeset.get_change(cs, :name)`   | `kura_changeset:get_change(CS, name)`|
| `Changeset.get_field(cs, :name)`    | `kura_changeset:get_field(CS, name)` |
| `Changeset.put_change(cs, :name, V)`| `kura_changeset:put_change(CS, name, V)` |
| `Changeset.add_error(cs, :f, "msg")`| `kura_changeset:add_error(CS, f, ~"msg")` |
| `Changeset.apply_changes(cs)`       | `kura_changeset:apply_changes(CS)`  |
| `Changeset.apply_action(cs, :insert)`| `kura_changeset:apply_action(CS, insert)` |
| `Changeset.traverse_errors(cs, fn)` | `kura_changeset:traverse_errors(CS, Fun)` |
| `Changeset.optimistic_lock(cs, :v)` | `kura_changeset:optimistic_lock(CS, v)` |
| `Changeset.prepare_changes(cs, fn)` | `kura_changeset:prepare_changes(CS, Fun)` |

## Repo

Ecto's repo dispatches CRUD; Kura's `kura_repo_worker` takes the repo module as an argument.

| Ecto                            | Kura                                              |
|---------------------------------|---------------------------------------------------|
| `MyRepo.insert(cs)`             | `kura_repo_worker:insert(MyRepo, CS)`             |
| `MyRepo.update(cs)`             | `kura_repo_worker:update(MyRepo, CS)`             |
| `MyRepo.delete(struct)`         | `kura_repo_worker:delete(MyRepo, CS)`             |
| `MyRepo.get(User, id)`          | `kura_repo_worker:get(MyRepo, my_user, Id)`       |
| `MyRepo.get_by(User, email: e)` | `kura_repo_worker:get_by(MyRepo, my_user, #{email => E})` |
| `MyRepo.one(query)`             | `kura_repo_worker:one(MyRepo, Q)`                 |
| `MyRepo.all(query)`             | `kura_repo_worker:all(MyRepo, Q)`                 |
| `MyRepo.exists?(query)`         | `kura_repo_worker:exists(MyRepo, Q)`              |
| `MyRepo.aggregate(q, :count)`   | `kura_repo_worker:aggregate(MyRepo, Q, count)`    |
| `MyRepo.insert_all(User, rows)` | `kura_repo_worker:insert_all(MyRepo, my_user, Rows)` |
| `MyRepo.update_all(query, set)` | `kura_repo_worker:update_all(MyRepo, Q, Updates)` |
| `MyRepo.delete_all(query)`      | `kura_repo_worker:delete_all(MyRepo, Q)`          |
| `MyRepo.transaction(fn)`        | `kura_repo_worker:transaction(MyRepo, Fun)`       |
| `MyRepo.preload(rows, :assoc)`  | `kura_repo_worker:preload(MyRepo, my_user, Rows, [assoc])` |

The repo module itself only needs `otp_app/0` plus optional `init/1`:

```erlang
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0]).
otp_app() -> my_app.
```

## Queries

### Ecto

```elixir
from u in User,
  where: u.age > 18 and u.active == true,
  order_by: [desc: u.inserted_at],
  limit: 10,
  preload: [:posts]
```

### Kura

```erlang
Q  = kura_query:from(my_user),
Q1 = kura_query:where(Q, {'and', [{age, '>', 18}, {active, true}]}),
Q2 = kura_query:order_by(Q1, [{inserted_at, desc}]),
Q3 = kura_query:limit(Q2, 10),
kura_query:preload(Q3, [posts]).
```

### WHERE conditions are data

Ecto uses a query DSL macro. Kura uses tuples.

| Ecto                         | Kura                                |
|------------------------------|-------------------------------------|
| `u.name == "Alice"`          | `{name, ~"Alice"}`                  |
| `u.name == "Alice"`          | `{name, '=', ~"Alice"}` (explicit)  |
| `u.age > 18`                 | `{age, '>', 18}`                    |
| `u.age >= 18`                | `{age, '>=', 18}`                   |
| `u.name != "x"`              | `{name, '!=', ~"x"}`                |
| `not is_nil(u.email)`        | `{email, is_not_nil}`               |
| `is_nil(u.email)`            | `{email, is_nil}`                   |
| `u.role in [:a, :b]`         | `{role, in, [a, b]}`                |
| `like(u.name, "%foo%")`      | `{name, like, ~"%foo%"}`            |
| `ilike(u.name, "%foo%")`     | `{name, ilike, ~"%foo%"}`           |
| `a and b`                    | `{'and', [A, B]}`                   |
| `a or b`                     | `{'or', [A, B]}`                    |

### Query operations

| Ecto                                      | Kura                                                |
|-------------------------------------------|-----------------------------------------------------|
| `from u in User`                          | `kura_query:from(my_user)`                          |
| `select: u.name`                          | `kura_query:select(Q, [name])`                      |
| `select: %{name: u.name, count: count()}` | `kura_query:select_expr(Q, [...])`                  |
| `where: u.age > 18`                       | `kura_query:where(Q, {age, '>', 18})`               |
| `join: p in Post, on: p.user_id == u.id`  | `kura_query:join(Q, inner, my_post, {id, user_id})` (`{LeftCol, RightCol}` = `{prev_table_col, joined_table_col}`) |
| `left_join: ...`                          | `kura_query:join(Q, left, ...)`                     |
| `order_by: [desc: u.inserted_at]`         | `kura_query:order_by(Q, [{inserted_at, desc}])`     |
| `group_by: u.role`                        | `kura_query:group_by(Q, [role])`                    |
| `having: count(p.id) > 5`                 | `kura_query:having(Q, ...)`                         |
| `limit: 10`                               | `kura_query:limit(Q, 10)`                           |
| `offset: 20`                              | `kura_query:offset(Q, 20)`                          |
| `distinct: true`                          | `kura_query:distinct(Q)`                            |
| `preload: [:posts]`                       | `kura_query:preload(Q, [posts])`                    |
| `lock: "FOR UPDATE"`                      | `kura_query:lock(Q, ~"FOR UPDATE")`                 |
| `with cte as ...`                         | `kura_query:with_cte(Q, ~"cte_name", SubQ)`         |
| `union ...`                               | `kura_query:union(Q1, Q2)`                          |

Soft-delete:
- `kura_query:with_deleted(Q)` - include soft-deleted rows
- `kura_query:only_deleted(Q)` - only soft-deleted rows

## Multi

### Ecto

```elixir
Multi.new()
|> Multi.insert(:user, user_cs)
|> Multi.insert(:profile, fn %{user: user} ->
     Profile.changeset(%Profile{user_id: user.id}, params)
   end)
|> Multi.run(:notify, fn _repo, %{user: u} -> notify(u) end)
|> MyRepo.transaction()
```

### Kura

```erlang
M  = kura_multi:new(),
M1 = kura_multi:insert(M, user, UserCS),
M2 = kura_multi:insert(M1, profile,
        fun(#{user := User}) ->
            profile_changeset(#{user_id => maps:get(id, User)}, Params)
        end),
M3 = kura_multi:run(M2, notify,
        fun(#{user := U}) -> notify(U) end),
kura_repo_worker:multi(MyRepo, M3).
```

Same pipeline shape, just without `|>`.

## Migrations

### Ecto

```elixir
defmodule MyApp.Repo.Migrations.CreateUsers do
  use Ecto.Migration

  def up do
    create table(:users) do
      add :name, :string, null: false
      add :email, :string, null: false
      timestamps()
    end
    create unique_index(:users, [:email])
  end

  def down do
    drop index(:users, [:email])
    drop table(:users)
  end
end
```

### Kura

```erlang
-module(m20250101120000_create_users).
-behaviour(kura_migration).
-include_lib("kura/include/kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_table, ~"users", [
        #kura_column{name = id, type = id, primary_key = true},
        #kura_column{name = name, type = string, nullable = false},
        #kura_column{name = email, type = string, nullable = false},
        #kura_column{name = inserted_at, type = utc_datetime, nullable = false},
        #kura_column{name = updated_at, type = utc_datetime, nullable = false}
     ]},
     {create_index, ~"users", [email], #{unique => true}}].

down() ->
    [{drop_index, ~"users_email_index"},
     {drop_table, ~"users"}].
```

Migrations are real Erlang modules. They live under `src/migrations/` (or any subdirectory of `src/`) so rebar3 compiles them. `kura_migrator:migrate(MyRepo)` runs pending ones in version order.

| Ecto                            | Kura                                                |
|---------------------------------|-----------------------------------------------------|
| `create table(:users) do ... end` | `{create_table, ~"users", [#kura_column{...}]}`   |
| `add :name, :string`            | `#kura_column{name = name, type = string}`          |
| `alter table(:users) do ... end`| `{alter_table, ~"users", [...]}`                    |
| `add :col, :type`               | `{add_column, ~"users", #kura_column{...}}`         |
| `remove :col`                   | `{drop_column, ~"users", col}`                      |
| `rename :col, to: :new_col`     | `{rename_column, ~"users", old, new}`               |
| `create unique_index(:t, [:c])` | `{create_index, ~"t", [c], #{unique => true}}`     |
| `drop index(:t, [:c])`          | `{drop_index, ~"t_c_index"}`                        |
| `drop table(:users)`            | `{drop_table, ~"users"}`                            |
| `execute "SQL"`                 | `{execute, ~"SQL"}`                                 |

## Associations

### Ecto

```elixir
defmodule User do
  schema "users" do
    has_many :posts, Post
  end
end

defmodule Post do
  schema "posts" do
    belongs_to :user, User
  end
end
```

### Kura

```erlang
-module(my_user).
%% ...
associations() ->
    [#kura_assoc{name = posts, type = has_many,
                 schema = my_post, foreign_key = user_id}].

-module(my_post).
%% ...
associations() ->
    [#kura_assoc{name = user, type = belongs_to,
                 schema = my_user, foreign_key = user_id}].
```

| Ecto                                      | Kura                                                  |
|-------------------------------------------|-------------------------------------------------------|
| `belongs_to :user, User`                  | `#kura_assoc{type = belongs_to, ...}`                 |
| `has_one :profile, Profile`               | `#kura_assoc{type = has_one, ...}`                    |
| `has_many :posts, Post`                   | `#kura_assoc{type = has_many, ...}`                   |
| `many_to_many :tags, Tag, join_through: ...`| `#kura_assoc{type = many_to_many, join_through = ..., join_keys = ...}` |
| `cast_assoc(cs, :posts)`                  | `kura_changeset:cast_assoc(CS, posts)`                |
| `put_assoc(cs, :posts, list)`             | `kura_changeset:put_assoc(CS, posts, List)`           |
| `Repo.preload(user, :posts)`              | `kura_repo_worker:preload(MyRepo, my_user, Users, [posts])` |

## Embedded schemas

### Ecto

```elixir
embeds_many :tags, Tag
embeds_one :address, Address
```

### Kura

```erlang
embeds() ->
    [#kura_embed{name = tags, type = embeds_many, schema = tag},
     #kura_embed{name = address, type = embeds_one, schema = address}].
```

Embeds are stored as JSONB. Cast with `cast_embed/2,3`, place with `put_change/3`.

## What's intentionally different

- **No `from u in User, where: u.x == y` macro.** Queries are values built by function calls. Composable, but more verbose for one-liners.
- **No `import Ecto.Query` and the magic that comes with it.** No bindings, no fragments-as-AST. Raw SQL fragments use `{fragment, ~"SQL", Params}` tuples in `select_expr`.
- **No protocols.** Ecto leans on `Enumerable`, `String.Chars`, etc. Kura returns plain maps and lists.
- **No `Ecto.Type` for custom field types.** Custom types are added in `kura_types` directly. Less hookable, simpler.
- **Repo is config, not dispatcher.** This trips Ecto users at first - then it stops mattering.

## Things Ecto has that Kura does not (yet)

- `Ecto.Query.windows/3` - windowed expressions exist but are less ergonomic.
- `prefix:` per-query at full Ecto parity. Kura has `prefix/2` and multitenancy via `kura_tenant`, but the surface is smaller.
- A query macro language. By design.

## Smallest end-to-end example

```erlang
%% schema
-module(my_user).
-behaviour(kura_schema).
-include_lib("kura/include/kura.hrl").
-export([table/0, fields/0]).
table() -> ~"users".
fields() ->
    [#kura_field{name = id, type = id, primary_key = true},
     #kura_field{name = email, type = string, nullable = false},
     #kura_field{name = name, type = string},
     #kura_field{name = age, type = integer},
     #kura_field{name = inserted_at, type = utc_datetime},
     #kura_field{name = updated_at, type = utc_datetime}].

%% repo
-module(my_repo).
-behaviour(kura_repo).
-export([otp_app/0]).
otp_app() -> my_app.

%% usage
ok = kura_repo_worker:start(my_repo),

CS  = kura_changeset:cast(my_user, #{},
        #{~"email" => ~"alice@example.com", ~"name" => ~"Alice", ~"age" => 30},
        [email, name, age]),
CS1 = kura_changeset:validate_required(CS, [email, name]),
{ok, Alice} = kura_repo_worker:insert(my_repo, CS1),

Q = kura_query:where(kura_query:from(my_user), {age, '>=', 18}),
{ok, Adults} = kura_repo_worker:all(my_repo, Q).
```

That's the whole loop. Schema, repo, changeset, query. If you've used Ecto, you know what each piece does. The names line up.
