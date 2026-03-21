# Testing

Kura provides `kura_sandbox` for isolating database tests. Each test runs inside a transaction that is rolled back at the end, so tests don't interfere with each other and no test data persists.

## Setup

Start the repo pool once (in your test suite setup), then use `checkout`/`checkin` around each test.

### Common Test

```erlang
-module(my_app_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("kura/include/kura.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([create_user_test/1, duplicate_email_test/1]).

all() -> [create_user_test, duplicate_email_test].

init_per_suite(Config) ->
    ok = application:ensure_all_started(kura),
    ok = kura_repo_worker:start(my_repo),
    {ok, _} = kura_migrator:migrate(my_repo),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    kura_sandbox:checkout(my_repo),
    Config.

end_per_testcase(_TestCase, _Config) ->
    kura_sandbox:checkin(my_repo),
    ok.

create_user_test(_Config) ->
    CS = kura_changeset:cast(user, #{}, #{
        ~"username" => ~"alice",
        ~"email" => ~"alice@example.com",
        ~"password" => ~"secret123"
    }, [username, email, password]),
    CS1 = kura_changeset:validate_required(CS, [username, email]),
    {ok, User} = kura_repo_worker:insert(my_repo, CS1),
    ~"alice" = maps:get(username, User).

duplicate_email_test(_Config) ->
    Params = #{~"username" => ~"bob",
               ~"email" => ~"bob@example.com"},
    CS = kura_changeset:cast(user, #{}, Params, [username, email]),
    {ok, _} = kura_repo_worker:insert(my_repo, CS),

    %% Second insert with same email
    CS2 = kura_changeset:cast(user, #{}, Params#{~"username" => ~"bob2"}, [username, email]),
    {error, ErrCS} = kura_repo_worker:insert(my_repo, CS2),
    [{email, ~"has already been taken"}] = ErrCS#kura_changeset.errors.
```

### EUnit

```erlang
-module(my_app_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kura/include/kura.hrl").

my_app_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        {foreach,
            fun() -> kura_sandbox:checkout(my_repo) end,
            fun(_) -> kura_sandbox:checkin(my_repo) end,
            [fun insert_test/1,
             fun query_test/1]}}.

setup() ->
    ok = application:ensure_all_started(kura),
    ok = kura_repo_worker:start(my_repo),
    {ok, _} = kura_migrator:migrate(my_repo).

teardown(_) ->
    ok.

insert_test(_) ->
    fun() ->
        CS = kura_changeset:cast(user, #{}, #{~"name" => ~"Alice"}, [name]),
        {ok, User} = kura_repo_worker:insert(my_repo, CS),
        ?assertEqual(~"Alice", maps:get(name, User))
    end.

query_test(_) ->
    fun() ->
        {ok, Users} = kura_repo_worker:all(my_repo, kura_query:from(user)),
        ?assertEqual([], Users)
    end.
```

## How It Works

`kura_sandbox:checkout/1` checks out a connection from the pool and begins a transaction. All queries within the test use this connection. `kura_sandbox:checkin/1` rolls back the transaction and returns the connection to the pool.

This means:
- Each test starts with a clean database state
- Tests can run in any order without interference
- No manual cleanup of test data needed

## Testing Constraints

Unique indexes declared via `indexes/0` are automatically tested when you try to insert duplicate data:

```erlang
constraint_test(_Config) ->
    Params = #{~"username" => ~"alice"},
    CS = kura_changeset:cast(user, #{}, Params, [username]),

    %% First insert succeeds
    {ok, _} = kura_repo_worker:insert(my_repo, CS),

    %% Second insert returns changeset error (not a crash)
    {error, ErrCS} = kura_repo_worker:insert(my_repo, CS),
    [{username, ~"has already been taken"}] = ErrCS#kura_changeset.errors.
```

## Testing with Docker Compose

For local development and CI, use Docker Compose to run PostgreSQL:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: my_app_test
    ports:
      - "5432:5432"
```

```bash
docker compose up -d
rebar3 eunit
docker compose down
```
