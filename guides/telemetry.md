# Query Telemetry

Kura can log every database query with timing information. Configure logging via `sys.config` under the `kura` application key.

## Configuration

Add a `log` key to the `kura` application environment in your `sys.config`:

```erlang
{kura, [
    {log, true}
]}.
```

Supported values:

| Value | Behavior |
|---|---|
| `true` | Use the built-in default logger (`logger:info`) |
| `{Module, Function}` | Call `Module:Function(Event)` for each query |
| `false` or absent | No logging (default) |

You can also set a `fun/1` at runtime for programmatic use:

```erlang
application:set_env(kura, log, fun(Event) -> logger:info("~p", [Event]) end).
```

### MFA example in sys.config

```erlang
{kura, [
    {log, {my_app_telemetry, handle_query}}
]}.
```

Where `my_app_telemetry:handle_query/1` receives the event map.

## Event Structure

Each event is a map with the following keys:

```erlang
#{query => <<"SELECT u0.\"id\", u0.\"name\" FROM \"users\" AS u0 WHERE (u0.\"id\" = $1)">>,
  params => [1],
  result => ok,
  num_rows => 1,
  duration_us => 1500,
  repo => my_repo}
```

| Key | Type | Description |
|---|---|---|
| `query` | `binary()` | The compiled SQL string |
| `params` | `[term()]` | Bind parameters |
| `result` | `ok \| error` | Whether the query succeeded |
| `num_rows` | `integer()` | Number of rows returned or affected |
| `duration_us` | `integer()` | Wall-clock time in microseconds |
| `repo` | `module()` | The repo module that ran the query |

## How It Works

`pgo_query/3` in `kura_repo_worker` wraps every database call:

1. Record `erlang:monotonic_time(microsecond)` before the query
2. Execute the query via `pgo:query/3`
3. Record the time again after
4. Call `emit_log/5` with the duration

`emit_log/5` reads `application:get_env(kura, log)`, builds the event with `build_log_event/5`, and dispatches based on the config value. The entire call is wrapped in `try/catch` -- if the callback crashes, the failure is silently ignored so logging never breaks your queries.

## Default Logger

Setting `{log, true}` uses `kura_repo_worker:default_logger/0`, which logs via `logger:info`:

```
Kura SELECT u0."id", u0."name" FROM "users" AS u0 WHERE (u0."id" = $1) 1500us
```

## Examples

### Slow Query Logging

Create a handler module and configure via sys.config:

```erlang
-module(my_app_telemetry).
-export([slow_query_log/1]).

slow_query_log(#{duration_us := D} = Event) ->
    case D > 5000 of
        true -> logger:warning("Slow query (~pus): ~s", [D, maps:get(query, Event)]);
        false -> ok
    end.
```

```erlang
{kura, [{log, {my_app_telemetry, slow_query_log}}]}.
```

### Runtime Configuration

Set a fun at runtime for development/debugging:

```erlang
application:set_env(kura, log, fun(Event) ->
    logger:debug("DB: ~s ~p (~pus)", [
        maps:get(query, Event),
        maps:get(params, Event),
        maps:get(duration_us, Event)
    ])
end).
```

## Testing

`build_log_event/5` is exported from `kura_repo_worker` for direct testing:

```erlang
Event = kura_repo_worker:build_log_event(
    my_repo,
    <<"SELECT 1">>,
    [],
    #{num_rows => 1, rows => [{1}]},
    42
),
42 = maps:get(duration_us, Event),
ok = maps:get(result, Event).
```
