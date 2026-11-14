# Telemetry

Kura emits [`telemetry`](https://github.com/beam-telemetry/telemetry) events for every database query. You can attach any handler - logging, metrics, OpenTelemetry - without Kura knowing about it.

## Events

Kura emits a single event:

### `[kura, repo, query]`

Emitted after every query execution.

**Measurements:**

| Key | Type | Description |
|---|---|---|
| `duration` | `integer()` | Wall-clock time in native time units |
| `duration_us` | `integer()` | Wall-clock time in microseconds |

**Metadata:**

| Key | Type | Description |
|---|---|---|
| `query` | `binary()` | The compiled SQL string |
| `params` | `[term()]` | Bind parameters, with [redacted fields](#redacted-fields) replaced |
| `result` | `ok \| error` | Whether the query succeeded |
| `num_rows` | `integer()` | Number of rows returned or affected |
| `repo` | `module()` | The repo module that ran the query |
| `source` | `binary() \| undefined` | The table name extracted from the query |
| `tenant` | `undefined \| {prefix, binary()} \| {attribute, {atom(), term()}}` | Current tenant (if any) at query time |
| `error_reason` | `term() \| undefined` | The error term when `result =:= error`, cut down for a schema with redacted fields |

### Redacted Fields

Bind parameters carry the values your application wrote, so a handler that
logs or exports `params` copies them. A schema names the fields that must
not leave the process this way:

```erlang
-module(account).
-behaviour(kura_schema).
-export([table/0, fields/0, redact/0]).

redact() -> [password_hash, reset_token].
```

Encrypted fields are redacted without being listed. In `params`, and in
the legacy query log, their values are replaced by `~"[redacted]"`:

| Statement | What is replaced |
|---|---|
| `insert`, `update` and `delete` of a changeset, `insert_all` | the redacted fields' values |
| an upsert (`insert` with `on_conflict`) that writes a redacted field | every parameter |
| a query, `update_all` or `delete_all` that filters on or sets a redacted field | every parameter |
| any statement that touches no redacted field | nothing |

A query is judged by the schema it reads from. A subquery over another
schema is looked at when the outer schema has redacted fields of its own,
or the query has a CTE or a combination.

When a statement on a schema with redacted fields fails, `error_reason`
keeps the error's code and the constraint, table and column it names, and
drops its message and detail: a database quotes the offending value there,
or the whole failing row. The caller of the repo function still gets the
full error.

Not covered: raw SQL through `query/3`, and the database's own statement
log, which kura does not write.

For your own log lines, `kura_redact:record(Schema, Map)` and
`kura_redact:changeset(Changeset)` return copies with the same fields
replaced.

## Attaching Handlers

Attach handlers at application startup using `telemetry:attach/4`:

```erlang
%% In your application's start/2
start(_Type, _Args) ->
    telemetry:attach(
        ~"kura-logger",
        [kura, repo, query],
        fun ?MODULE:handle_query_event/4,
        #{}
    ),
    my_sup:start_link().

handle_query_event(_Event, Measurements, Metadata, _Config) ->
    logger:info("Kura ~s (~pus) [~p rows]", [
        maps:get(query, Metadata),
        maps:get(duration_us, Measurements),
        maps:get(num_rows, Metadata)
    ]).
```

### Slow Query Logging

```erlang
telemetry:attach(
    ~"kura-slow-queries",
    [kura, repo, query],
    fun(_, #{duration_us := D}, #{query := Q}, _) ->
        case D > 5000 of
            true -> logger:warning("Slow query (~pus): ~s", [D, Q]);
            false -> ok
        end
    end,
    #{}
).
```

### Metrics with telemetry_metrics

If you use [`telemetry_metrics`](https://github.com/beam-telemetry/telemetry_metrics), define metrics against the Kura event:

```erlang
[
    Telemetry.Metrics.distribution("kura.repo.query.duration",
        unit: {:native, :millisecond},
        tags: [:source, :result]
    ),
    Telemetry.Metrics.counter("kura.repo.query.count",
        tags: [:source, :result]
    )
]
```

### OpenTelemetry

Use [opentelemetry_kura](https://github.com/novaframework/opentelemetry_kura) to automatically create OpenTelemetry spans for every query:

```erlang
%% In your application's start/2
opentelemetry_kura:setup().
```

See the [opentelemetry_kura](https://github.com/novaframework/opentelemetry_kura) documentation for details.

## Legacy Log Configuration

For backward compatibility, the `{kura, [{log, ...}]}` application environment is still supported. When set, it runs **in addition to** telemetry events.

```erlang
%% sys.config
{kura, [
    {log, true}              %% Built-in logger (logger:info)
    %% {log, {M, F}}         %% Call M:F(Event) for each query
    %% {log, fun(E) -> ... end}  %% Runtime fun
]}.
```

The legacy log event map:

```erlang
#{query => ~"SELECT ...",
  params => [1],
  result => ok,
  num_rows => 1,
  duration_us => 1500,
  repo => my_repo}
```

> **Recommendation:** Prefer attaching `telemetry` handlers over the legacy `log` config. The telemetry approach is composable (multiple handlers), standard (same pattern as Ecto, Phoenix, etc.), and supports richer measurements.

## Testing

`build_telemetry_metadata/4` and `extract_source/1` are exported for testing:

```erlang
Meta = kura_repo_worker:build_telemetry_metadata(
    my_repo, ~"SELECT * FROM \"users\"", [], #{rows => []}
),
~"users" = maps:get(source, Meta).
```
