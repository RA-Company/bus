## Bus 
Simple bus for asyncronus communication between microservices. It uses Redis streams for communication.

# Semantics

The bus is a thin layer over a Redis Stream and one consumer group. Know these rules before
building on it:

* **Delivery is "at least once".** A message is acknowledged (`XACK`) only after the receiver's
  `Execute` returned `nil`. If the receiver fails, panics, or the worker dies, the message stays
  pending and is redelivered, so a receiver can run more than once for the same message and must
  be idempotent. Acknowledgement is done even if the bus context was cancelled meanwhile, and a
  failed `XACK` is logged (with the message id) and returned, never silently ignored.
* **There is no ordering.** Messages are processed by `WorkersCount` workers in parallel, and a retried
  message is delivered after newer ones. Do not rely on the order of messages.
* **A consumer group is a subscription.** All instances that use the same `Stream` and `Group` share
  the messages (each message goes to one of them). Two services that must both receive every message
  need different `Group` names on the same `Stream`. `Init` creates the bus' own group at the end of
  the stream (`$`), so it sees only messages added after it was created. To make sure other services
  do not miss events published before they first started, the producer calls `EnsureGroups(ctx, "service-a", ...)`:
  it creates the missing groups (and the stream) at position `0`; existing groups are left untouched.
* **Unregistered receivers are acknowledged and ignored.** A consumer group sees the whole stream, so events
  this service does not handle are normal. If a message names a receiver that is not registered in this
  process, it is acknowledged and dropped without a trace in the dead-letter stream (a warning is logged
  only when `ShowUnregistered` is set). `Subscribe(ctx, title, factory)` is a synonym of `RegisterReceiver`
  that states the intent: "this service handles this event". Register all receivers before `Start`; a message
  that arrives while its receiver is missing is lost for this group.
* **Retries.** `NumRetries` (default 5) is the maximum number of executions of one message. The count is
  the delivery counter Redis keeps for every pending entry (`XPENDING`; `XREADGROUP` sets it to 1, every
  `XAUTOCLAIM` adds one), so it is atomic, needs no extra keys or TTL, works the same on a single node and in
  a cluster, and disappears when the message is acknowledged. A receiver can have its own limit:
  `RegisterReceiverWithOptions(ctx, title, factory, bus.ReceiverOptions{NumRetries: 2})`; zero means the
  group's value. If the counter cannot be read (Redis is briefly unavailable) the message is left pending and
  retried later instead of being discarded. A failed message is picked up again after it has been idle for
  `RetryIdleTime` seconds (default 60); this is a setting of the group only. A panic in a receiver is recovered
  and counts as a failure. `SetRetry`, `GetRetry` and `DelRetry` operate on the legacy `<stream>:retry:<id>`
  keys, are deprecated and no longer influence retries.
* **Dead-letter.** A message that has been delivered more than its limit allows, or whose receiver returned
  `bus.Permanent(err)`, is copied to the dead-letter stream and then acknowledged. The stream is
  `<Stream>:dead` unless `DeadLetterStream` is set. An entry keeps `receiver`, `id`, `payload` and `time`
  unchanged and adds `dead_reason` (`retries_exhausted` or `permanent`), `dead_error`, `dead_stream`,
  `dead_group`, `dead_message_id` (entry id in the original stream), `dead_deliveries` and `dead_at`.
  If the copy cannot be written, the message is not acknowledged and stays pending, so it is not lost; if the
  process dies between the copy and the acknowledgement, the entry can appear twice. Entries are kept for
  `DeadLetterRetention` (any duration, default 30 days; trimmed with `XADD ... MINID ~` when a new entry is
  written, so trimming is approximate and happens only on writes) and at most `DeadLetterSize` (default 10000).
  Nothing reads the dead-letter stream automatically; inspect it and re-publish with `AddMessage`.
* **Permanent errors.** `bus.Permanent(err)` marks an error that retrying cannot fix (for example the payload
  is invalid): the message goes to the dead-letter stream at once, without retries. `bus.IsPermanent(err)` tells
  whether an error is marked; `errors.Is` / `errors.As` still see the wrapped error.
* **Stream size.** Every `AddMessage` trims the stream exactly (`XADD MAXLEN`) to `StreamSize` entries
  (default 10000), dropping the oldest ones, even if they have not been processed yet. Size the stream so
  that it holds more than the backlog you can have.
* **Message format.** Fields `receiver`, `id` (UUID), `payload` (JSON) and `time` (RFC 3339 with nanoseconds,
  UTC). `Receiver` exposes the time to the receiver; the legacy field `event` is read if `time` is absent.
* **Redis.** A single node and a cluster (comma-separated `Hosts`) are supported and tested. Redis 6.2 or newer
  is required (`XAUTOCLAIM`, `XADD ... MINID`); `Lag` needs 7.0.

# Metrics

`bus.Stats()` returns a snapshot of in-process counters: `processed` (receiver succeeded and the message was
acknowledged), `retried` (executions of a message that had been delivered before), `failed` (executions that
failed to start, returned an error or panicked; a message that fails three times counts three), `dead_lettered`
(messages moved to the dead-letter stream), `ack_errors` (failed `XACK`, the message will be redelivered) and
`in_flight` (messages being executed now). Counters live in memory and reset when the process restarts.

`bus.Lag(ctx)` returns how many messages of the stream the group has not been delivered yet (Redis `XINFO GROUPS`
`lag`). Delivered but unacknowledged messages are not counted. It returns `-1` and `bus.ErrorLagUnavailable` on
Redis older than 7.0 or when Redis cannot compute the lag.

For graphs and alerts implement `bus.MetricsHook` (embed `bus.NopMetricsHook` to implement only some methods) and
install it with `SetMetricsHook` before `Start`; it is called for every processed, retried, failed, dead-lettered
and unacknowledged message, with the receiver name and, where it makes sense, the duration. Hooks run on worker
goroutines, so keep them fast; a panic in a hook is recovered and logged.

# Running tests

Tests need a running Redis and cover both topologies in one run: every Redis test is executed as
subtests `single` and `cluster`. A mode whose variables are not set is skipped.

| Variable | Meaning |
| --- | --- |
| `REDIS_HOST` | single node, `host:port` (`REDIS_HOSTS` is accepted as an alias) |
| `REDIS_CLUSTER` | comma-separated cluster nodes |
| `REDIS_PASSWORD` | password, used in both modes (`REDIS_PWD` is accepted as an alias) |
| `REDIS_DB` | database number for the single node, default 0 |

Tests use a unique stream per test and remove it afterwards. `make tests` reads these variables from
`.test.env`.

```
make tests
```

# Staying up to date
To update library to the latest version, use go get -u github.com/ra-company/bus.

# Supported go versions
We currently support the most recent major Go versions from 1.26.4 onward.

# License
MIT — see [LICENSE](LICENSE).

# Dependencies

**Runtime**

| Library | Purpose | License |
|---------|---------|---------|
| [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) | Redis client | BSD-2-Clause |
| [github.com/google/uuid](https://github.com/google/uuid) | RFC 4122 UUID generation | BSD-3-Clause |
| [github.com/ra-company/database](https://github.com/ra-company/database) | Redis client wrapper with logging | MIT |
| [github.com/ra-company/logging](https://github.com/ra-company/logging) | Structured logger | MIT |

**Tests only**

| Library | Purpose | License |
|---------|---------|---------|
| [github.com/stretchr/testify](https://github.com/stretchr/testify) | Test assertions | MIT |
| [github.com/brianvoe/gofakeit](https://github.com/brianvoe/gofakeit) | Random test data | MIT |
| [github.com/ra-company/env](https://github.com/ra-company/env) | Environment variables for tests | MIT |
