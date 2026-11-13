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
  need different `Group` names on the same `Stream`. A new group starts from the messages added after
  it was created (`$`).
* **Unregistered receivers are acknowledged and ignored.** If a message names a receiver that is not
  registered in this process, it is acknowledged and dropped (a warning is logged only when
  `ShowUnregistered` is set). Register all receivers before `Start`; a message that arrives while
  the receiver is missing is lost for this group.
* **Retries.** `NumRetries` (default 5) is the maximum number of executions of one message. The count is
  the delivery counter Redis keeps for every pending entry (`XPENDING`; `XREADGROUP` sets it to 1, every
  `XAUTOCLAIM` adds one), so it is atomic, needs no extra keys or TTL, works the same on a single node and in
  a cluster, and disappears when the message is acknowledged. A message that was delivered more than
  `NumRetries` times is acknowledged without being executed. If the counter cannot be read (Redis is briefly
  unavailable) the message is left pending and retried later instead of being discarded. A failed message
  is picked up again after it has been idle for `RetryIdleTime` seconds (default 60).
  `SetRetry`, `GetRetry` and `DelRetry` operate on the legacy `<stream>:retry:<id>` keys, are deprecated and
  no longer influence retries.
* **Stream size.** Every `AddMessage` trims the stream exactly (`XADD MAXLEN`) to `StreamSize` entries
  (default 10000), dropping the oldest ones, even if they have not been processed yet. Size the stream so
  that it holds more than the backlog you can have.
* **Message format.** Fields `receiver`, `id` (UUID), `payload` (JSON) and `time` (RFC 3339 with nanoseconds,
  UTC). `Receiver` exposes the time to the receiver; the legacy field `event` is read if `time` is absent.
* **Redis.** A single node and a cluster (comma-separated `Hosts`) are supported and tested.

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
