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
* **Unregistered receivers.** A consumer group sees the whole stream, so events this service does not handle
  are normal. What happens to a message whose receiver is not registered in this process is set by
  `BusConfiguration.Unregistered` (a warning is logged only when `ShowUnregistered` is set):
  * `UnregisteredAck` (default) — acknowledged and dropped without a trace in the dead-letter stream.
    Right when every instance of the group runs the same code.
  * `UnregisteredLeave` — left pending and redelivered after `RetryIdleTime`, possibly to an instance that has
    the receiver; after `NumRetries` deliveries it goes to the dead-letter stream (reason `unregistered`), that
    is after about `NumRetries × RetryIdleTime` (5 minutes by default) if no instance has the receiver.
    Use it for rolling deploys that add receivers: old instances then do not drop the new messages.
  * `UnregisteredDeadLetter` — moved to the dead-letter stream at once (reason `unregistered`).

  `Subscribe(ctx, title, factory)` is a synonym of `RegisterReceiver` that states the intent: "this service
  handles this event". Register all receivers before `Start`.
* **Receiver title.** A receiver that embeds `bus.Receiver` and has its own `Title() string` method is named
  in the logs automatically. A type built on another receiver can redefine `Title` again: the outermost
  definition is used, and without one the inherited title stays. The same holds for
  `DrawParams() []string` (`ParamsInterface`) on the receiver or on its payload.
  `bus.Receiver` has no `Execute` of its own: a type that forgets it does not compile as a factory result.
  `Finish` runs after every started execution, also after a panic. The factory must return a new receiver
  (with a new payload) on every call: receivers run concurrently. A `null` payload (`AddMessage` with `nil`)
  leaves the payload pointer set by the factory untouched.
* **Retries.** `NumRetries` (default 5) is the maximum number of executions of one message. The count is
  the delivery counter Redis keeps for every pending entry (`XPENDING`; `XREADGROUP` sets it to 1, every
  redelivery adds one), so it is atomic, needs no extra keys or TTL, works the same on a single node and in
  a cluster, and disappears when the message is acknowledged. A receiver can have its own limit:
  `RegisterReceiverWithOptions(ctx, title, factory, bus.ReceiverOptions{NumRetries: 2})`; zero means the
  group's value. If the counter cannot be read (Redis is briefly unavailable) the message is left pending and
  retried later instead of being discarded. A failed message is picked up again after it has been idle for
  `RetryIdleTime` seconds (default 60) without a sign of life from its worker (see Lease); this is a setting of the group only. A panic in a receiver is recovered
  and counts as a failure.
* **Lost group.** If Redis answers `NOGROUP` (the stream key was deleted or the group destroyed), the workers
  pause, create the group again and carry on; `Init` does the same when it finds no group. Redis cannot tell
  where the group was, so the bus marks a recreated stream: whoever creates the stream key through this library
  (a producer's `AddMessage`, `Init`, `EnsureGroups`, a worker) sets the key `{<stream>}:reborn` in the same
  atomic step, kept for `StreamRebirthTTL` (default 1 hour). A group created while the mark exists starts at the
  beginning of the stream: everything in it is new, nothing is lost. Without the mark only the group was
  destroyed: it starts at the end of the stream, so messages processed already do not run again.
  Limits: a stream created by a writer outside this library has no mark, and a service that notices the loss
  later than `StreamRebirthTTL` (for example it was down longer) starts at the end; set `StreamRebirthTTL` above
  the longest downtime. A failed read pauses the worker for 5 seconds, so an unreachable Redis does not turn the
  workers into a busy loop.
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
* **Stream size.** Every `AddMessage` trims the stream (`XADD MAXLEN ~`) to about `StreamSize` entries
  (default 10000), dropping the oldest ones, even if they have not been processed yet. The trimming is
  approximate: Redis removes whole nodes, so the stream may hold up to about a hundred entries more, which keeps
  every `AddMessage` cheap. Size the stream so
  that it holds more than the backlog you can have. This limit is hard: it is the only limit of the stream
  and it can remove an unprocessed message. There is no limit by age: how many days the stream holds follows
  from `StreamSize` and the publishing rate.
* **Lease.** A message that is being executed is not redelivered to another worker while its own worker is alive,
  however long the receiver takes. One goroutine per bus (none per message) looks at the running messages every
  `RetryIdleTime/6` and, for those that have run for `RetryIdleTime/3` since the start or the last renewal, resets the
  idle time Redis keeps (`XCLAIM ... JUSTID`, one script call for all of them; the delivery counter does not
  change, so a renewal is not a retry). The script renews a message only while it is still pending for the worker
  that runs it; a message that was acknowledged, trimmed or taken over is not touched and a warning is logged.
  A message that finishes sooner than `RetryIdleTime/3` costs nothing. So `RetryIdleTime` means how long a worker may
  be silent: if the process dies, or loses Redis for that long, nobody renews and the message is redelivered. A
  receiver may therefore run far longer than `RetryIdleTime` without being executed twice at once; what remains is
  the usual at-least-once duplicate after a crash or a long network failure.
* **Receiver time limit.** `HandlerTimeout` is the longest time one receiver may run for one message (default
  `10 * RetryIdleTime`, `-1` switches the limit off). It is independent of `RetryIdleTime`. When the time is up the
  receiver's context is cancelled (its cause is `bus.ErrorHandlerTimeout`), `Stats().TimedOut` grows and the lease is
  no longer renewed, so the message is redelivered `RetryIdleTime` later at the latest. When the receiver returns an
  error, the run counts as a failure (`Stats().Failed`) and the message is retried like after any failure, or goes
  to the dead-letter stream when its retries are exhausted. When it returns nil late, the work is done: the message
  is acknowledged and not executed again. The receiver runs in the worker's own
  goroutine: there are never more receivers running than `WorkersCount`, so a hanging receiver cannot pile up
  goroutines or connections. The price: Go cannot kill a goroutine, so a receiver that ignores its context keeps its
  worker until it returns on its own, and `Stats().Stuck` shows how many workers are held that way (when it equals
  the number of workers, nothing is processed: alert on it). Receivers should pass their `ctx` to every call that can
  block. With the limit off a receiver that hangs and ignores its context holds its worker and its message for ever.
  What a run past its limit that then fails has already done (a database write, a request sent) stays done and the
  message is executed again; a message redelivered while the late run was still going runs twice. So receivers must
  be idempotent, as with any retry (`GetID()` is the key).
* **Initialisation.** `Init(ctx, &bus.BusConfiguration{...})` connects to Redis and creates the bus' group; call
  it once. Receivers may be registered before or after it. A second `Init` after a successful one (or while
  another runs) returns `bus.ErrorAlreadyInitialized` and changes nothing; a failed `Init` (for example Redis is
  unreachable) closes its connection and may be retried; `Init(ctx, nil)` returns `bus.ErrorNilConfiguration`.
  Until `Init` has succeeded, `AddMessage`, `EnsureGroups`, `Counters` and `Snapshot` return
  `bus.ErrorNotInitialized` and `Start` logs an error and returns at once; `Stop` and `Close` are safe in any state.
* **Stopping.** `bus.Stop(ctx)` makes the workers stop reading new messages and waits until the messages being
  processed are finished and acknowledged; a worker that is blocked on an empty stream notices within a second.
  Messages that were not read stay in the stream for the next run. If `ctx` expires first, the context of the
  running receivers is cancelled (they may abort), `Stop` returns an error wrapping `bus.ErrorStopTimeout` and
  `ctx.Err()` at once and does not wait for a receiver that ignores its context: its message was not acknowledged,
  so it stays pending and is redelivered after `RetryIdleTime` (or is acknowledged when the receiver finally
  succeeds). After such a timeout nothing new is executed: a read that was already blocked may still bring a message,
  it is left pending. `Stop` is safe to repeat and to call concurrently; `Start` returns when the bus has stopped, and
  does nothing after `Stop` or when the bus is already running; the bus cannot be restarted. `bus.Close(ctx)` is `Stop` plus closing the Redis connection.
  `Start` also reacts to its own context and to SIGINT/SIGTERM. By default this is a hard stop:
  the context of the running receivers is cancelled at once and their messages stay pending. Set
  `ShutdownTimeout` (for example `30 * time.Second`) to make it graceful: running receivers then get that long to
  finish with an uncancelled context, after which they are cancelled and `Start` returns. Prefer this setting for
  services that handle SIGTERM through `Start`; call `Stop` yourself for a custom timeout or in tests.
* **Hot path.** One message costs two Redis commands: `XREADGROUP` (one message, 1 s block) and the acknowledgement (`XACK` and the counters in one script call). A message that
  was just read has been delivered exactly once, so its delivery counter is not requested (`XPENDING`); the counter is
  not read for a redelivered one either, it comes with the search for idle messages. Idle messages are looked for at
  most every `min(RetryIdleTime/4, 1 s)` across all workers, and back to back while the search keeps finding them,
  instead of before every read of every worker. So a message is redelivered after `RetryIdleTime` plus at most that
  interval. The search is `XPENDING ... IDLE <RetryIdleTime>` (Redis picks the idle entries from the whole pending
  list) followed by `XCLAIM ... MINIDLE` for one of them, which is atomic: if another worker took the entry first,
  the claim returns nothing and the next candidate is tried. `XAUTOCLAIM` is deliberately not used: it looks only at
  the first `Count*10` entries of the pending list (ordered by entry id), so an idle message behind ten recently
  redelivered ones would not be found until those are acknowledged or dead-lettered. Measured
  on a local Redis, 300 messages and 4 workers: 4.0 commands per message before, 2.0 after
  (`go test -run '^$' -bench HotPath`, `TestHotPath_CommandsPerMessage`).
* **Deduplication of publishing.** Some event sources (for example webhooks without a unique id) can deliver
  the same event twice. `AddMessage(ctx, receiver, payload, key)` takes a deduplication key chosen by the producer (for example a hash of the raw request body): the message is added only if no message
  with the same key and receiver was published within `DedupTTL` (a `BusConfiguration` setting, default 10 minutes,
  counted from the first publication; duplicates do not extend it). For a duplicate `AddMessage` returns the id of
  the first message and a `nil` error, so callers need not care, and `Stats().Deduplicated` is incremented. With an
  empty key the message is added unconditionally. Check and `XADD` are one Lua script, so concurrent producers
  on different hosts cannot both add it. Each remembered key is one small Redis key
  `{<stream>}:dedup:<sha256(receiver, key)>` (about 200 bytes, the braces keep it in the stream's cluster slot)
  that expires by itself, so Redis holds only the keys of the last `DedupTTL`; lower `DedupTTL` to the real window
  of duplicates to keep fewer. Deduplication covers publishing only: a message that was added and then trimmed or
  dead-lettered is still remembered until its key expires, and receivers must stay idempotent (delivery is at least
  once). Keys with a TTL can be evicted early under `maxmemory-policy volatile-*` when memory is short. Stream names
  with unbalanced braces are rejected with `ErrorDedupStreamName`.
* **Message format.** Fields `receiver`, `id` (UUID), `payload` (JSON) and `time` (RFC 3339 with nanoseconds,
  UTC). A receiver that embeds `Receiver` reads them with `GetID()`, `GetTime()`, `GetName()` and `GetPayload()`;
  `GetID()` is the same for every delivery of a message, use it to make the receiver idempotent.
* **Memory policy.** Use `maxmemory-policy noeviction` (or a `volatile-*` policy) for the Redis that holds the bus.
  Under `allkeys-*` Redis may evict the stream itself: that is the same as deleting it, and the messages that were
  delivered but not acknowledged yet (the pending list lives in the stream key) are lost; no mark can bring them back.
* **Redis.** A single node and a cluster (comma-separated `Hosts`) are supported and tested. Redis 7.0 or newer
  is required (`XPENDING ... IDLE`, `XADD ... MINID`, the group lag); older servers are not supported.

# Monitoring

The bus keeps its counters in Redis, the way asynq does: they are shared by all instances of a service, survive
restarts and can be read by any process (a monitoring service, a CLI, an admin endpoint) without touching the
workers. Nothing is pushed anywhere: you read the numbers when you need them.

`bus.Counters(ctx, group, days)` returns the counters of the last `days` days (UTC, today first, at most 366)
for a consumer group (empty = the bus' own group), as a total and per receiver:

| Counter | Meaning |
| --- | --- |
| `Published` | messages added to the stream (by every producer; the same for every group) |
| `Deduplicated` | publications skipped as duplicates (`AddMessage` with a dedup key) |
| `Received` | deliveries to the group whose handling has finished, whatever the outcome, except `Duplicates` |
| `Processed` | the receiver succeeded and the message was acknowledged |
| `Failed` | executions that failed to start, returned an error or panicked (a message that fails three times counts three) |
| `Retried` | executions of a message that had been delivered before |
| `DeadLettered` | messages moved to the dead-letter stream |
| `Unregistered` | messages for receivers this service has not registered (acknowledged and ignored) |
| `Invalid` | messages without a valid receiver name |
| `Duplicates` | successful runs whose message had been acknowledged by another run already (a late success after `HandlerTimeout`, or an at-least-once duplicate); not counted in `Received` or `Processed` |

`bus.Snapshot(ctx)` returns the state right now: `Size` of the stream against `SizeLimit` (the `StreamSize` of this
bus: at the limit every new message pushes the oldest one out, processed or not), `DeadLetterSize`, and for **every**
consumer group of the stream `Pending` (delivered, not acknowledged), `OldestPending` (age of the oldest pending
message), `Lag` (not delivered yet; `-1` when Redis cannot compute it, for example after entries were deleted), `LastDeliveredID`, `LiveConsumers` (how many workers
run right now, over all instances on all servers) and `Instances` (the running processes: `host/pid/suffix`,
workers, time since the last heartbeat). A few Redis commands, plus one per
group with pending messages. `LiveConsumers` equal to 0 with a growing `Lag` means nobody consumes the group. A group whose `Pending` or `OldestPending` only grows is stuck; a `Size` close to
`SizeLimit` means messages are about to be lost.

How it works. Counters are hashes per day, `{<stream>}:stats:g:<group>:<YYYYMMDD>` (consumers) and
`{<stream>}:stats:p:<YYYYMMDD>` (producers); a hash has the field `<counter>` for the total and
`<counter>:<receiver>` per receiver. They are updated in the same script call as `XACK` and `XADD`, so a message
still costs two round trips (`XREADGROUP` and the acknowledgement) and the counters are consistent with the stream;
a failed execution costs one more call. The braces keep the keys in the stream's cluster slot. A problem with a counter
(for example a key of a wrong type) never fails a publication or leaves a message unacknowledged. Counters expire after
`StatsRetention` (default 30 days, a negative value switches them off). Receivers
that are not registered in the process get their own counters only for the first 500 names, the rest is counted as
`(other)`. `Received` is counted when the handling of a delivery finishes, so a message that is being executed now, or
whose worker has died, shows up when it finishes or is redelivered.

**Live consumers.** The consumer records of a Redis stream group are not a count of live workers: a record stays
in the group after its process is gone (and a worker gets a new name at every start), and the idle time Redis reports
grows for a live worker that is busy with a long receiver. So the bus keeps its own registry: while `Start` runs,
every instance renews its workers in the sorted set `{<stream>}:alive:<group>` every 5 seconds (one Redis call per
instance, nothing per message; the time is the Redis server's, so servers with different clocks agree). A worker not
renewed for 15 seconds is dead, so a crashed server drops out of `LiveConsumers` within that time; a graceful `Stop`
removes the instance at once. The same loop deletes the consumer records of dead workers from the group (nothing
pending, silent for 10 minutes, not in the registry; a record with pending messages is never deleted, because that
would drop its messages), so the stream does not keep thousands of them either. A group is shown with 0 live consumers
until its services run a version of the library with the registry.

`bus.Stats()` returns the in-process counters of this instance (reset on restart), including `TimedOut` and `Stuck`
(see "Receiver time limit"); the history and the state of the whole stream are `Counters` and `Snapshot`.

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
