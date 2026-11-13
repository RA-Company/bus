package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/database/redis"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

var errInjected = errors.New("injected redis failure")

// faultHook is a go-redis hook that fails selected commands on the bus' own
// Redis connection. It lets a test simulate a short Redis outage that hits only
// some commands, while the test itself keeps inspecting Redis through a plain,
// hook-free client.
type faultHook struct {
	active atomic.Bool
	mu     sync.RWMutex
	deny   func(cmd string) bool
}

func (h *faultHook) set(deny func(cmd string) bool) {
	h.mu.Lock()
	h.deny = deny
	h.mu.Unlock()
	h.active.Store(deny != nil)
}

func (h *faultHook) failing(cmd string) bool {
	if !h.active.Load() {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.deny != nil && h.deny(cmd)
}

func (h *faultHook) DialHook(next re.DialHook) re.DialHook { return next }

func (h *faultHook) ProcessHook(next re.ProcessHook) re.ProcessHook {
	return func(ctx context.Context, cmd re.Cmder) error {
		if h.failing(cmd.Name()) {
			return errInjected
		}
		return next(ctx, cmd)
	}
}

func (h *faultHook) ProcessPipelineHook(next re.ProcessPipelineHook) re.ProcessPipelineHook {
	return func(ctx context.Context, cmds []re.Cmder) error {
		for _, cmd := range cmds {
			if h.failing(cmd.Name()) {
				return errInjected
			}
		}
		return next(ctx, cmds)
	}
}

// denyAllExceptStreamDelivery simulates an outage in which stream delivery and
// acknowledgement still work but every other command (counters, reads, scripts)
// fails.
func denyAllExceptStreamDelivery(cmd string) bool {
	switch cmd {
	case "xack", "xadd", "xreadgroup", "xautoclaim", "xgroup", "hello", "ping", "client", "info":
		return false
	}
	return true
}

// denyAck simulates a failing XACK only.
func denyAck(cmd string) bool { return cmd == "xack" }

// captureLogger records everything the bus logs so tests can make assertions on it.
type captureLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	level string
	text  string
}

func (c *captureLogger) record(level string, formatted bool, args []any) {
	if len(args) > 0 {
		if _, ok := args[0].(context.Context); ok {
			args = args[1:]
		}
	}
	var text string
	if formatted && len(args) > 0 {
		if format, ok := args[0].(string); ok {
			text = fmt.Sprintf(format, args[1:]...)
		}
	} else {
		text = fmt.Sprint(args...)
	}
	c.mu.Lock()
	c.entries = append(c.entries, logEntry{level: level, text: text})
	c.mu.Unlock()
}

func (c *captureLogger) Debug(args ...any)  { c.record("debug", false, args) }
func (c *captureLogger) Debugf(args ...any) { c.record("debug", true, args) }
func (c *captureLogger) Info(args ...any)   { c.record("info", false, args) }
func (c *captureLogger) Infof(args ...any)  { c.record("info", true, args) }
func (c *captureLogger) Warn(args ...any)   { c.record("warn", false, args) }
func (c *captureLogger) Warnf(args ...any)  { c.record("warn", true, args) }
func (c *captureLogger) Error(args ...any)  { c.record("error", false, args) }
func (c *captureLogger) Errorf(args ...any) { c.record("error", true, args) }
func (c *captureLogger) Fatal(args ...any)  { c.record("fatal", false, args) }
func (c *captureLogger) Fatalf(args ...any) { c.record("fatal", true, args) }

// errorsMentioning returns error-level entries that contain every given fragment
// (case-insensitive).
func (c *captureLogger) errorsMentioning(fragments ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.entries {
		if e.level != "error" {
			continue
		}
		low := strings.ToLower(e.text)
		match := true
		for _, f := range fragments {
			if !strings.Contains(low, strings.ToLower(f)) {
				match = false
				break
			}
		}
		if match {
			out = append(out, e.text)
		}
	}
	return out
}

// testEnv is a Bus on a unique stream plus a plain Redis client for inspection.
type testEnv struct {
	t    *testing.T
	b    *Bus
	rdb  *redis.RedisClient
	hook *faultHook
	log  *captureLogger
}

func newTestEnv(t *testing.T, mutate func(*BusConfiguration)) *testEnv {
	t.Helper()
	rdb, cfg := redisStart(t)

	bc := BusConfiguration{
		Redis:         *cfg,
		Stream:        "bus-test-" + uuid.NewString(),
		Group:         "group",
		WorkersCount:  2,
		NumRetries:    3,
		RetryIdleTime: 1,
	}
	if mutate != nil {
		mutate(&bc)
	}

	b := &Bus{}
	require.NoError(t, b.Init(t.Context(), &bc))
	t.Cleanup(func() {
		ctx := context.Background()
		_ = rdb.XGroupDestroy(ctx, b.stream, b.group)
		_ = rdb.Del(ctx, b.stream)
		if keys, err := scanAll(ctx, rdb.Client(), b.stream+":retry:*"); err == nil {
			for _, k := range keys {
				_ = rdb.Del(ctx, k)
			}
		}
	})

	env := &testEnv{t: t, b: b, rdb: rdb, hook: &faultHook{}, log: &captureLogger{}}
	// Both *redis.Client and *redis.ClusterClient accept hooks, so the fault
	// injection works against a single node and against a cluster.
	hooker, ok := b.redis.Client().(interface{ AddHook(re.Hook) })
	require.True(t, ok, "Redis client does not support hooks")
	hooker.AddHook(env.hook)
	b.SetLogger(env.log)
	return env
}

// scanAll returns all keys matching pattern on a single node or on every master
// of a cluster.
func scanAll(ctx context.Context, cl re.Cmdable, pattern string) ([]string, error) {
	scanNode := func(ctx context.Context, c *re.Client) ([]string, error) {
		var keys []string
		var cursor uint64
		for {
			batch, next, err := c.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return nil, err
			}
			keys = append(keys, batch...)
			if cursor = next; cursor == 0 {
				return keys, nil
			}
		}
	}

	if cluster, ok := cl.(*re.ClusterClient); ok {
		var mu sync.Mutex
		var all []string
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, c *re.Client) error {
			keys, err := scanNode(ctx, c)
			mu.Lock()
			all = append(all, keys...)
			mu.Unlock()
			return err
		})
		return all, err
	}
	return scanNode(ctx, cl.(*re.Client))
}

// scriptReceiver is a receiver whose Execute is supplied by the test.
type scriptReceiver struct {
	Receiver
	run func(ctx context.Context, r *scriptReceiver) error
}

func (s *scriptReceiver) Execute(ctx context.Context) error { return s.run(ctx, s) }

func (e *testEnv) register(name string, run func(ctx context.Context, r *scriptReceiver) error) {
	e.b.RegisterReceiver(e.t.Context(), name, func() ReceiverInterface { return &scriptReceiver{run: run} })
}

// add publishes a message through the public API.
func (e *testEnv) add(receiver string) string {
	e.t.Helper()
	id, err := e.b.AddMessage(e.t.Context(), receiver, ReceiverPayload{Title: "t", Foo: "f"})
	require.NoError(e.t, err)
	return id
}

// deliver reads the next new message of the group with a plain client, which
// makes it pending exactly like a worker's XREADGROUP would.
func (e *testEnv) deliver() re.XMessage {
	e.t.Helper()
	streams, err := e.rdb.XReadGroup(e.t.Context(), &re.XReadGroupArgs{
		Group:    e.b.group,
		Consumer: "test-consumer",
		Streams:  []string{e.b.stream, ">"},
		Count:    1,
		Block:    2 * time.Second,
	})
	require.NoError(e.t, err)
	require.Len(e.t, streams, 1, "expected a new message to be delivered")
	require.Len(e.t, streams[0].Messages, 1)
	return streams[0].Messages[0]
}

// redeliver simulates XAUTOCLAIM of an idle message by another consumer: it bumps
// the delivery counter exactly like a real redelivery. It returns false when the
// message is no longer pending.
func (e *testEnv) redeliver(id string) (re.XMessage, bool) {
	e.t.Helper()
	msgs, err := e.rdb.Client().XClaim(e.t.Context(), &re.XClaimArgs{
		Stream:   e.b.stream,
		Group:    e.b.group,
		Consumer: "test-consumer-2",
		MinIdle:  0,
		Messages: []string{id},
	}).Result()
	require.NoError(e.t, err)
	if len(msgs) == 0 {
		return re.XMessage{}, false
	}
	return msgs[0], true
}

func (e *testEnv) pending() int64 {
	e.t.Helper()
	res, err := e.rdb.Client().XPending(e.t.Context(), e.b.stream, e.b.group).Result()
	require.NoError(e.t, err)
	return res.Count
}

func (e *testEnv) retryKeys() []string {
	e.t.Helper()
	keys, err := scanAll(e.t.Context(), e.rdb.Client(), e.b.stream+":retry:*")
	require.NoError(e.t, err)
	return keys
}

// startBus runs the real worker loop and returns a function that stops it and
// waits until all workers are gone.
func (e *testEnv) startBus() (stop func()) {
	ctx, cancel := context.WithCancel(e.t.Context())
	done := make(chan struct{})
	go func() {
		e.b.Start(ctx)
		close(done)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				e.t.Error("bus did not stop in time")
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// ---------------------------------------------------------------------------
// Characterization tests: describe the delivery contract and must pass both
// before and after the defect fixes.
// ---------------------------------------------------------------------------

// A message is acknowledged only after the receiver succeeded.
func redisDelivery_AckOnlyAfterSuccess(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.register("fail", func(context.Context, *scriptReceiver) error { return errors.New("boom") })

	env.add("fail")
	env.add("ok")
	failMsg := env.deliver()
	okMsg := env.deliver()
	require.EqualValues(t, 2, env.pending())

	require.Error(t, env.b.processMessage(t.Context(), failMsg))
	require.EqualValues(t, 2, env.pending(), "a failed message must stay pending")

	require.NoError(t, env.b.processMessage(t.Context(), okMsg))
	require.EqualValues(t, 1, env.pending(), "a successful message must be acknowledged")

	ext, err := env.rdb.Client().XPendingExt(t.Context(), &re.XPendingExtArgs{
		Stream: env.b.stream, Group: env.b.group, Start: "-", End: "+", Count: 10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, ext, 1)
	require.Equal(t, failMsg.ID, ext[0].ID, "the failed message is the one left pending")
}

// A message that fails is redelivered by the workers until it succeeds.
func redisDelivery_RetryAfterFailure(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, nil)
	var runs atomic.Int32
	env.register("flaky", func(context.Context, *scriptReceiver) error {
		if runs.Add(1) < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})
	stop := env.startBus()
	env.add("flaky")

	require.Eventually(t, func() bool { return runs.Load() == 3 && env.pending() == 0 },
		20*time.Second, 200*time.Millisecond, "message should succeed on the third attempt and be acknowledged")
	stop()
	require.EqualValues(t, 3, runs.Load(), "no executions after success")
}

// NumRetries is the maximum number of executions; after that the message is
// acknowledged without being executed again.
func redisDelivery_RetryLimit(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 3 })
	var runs atomic.Int32
	env.register("always-fails", func(context.Context, *scriptReceiver) error {
		runs.Add(1)
		return errors.New("permanent failure")
	})
	stop := env.startBus()
	env.add("always-fails")

	require.Eventually(t, func() bool { return runs.Load() >= 3 && env.pending() == 0 },
		30*time.Second, 200*time.Millisecond, "message should be discarded after NumRetries executions")
	stop()
	require.EqualValues(t, 3, runs.Load(), "receiver must run exactly NumRetries times")
}

// The stream is trimmed exactly to StreamSize, keeping the newest entries.
func redisDelivery_StreamTrimmedToMaxLen(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StreamSize = 5 })

	var ids []string
	for range 20 {
		ids = append(ids, env.add("whatever"))
	}

	n, err := env.rdb.Client().XLen(t.Context(), env.b.stream).Result()
	require.NoError(t, err)
	require.EqualValues(t, 5, n)

	entries, err := env.rdb.Client().XRange(t.Context(), env.b.stream, "-", "+").Result()
	require.NoError(t, err)
	var kept []string
	for _, e := range entries {
		kept = append(kept, e.Values["id"].(string))
	}
	require.Equal(t, ids[15:], kept, "the newest five messages must be kept")
}

// A message for a receiver nobody registered is acknowledged and ignored, and
// does not hold up messages behind it.
func redisDelivery_UnregisteredReceiverIsAcknowledged(t *testing.T) {
	t.Run("processMessage", func(t *testing.T) {
		env := newTestEnv(t, nil)
		env.add("ghost")
		msg := env.deliver()
		require.EqualValues(t, 1, env.pending())

		err := env.b.processMessage(t.Context(), msg)
		require.ErrorIs(t, err, ErrorReceiverNotRegistered)
		require.EqualValues(t, 0, env.pending())
	})

	t.Run("workers", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t, nil)
		var runs atomic.Int32
		env.register("ok", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })
		stop := env.startBus()
		env.add("ghost")
		env.add("ok")

		require.Eventually(t, func() bool { return runs.Load() == 1 && env.pending() == 0 },
			15*time.Second, 200*time.Millisecond)
		stop()
	})
}

// A panic in Execute is recovered, the worker keeps going, the message is not
// acknowledged by the panicking attempt and is retried up to NumRetries times.
func redisDelivery_PanicInExecute(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 3 })
	var panics, oks atomic.Int32
	env.register("panics", func(context.Context, *scriptReceiver) error {
		panics.Add(1)
		panic("simulated panic")
	})
	env.register("ok", func(context.Context, *scriptReceiver) error { oks.Add(1); return nil })
	stop := env.startBus()
	env.add("panics")
	env.add("ok")

	require.Eventually(t, func() bool { return oks.Load() == 1 }, 15*time.Second, 100*time.Millisecond,
		"a panicking message must not stop the workers")
	require.Eventually(t, func() bool { return panics.Load() >= 3 && env.pending() == 0 },
		30*time.Second, 200*time.Millisecond, "the panicking message is retried and finally discarded")
	stop()
	require.EqualValues(t, 3, panics.Load())
	require.EqualValues(t, 0, env.b.inFlight.Load(), "in-flight counter must be balanced after panics")
}

// Receiver time fallbacks that existed before the "time" field was honoured.
func TestReceiver_PreloadVariables_EventFallbacks(t *testing.T) {
	t.Run("legacy event field is still read", func(t *testing.T) {
		at := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC)
		r := &Receiver{}
		_, err := r.PreloadVariables(t.Context(), map[string]any{
			"receiver": "r", "event": at.Format(time.RFC3339Nano),
		})
		require.NoError(t, err)
		require.True(t, at.Equal(r.event), "got %v", r.event)
	})
}

// ---------------------------------------------------------------------------
// Defect 1.1: an unreadable retry counter must not discard the message.
// ---------------------------------------------------------------------------

func redisDefect_CounterReadFailureKeepsMessagePending(t *testing.T) {
	env := newTestEnv(t, nil)
	var runs atomic.Int32
	env.register("ok", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })
	env.add("ok")
	msg := env.deliver()

	env.hook.set(denyAllExceptStreamDelivery) // short Redis outage for everything but XACK/XADD/XREADGROUP/XAUTOCLAIM
	err := env.b.processMessage(t.Context(), msg)
	env.hook.set(nil) // outage over

	require.Error(t, err, "failure to read the retry counter must be reported")
	require.Zero(t, runs.Load(), "receiver must not run when the counter is unknown")
	require.EqualValues(t, 1, env.pending(), "the message must stay pending, not be acknowledged and lost")

	// Once Redis is back the very same message is processed normally.
	require.NoError(t, env.b.processMessage(t.Context(), msg))
	require.EqualValues(t, 1, runs.Load())
	require.EqualValues(t, 0, env.pending())
}

// ---------------------------------------------------------------------------
// Defect 1.2: the delivery counter must live in Redis next to the message, not in
// an auxiliary key. Losing every auxiliary key (expiry, eviction, failover) must
// not turn a failing message into an endless retry loop.
// ---------------------------------------------------------------------------

func redisDefect_RetryCountDoesNotDependOnAuxiliaryKeys(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 3; c.RetryIdleTime = 600 })
	var runs atomic.Int32
	env.register("always-fails", func(context.Context, *scriptReceiver) error {
		runs.Add(1)
		return errors.New("permanent failure")
	})
	env.add("always-fails")
	msg := env.deliver()

	for range 10 {
		_ = env.b.processMessage(t.Context(), msg)

		// Wipe any auxiliary counter keys, as an expiry or eviction would.
		for _, k := range env.retryKeys() {
			require.NoError(t, env.rdb.Del(t.Context(), k))
		}

		next, ok := env.redeliver(msg.ID)
		if !ok {
			break
		}
		msg = next
	}

	require.EqualValues(t, 3, runs.Load(), "receiver must run exactly NumRetries times")
	require.EqualValues(t, 0, env.pending(), "message must be discarded after the limit")
	require.Empty(t, env.retryKeys(), "no auxiliary retry keys should be left behind")
}

// ---------------------------------------------------------------------------
// Defect 1.3: the receiver must see the time stored by AddMessage ("time").
// ---------------------------------------------------------------------------

func redisDefect_ReceiverGetsMessageTime(t *testing.T) {
	env := newTestEnv(t, nil)
	var seen time.Time
	env.register("timed", func(_ context.Context, r *scriptReceiver) error { seen = r.event; return nil })
	env.add("timed")

	entries, err := env.rdb.Client().XRange(t.Context(), env.b.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	stored, err := time.Parse(time.RFC3339Nano, entries[0].Values["time"].(string))
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond) // make "now" distinguishable from the stored time
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
	require.True(t, stored.Equal(seen), "receiver time %v should equal the stored message time %v", seen, stored)
}

func TestDefect_PreloadVariablesReadsTime(t *testing.T) {
	at := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC)
	other := at.Add(-time.Hour)

	t.Run("time field is read", func(t *testing.T) {
		r := &Receiver{}
		_, err := r.PreloadVariables(t.Context(), map[string]any{
			"receiver": "r", "time": at.Format(time.RFC3339Nano),
		})
		require.NoError(t, err)
		require.True(t, at.Equal(r.event), "got %v", r.event)
	})

	t.Run("time wins over legacy event", func(t *testing.T) {
		r := &Receiver{}
		_, err := r.PreloadVariables(t.Context(), map[string]any{
			"receiver": "r", "time": at.Format(time.RFC3339Nano), "event": other.Format(time.RFC3339Nano),
		})
		require.NoError(t, err)
		require.True(t, at.Equal(r.event), "got %v", r.event)
	})

	t.Run("invalid time falls back to now without error", func(t *testing.T) {
		before := time.Now().UTC()
		r := &Receiver{}
		_, err := r.PreloadVariables(t.Context(), map[string]any{
			"receiver": "r", "time": "not-a-time",
		})
		require.NoError(t, err)
		require.False(t, r.event.Before(before))
	})
}

// ---------------------------------------------------------------------------
// Defect 1.4: XACK failures must be logged with the message id and must not stay
// unnoticed on the success path.
// ---------------------------------------------------------------------------

func redisDefect_AckErrorsAreLogged(t *testing.T) {
	t.Run("success path", func(t *testing.T) {
		env := newTestEnv(t, nil)
		env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
		env.add("ok")
		msg := env.deliver()

		env.hook.set(denyAck)
		err := env.b.processMessage(t.Context(), msg)
		env.hook.set(nil)

		require.Error(t, err, "a failed acknowledgement of a processed message must be surfaced")
		require.ErrorIs(t, err, errInjected)
		require.NotEmpty(t, env.log.errorsMentioning(msg.ID, "ack"), "ack failure must be logged with the message id")
		require.EqualValues(t, 1, env.pending())
	})

	t.Run("unregistered receiver", func(t *testing.T) {
		env := newTestEnv(t, nil)
		env.add("ghost")
		msg := env.deliver()

		env.hook.set(denyAck)
		err := env.b.processMessage(t.Context(), msg)
		env.hook.set(nil)

		require.ErrorIs(t, err, ErrorReceiverNotRegistered)
		require.NotEmpty(t, env.log.errorsMentioning(msg.ID, "ack"), "ack failure must be logged with the message id")
	})

	t.Run("invalid receiver name", func(t *testing.T) {
		env := newTestEnv(t, nil)
		msg := re.XMessage{ID: "1-0", Values: map[string]any{"receiver": 42}}

		env.hook.set(denyAck)
		err := env.b.processMessage(t.Context(), msg)
		env.hook.set(nil)

		require.ErrorIs(t, err, ErrorInvalidReceiverName)
		require.NotEmpty(t, env.log.errorsMentioning(msg.ID, "ack"), "ack failure must be logged with the message id")
	})

	t.Run("retry limit exceeded", func(t *testing.T) {
		env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 1; c.RetryIdleTime = 600 })
		env.register("fail", func(context.Context, *scriptReceiver) error { return errors.New("boom") })
		env.add("fail")
		msg := env.deliver()
		require.Error(t, env.b.processMessage(t.Context(), msg)) // attempt 1 of 1

		msg, ok := env.redeliver(msg.ID)
		require.True(t, ok)
		env.hook.set(denyAck)
		err := env.b.processMessage(t.Context(), msg) // over the limit: discard + ack
		env.hook.set(nil)

		require.Error(t, err, "failing to discard an exhausted message must be reported")
		require.NotEmpty(t, env.log.errorsMentioning(msg.ID, "ack"), "ack failure must be logged with the message id")
		require.EqualValues(t, 1, env.pending())
	})
}

// A receiver that finished successfully must be acknowledged even if the bus
// context was cancelled meanwhile (graceful shutdown), otherwise every deploy
// would redeliver the messages that were in flight.
func redisDefect_AckSurvivesContextCancellation(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.register("cancels", func(context.Context, *scriptReceiver) error {
		cancel() // shutdown signal arrives while the receiver is running
		return nil
	})
	env.add("cancels")

	require.NoError(t, env.b.processMessage(ctx, env.deliver()))
	require.EqualValues(t, 0, env.pending(), "a completed message must be acknowledged despite cancelled context")
}
