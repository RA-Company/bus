package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (e *testEnv) deadEntries() []re.XMessage {
	e.t.Helper()
	entries, err := e.rdb.Client().XRange(e.t.Context(), e.b.deadStream, "-", "+").Result()
	require.NoError(e.t, err)
	return entries
}

func (e *testEnv) deadLen() int64 {
	e.t.Helper()
	n, err := e.rdb.Client().XLen(e.t.Context(), e.b.deadStream).Result()
	require.NoError(e.t, err)
	return n
}

// drain keeps processing and redelivering a message until it is no longer pending.
func (e *testEnv) drain(msg re.XMessage) {
	e.t.Helper()
	for range 20 {
		_ = e.b.processMessage(e.t.Context(), msg)
		next, ok := e.redeliver(msg.ID)
		if !ok {
			return
		}
		msg = next
	}
	e.t.Fatalf("message %s is still pending after 20 deliveries", msg.ID)
}

func alwaysFails(runs *atomic.Int32) func(context.Context, *scriptReceiver) error {
	return func(context.Context, *scriptReceiver) error {
		runs.Add(1)
		return errors.New("permanent failure")
	}
}

// recordingHook records every MetricsHook call.
type recordingHook struct {
	mu     sync.Mutex
	events []string
}

func (h *recordingHook) add(ev string) {
	h.mu.Lock()
	h.events = append(h.events, ev)
	h.mu.Unlock()
}

func (h *recordingHook) count(prefix string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, ev := range h.events {
		if len(ev) >= len(prefix) && ev[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (h *recordingHook) Processed(r string, d time.Duration) {
	if d < 0 {
		h.add("bad-duration")
	}
	h.add("processed:" + r)
}
func (h *recordingHook) Retried(r string) { h.add("retried:" + r) }
func (h *recordingHook) Failed(r string, d time.Duration) {
	if d < 0 {
		h.add("bad-duration")
	}
	h.add("failed:" + r)
}
func (h *recordingHook) DeadLettered(r, reason string) { h.add("dead:" + r + ":" + reason) }
func (h *recordingHook) AckError(r string)             { h.add("ackerror:" + r) }

type panickingHook struct{ NopMetricsHook }

func (panickingHook) Processed(string, time.Duration) { panic("hook bug") }

// ---------------------------------------------------------------------------
// 1. Dead-letter
// ---------------------------------------------------------------------------

func redisDeadLetterExhausted(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 2; c.RetryIdleTime = 600 })
	require.Equal(t, env.b.stream+":dead", env.b.deadStream, "default dead-letter stream name")
	require.EqualValues(t, 10000, env.b.deadSize)
	require.Equal(t, 30*24*time.Hour, env.b.deadRetention)

	var runs atomic.Int32
	env.register("fails", alwaysFails(&runs))
	env.add("fails")
	msg := env.deliver()
	original := msg.Values

	env.drain(msg)

	require.EqualValues(t, 2, runs.Load(), "receiver runs NumRetries times, then the message is moved")
	require.EqualValues(t, 0, env.pending(), "the original message is acknowledged")

	entries := env.deadEntries()
	require.Len(t, entries, 1)
	v := entries[0].Values
	for _, field := range []string{"receiver", "id", "payload", "time"} {
		require.Equal(t, original[field], v[field], "field %q of the message is kept unchanged", field)
	}
	require.Equal(t, DeadReasonRetriesExhausted, v["dead_reason"])
	require.Equal(t, env.b.stream, v["dead_stream"])
	require.Equal(t, env.b.group, v["dead_group"])
	require.Equal(t, msg.ID, v["dead_message_id"])
	require.Equal(t, "3", v["dead_deliveries"])
	require.NotEmpty(t, v["dead_error"])
	_, err := time.Parse(time.RFC3339Nano, v["dead_at"].(string))
	require.NoError(t, err)
	require.EqualValues(t, 1, env.b.Stats().DeadLettered)
}

func redisDeadLetterWriteFailureKeepsPending(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 1; c.RetryIdleTime = 600 })
	var runs atomic.Int32
	env.register("fails", alwaysFails(&runs))
	env.add("fails")
	msg := env.deliver()
	require.Error(t, env.b.processMessage(t.Context(), msg)) // the only allowed attempt
	msg, ok := env.redeliver(msg.ID)
	require.True(t, ok)

	env.hook.set(func(cmd string) bool { return cmd == "xadd" })
	err := env.b.processMessage(t.Context(), msg)
	env.hook.set(nil)

	require.ErrorIs(t, err, errInjected)
	require.EqualValues(t, 1, env.pending(), "a message that could not be moved must stay pending, not vanish")
	require.EqualValues(t, 0, env.deadLen())
	require.NotEmpty(t, env.log.errorsMentioning(msg.ID, "dead-letter"))

	require.NoError(t, env.b.processMessage(t.Context(), msg)) // Redis is back
	require.EqualValues(t, 0, env.pending())
	require.EqualValues(t, 1, env.deadLen())
}

func redisDeadLetterRetention(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.DeadLetterRetention = time.Hour })
	require.Equal(t, time.Hour, env.b.deadRetention)

	// 300 entries from 1970 are far older than the retention.
	for i := range 300 {
		_, err := env.rdb.Client().XAdd(t.Context(), &re.XAddArgs{
			Stream: env.b.deadStream,
			ID:     fmt.Sprintf("%d-0", 1000+i),
			Values: map[string]any{"receiver": "old"},
		}).Result()
		require.NoError(t, err)
	}
	require.EqualValues(t, 300, env.deadLen())

	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("bad")) })
	env.add("perm")
	_ = env.b.processMessage(t.Context(), env.deliver())

	// XADD ... MINID ~ trims whole stream nodes, so the result is approximate, but the old
	// entries must go and the new one must stay.
	require.Less(t, env.deadLen(), int64(301), "entries older than DeadLetterRetention are trimmed")
	entries := env.deadEntries()
	require.Equal(t, "perm", entries[len(entries)-1].Values["receiver"])
}

func redisDeadLetterSizeCap(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.DeadLetterSize = 3 })
	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("bad")) })
	for range 6 {
		env.add("perm")
		_ = env.b.processMessage(t.Context(), env.deliver())
	}
	require.EqualValues(t, 3, env.deadLen(), "DeadLetterSize is an upper bound on the number of entries")
}

func redisDeadLetterCustomStream(t *testing.T) {
	name := "bus-test-custom-dead-" + t.Name()
	env := newTestEnv(t, func(c *BusConfiguration) { c.DeadLetterStream = name })
	t.Cleanup(func() { _ = env.rdb.Del(context.Background(), name) })
	require.Equal(t, name, env.b.deadStream)
	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("bad")) })
	env.add("perm")
	_ = env.b.processMessage(t.Context(), env.deliver())
	require.EqualValues(t, 1, env.deadLen())
}

func TestDeadLetter_Exhausted(t *testing.T) { eachRedis(t, redisDeadLetterExhausted) }
func TestDeadLetter_WriteFailureKeepsPending(t *testing.T) {
	eachRedis(t, redisDeadLetterWriteFailureKeepsPending)
}
func TestDeadLetter_Retention(t *testing.T)    { eachRedis(t, redisDeadLetterRetention) }
func TestDeadLetter_SizeCap(t *testing.T)      { eachRedis(t, redisDeadLetterSizeCap) }
func TestDeadLetter_CustomStream(t *testing.T) { eachRedis(t, redisDeadLetterCustomStream) }

// ---------------------------------------------------------------------------
// 2. Permanent
// ---------------------------------------------------------------------------

func TestPermanent_Wrapping(t *testing.T) {
	require.Nil(t, Permanent(nil))
	require.False(t, IsPermanent(nil))
	require.False(t, IsPermanent(errors.New("plain")))

	sentinel := errors.New("bad payload")
	err := Permanent(fmt.Errorf("validate: %w", sentinel))
	require.True(t, IsPermanent(err))
	require.ErrorIs(t, err, sentinel, "the wrapped error stays reachable")
	require.Equal(t, "validate: bad payload", err.Error())
	require.True(t, IsPermanent(fmt.Errorf("outer: %w", err)), "still permanent when wrapped again")
}

func redisPermanentGoesToDeadLetterWithoutRetries(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 5; c.RetryIdleTime = 600 })
	sentinel := errors.New("unknown event type")
	var runs atomic.Int32
	env.register("perm", func(context.Context, *scriptReceiver) error {
		runs.Add(1)
		return Permanent(sentinel)
	})
	env.add("perm")
	msg := env.deliver()

	err := env.b.processMessage(t.Context(), msg)

	require.ErrorIs(t, err, sentinel)
	require.True(t, IsPermanent(err))
	require.EqualValues(t, 1, runs.Load(), "a permanent error is never retried")
	require.EqualValues(t, 0, env.pending(), "the message is acknowledged")
	entries := env.deadEntries()
	require.Len(t, entries, 1)
	require.Equal(t, DeadReasonPermanent, entries[0].Values["dead_reason"])
	require.Contains(t, entries[0].Values["dead_error"], "unknown event type")
	require.Equal(t, "1", entries[0].Values["dead_deliveries"])
}

func TestPermanent_GoesToDeadLetter(t *testing.T) {
	eachRedis(t, redisPermanentGoesToDeadLetterWithoutRetries)
}

// ---------------------------------------------------------------------------
// 3. Subscribe; unregistered receivers keep being acknowledged and ignored
// ---------------------------------------------------------------------------

func redisSubscribeAndUnregisteredIgnored(t *testing.T) {
	env := newTestEnv(t, nil)
	var runs atomic.Int32
	env.b.Subscribe(t.Context(), "subscribed", func() ReceiverInterface {
		return &scriptReceiver{run: func(context.Context, *scriptReceiver) error { runs.Add(1); return nil }}
	})
	_, ok := env.b.GetReceiver("subscribed")
	require.True(t, ok, "Subscribe registers the receiver")

	env.add("subscribed")
	env.add("not-subscribed")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
	require.EqualValues(t, 1, runs.Load())

	err := env.b.processMessage(t.Context(), env.deliver())
	require.ErrorIs(t, err, ErrorReceiverNotRegistered)
	require.EqualValues(t, 0, env.pending(), "an unsubscribed event is acknowledged")
	require.EqualValues(t, 0, env.deadLen(), "and it does not pollute the dead-letter stream")
	require.Zero(t, env.b.Stats().DeadLettered)
}

func TestSubscribe_UnregisteredIgnored(t *testing.T) {
	eachRedis(t, redisSubscribeAndUnregisteredIgnored)
}

// ---------------------------------------------------------------------------
// 4. EnsureGroups
// ---------------------------------------------------------------------------

func redisEnsureGroups(t *testing.T) {
	env := newTestEnv(t, nil)

	// A producer publishes before the other service ever started.
	env.add("event")
	env.add("event")

	require.NoError(t, env.b.EnsureGroups(t.Context(), "service-a", "service-b"))
	require.NoError(t, env.b.EnsureGroups(t.Context(), "service-a"), "creating an existing group is not an error")

	for _, group := range []string{"service-a", "service-b"} {
		streams, err := env.rdb.XReadGroup(t.Context(), &re.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{env.b.stream, ">"}, Count: 10, Block: time.Second,
		})
		require.NoError(t, err)
		require.Len(t, streams, 1, group)
		require.Len(t, streams[0].Messages, 2, "group %s must see messages added before it existed", group)
	}

	// Init still creates its own group at the end of the stream ("$"): a service that starts
	// after the messages were added does not see them.
	late := &Bus{}
	require.NoError(t, late.Init(t.Context(), &BusConfiguration{Redis: redisConfig(t), Stream: env.b.stream, Group: "late-init"}))
	own, err := env.rdb.XReadGroup(t.Context(), &re.XReadGroupArgs{
		Group: "late-init", Consumer: "c", Streams: []string{env.b.stream, ">"}, Count: 10, Block: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Empty(t, own, "Init keeps creating its group at $")

	require.ErrorIs(t, env.b.EnsureGroups(t.Context(), "ok", ""), ErrorEmptyGroupName)
}

func TestEnsureGroups(t *testing.T) { eachRedis(t, redisEnsureGroups) }

// ---------------------------------------------------------------------------
// 5. Metrics
// ---------------------------------------------------------------------------

func redisStatsAndHook(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 2; c.RetryIdleTime = 600 })
	hook := &recordingHook{}
	env.b.SetMetricsHook(hook)

	var failRuns atomic.Int32
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.register("fails", alwaysFails(&failRuns))
	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("bad")) })

	// success
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))

	// fails twice (one retry), then dead-letter
	env.add("fails")
	env.drain(env.deliver())

	// permanent: one failure, dead-letter at once
	env.add("perm")
	_ = env.b.processMessage(t.Context(), env.deliver())

	// ack error: the receiver succeeded but the acknowledgement failed
	env.add("ok")
	msg := env.deliver()
	env.hook.set(denyAck)
	require.Error(t, env.b.processMessage(t.Context(), msg))
	env.hook.set(nil)

	require.Equal(t, Stats{Processed: 1, Retried: 1, DeadLettered: 2, Failed: 3, AckErrors: 1, InFlight: 0}, env.b.Stats())

	require.Equal(t, 1, hook.count("processed:ok"))
	require.Equal(t, 1, hook.count("retried:fails"))
	require.Equal(t, 3, hook.count("failed:"))
	require.Equal(t, 1, hook.count("dead:fails:"+DeadReasonRetriesExhausted))
	require.Equal(t, 1, hook.count("dead:perm:"+DeadReasonPermanent))
	require.Equal(t, 1, hook.count("ackerror:ok"))
	require.Zero(t, hook.count("bad-duration"))
}

func redisHookPanicDoesNotBreakProcessing(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.SetMetricsHook(panickingHook{})
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")

	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
	require.EqualValues(t, 0, env.pending())
	require.EqualValues(t, 1, env.b.Stats().Processed)
	require.NotEmpty(t, env.log.errorsMentioning("hook"))

	env.b.SetMetricsHook(nil) // removable
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
}

func redisLag(t *testing.T) {
	env := newTestEnv(t, nil)
	env.add("event")
	env.add("event")
	env.add("event")

	major, ok := env.b.serverMajorVersion(t.Context())
	require.True(t, ok)
	if major < 7 {
		lag, err := env.b.Lag(t.Context())
		require.ErrorIs(t, err, ErrorLagUnavailable)
		require.EqualValues(t, -1, lag)
		return
	}

	lag, err := env.b.Lag(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, lag, "three messages are waiting for the group")

	env.deliver()
	lag, err = env.b.Lag(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, lag, "a delivered message is no longer lag")
}

func TestMetrics_StatsAndHook(t *testing.T)      { eachRedis(t, redisStatsAndHook) }
func TestMetrics_HookPanicIsolated(t *testing.T) { eachRedis(t, redisHookPanicDoesNotBreakProcessing) }
func TestMetrics_Lag(t *testing.T)               { eachRedis(t, redisLag) }

// ---------------------------------------------------------------------------
// 6. Retries per receiver
// ---------------------------------------------------------------------------

func redisPerReceiverRetries(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 4; c.RetryIdleTime = 600 })

	var onceRuns, defaultRuns atomic.Int32
	env.b.RegisterReceiverWithOptions(t.Context(), "once", func() ReceiverInterface {
		return &scriptReceiver{run: alwaysFails(&onceRuns)}
	}, ReceiverOptions{NumRetries: 1})
	env.register("default", alwaysFails(&defaultRuns))

	env.add("once")
	env.drain(env.deliver())
	env.add("default")
	env.drain(env.deliver())

	require.EqualValues(t, 1, onceRuns.Load(), "the receiver's own limit applies")
	require.EqualValues(t, 4, defaultRuns.Load(), "receivers without options use the group's NumRetries")
	require.EqualValues(t, 0, env.pending())
	require.EqualValues(t, 2, env.deadLen(), "both ended in the dead-letter stream")
}

func TestRetries_PerReceiver(t *testing.T) { eachRedis(t, redisPerReceiverRetries) }
