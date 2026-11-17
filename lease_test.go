package bus

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// mentioning returns the entries of a level that contain every fragment (case-insensitive).
func (c *captureLogger) mentioning(level string, fragments ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.entries {
		if e.level != level {
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

// owner returns the consumer that the only pending message belongs to.
func (e *testEnv) owner() string {
	e.t.Helper()
	ext, err := e.rdb.Client().XPendingExt(e.t.Context(), &re.XPendingExtArgs{
		Stream: e.b.stream, Group: e.b.group, Start: "-", End: "+", Count: 10,
	}).Result()
	require.NoError(e.t, err)
	require.Len(e.t, ext, 1)
	return ext[0].Consumer
}

func (e *testEnv) pendingID() string {
	e.t.Helper()
	res, err := e.rdb.Client().XPending(e.t.Context(), e.b.stream, e.b.group).Result()
	require.NoError(e.t, err)
	require.EqualValues(e.t, 1, res.Count)
	return res.Lower
}

// A receiver that runs several times longer than RetryIdleTime is executed once: its worker renews the lease, so the
// message is not taken over by another worker. Without the renewal (control) it is executed again meanwhile.
func redisLeaseLongRun(t *testing.T) {
	run := func(t *testing.T, renew bool) (runs int32, retried int64) {
		env := newTestEnv(t, nil) // RetryIdleTime = 1 s, 2 workers
		if !renew {
			env.b.leaseEvery = time.Hour
		}
		var n atomic.Int32
		env.register("event", func(context.Context, *scriptReceiver) error {
			n.Add(1)
			time.Sleep(2500 * time.Millisecond)
			return nil
		})
		env.add("event")
		stop := env.startBus()
		defer stop()
		require.Eventually(t, func() bool { return env.b.Stats().Processed >= 1 }, 15*time.Second, 20*time.Millisecond)
		return n.Load(), env.b.Stats().Retried
	}

	runs, retried := run(t, true)
	require.EqualValues(t, 1, runs, "the message is executed once, however long the receiver runs")
	require.EqualValues(t, 0, retried)

	runs, _ = run(t, false)
	require.Greater(t, runs, int32(1), "control: without the renewal the message is taken over")
}

// When the process is gone nobody renews: the message is redelivered after RetryIdleTime.
func redisLeaseProcessDies(t *testing.T) {
	env := newTestEnv(t, nil)
	release := make(chan struct{})
	var released onceFunc
	var starts atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error {
		if starts.Add(1) == 1 {
			<-release // the "dead" process: it never finishes
		}
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()
	defer released.do(func() { close(release) }) // runs first: a receiver that is still held would block stop
	require.Eventually(t, func() bool { return starts.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	require.EqualValues(t, 1, starts.Load(), "alive, so renewed: not redelivered")

	// The process dies: the bus gives up on its workers and stops renewing.
	stopCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, env.b.Stop(stopCtx), ErrorStopTimeout)

	other := env.restarted() // another instance of the service
	other.register("event", func(context.Context, *scriptReceiver) error { starts.Add(1); return nil })
	stopOther := other.startBus()
	defer stopOther()
	require.Eventually(t, func() bool { return other.b.Stats().Processed == 1 }, 10*time.Second, 20*time.Millisecond,
		"the message is redelivered once the lease has run out")
	require.EqualValues(t, 2, starts.Load())
}

// A renewal never takes a message back from the consumer that took it over, and says so.
func redisLeaseForeignOwner(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 3 }) // the intruder's own idle time is long enough to look
	release := make(chan struct{})
	var released onceFunc
	var starts atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error {
		starts.Add(1)
		<-release
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()
	defer released.do(func() { close(release) }) // runs first: a receiver that is still held would block stop
	require.Eventually(t, func() bool { return starts.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

	id := env.pendingID()
	_, ok := env.redeliver(id) // somebody else claims it while it is being executed
	require.True(t, ok)
	require.Equal(t, "test-consumer-2", env.owner())

	require.Eventually(t, func() bool {
		return len(env.log.mentioning("warn", id, "no longer pending for the worker")) > 0
	}, 5*time.Second, 20*time.Millisecond, "the lost lease is reported")
	require.Equal(t, "test-consumer-2", env.owner(), "the renewal did not take the message back")
}

// When HandlerTimeout is over the lease is not renewed any more, so a receiver that ignores its context cannot
// keep the message for ever: another worker takes it RetryIdleTime later.
func redisLeaseEndsWithTimeout(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.HandlerTimeout = 1500 * time.Millisecond })
	release := make(chan struct{})
	var released onceFunc
	var starts atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error {
		if starts.Add(1) == 1 {
			<-release // ignores its context
		}
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()
	defer released.do(func() { close(release) }) // runs first: a receiver that is still held would block stop

	require.Eventually(t, func() bool { return starts.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(1200 * time.Millisecond)
	require.EqualValues(t, 1, starts.Load(), "within HandlerTimeout the lease is renewed")

	require.Eventually(t, func() bool { return starts.Load() == 2 && env.b.Stats().Processed == 1 }, 10*time.Second, 20*time.Millisecond,
		"after HandlerTimeout the message is taken over although its first receiver still runs")
	require.EqualValues(t, 1, env.b.Stats().Stuck)
}

// A failing renewal does not touch the receiver: it finishes and the message is acknowledged.
func redisLeaseRenewalFailure(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 2 })
	var runs atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error {
		runs.Add(1)
		env.hook.set(func(cmd string) bool { return cmd == "evalsha" || cmd == "eval" }) // renewals fail
		time.Sleep(1200 * time.Millisecond)
		env.hook.set(nil)
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()

	require.Eventually(t, func() bool { return env.b.Stats().Processed == 1 }, 10*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 1, runs.Load())
	require.NotEmpty(t, env.log.mentioning("warn", "failed to renew the leases"))
}

// Messages that finish quickly are never renewed and leave nothing behind.
func redisLeaseShortRuns(t *testing.T) {
	env := newTestEnv(t, nil)
	var runs atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })
	for range 30 {
		env.add("event")
	}
	stop := env.startBus()
	defer stop()
	require.Eventually(t, func() bool { return env.b.Stats().Processed == 30 }, 10*time.Second, 10*time.Millisecond)

	env.b.leaseMu.Lock()
	left := len(env.b.leases)
	env.b.leaseMu.Unlock()
	require.Zero(t, left, "a finished message is no longer in the registry")
	require.Empty(t, env.log.mentioning("warn", "lease"))
}

// Two workers that run the same message one after the other (the first past its limit, the second after the
// redelivery) do not end each other's lease.
func TestLease_SameMessageTwoWorkers(t *testing.T) {
	b := &Bus{}
	ctxA := context.WithValue(t.Context(), workerKey{}, "A")
	ctxB := context.WithValue(t.Context(), workerKey{}, "B")

	endA := b.leaseStart(ctxA, "1-0")
	endB := b.leaseStart(ctxB, "1-0")
	require.Len(t, b.leases, 2)

	endA() // the old run finally returns
	endA() // and ending twice is harmless
	require.Len(t, b.leases, 1)
	_, ok := b.leases[leaseKey{"1-0", "B"}]
	require.True(t, ok, "the lease of the second worker is still renewed")

	endB()
	require.Empty(t, b.leases)

	b.leaseStart(t.Context(), "2-0")() // handled directly, not by a worker: nothing to renew
	require.Empty(t, b.leases)
}

func TestLease_LongRun(t *testing.T)         { eachRedis(t, redisLeaseLongRun) }
func TestLease_ProcessDies(t *testing.T)     { eachRedis(t, redisLeaseProcessDies) }
func TestLease_ForeignOwner(t *testing.T)    { eachRedis(t, redisLeaseForeignOwner) }
func TestLease_EndsWithTimeout(t *testing.T) { eachRedis(t, redisLeaseEndsWithTimeout) }
func TestLease_RenewalFailure(t *testing.T)  { eachRedis(t, redisLeaseRenewalFailure) }
func TestLease_ShortRuns(t *testing.T)       { eachRedis(t, redisLeaseShortRuns) }
