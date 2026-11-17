package bus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// runBus starts env.b.Start in the background and returns a channel closed when Start returns.
func (e *testEnv) runBus(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		e.b.Start(ctx)
		close(done)
	}()
	return done
}

func waitClosed(t *testing.T, ch <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timeout waiting for %s", what)
	}
}

// Stop stops reading, waits for the message being processed, lets it finish with a live context and
// acknowledge it. Messages that were not read stay in the stream and are processed by the next run.
func redisStopWaitsForRunningReceiver(t *testing.T) {
	env := newTestEnv(t, nil)

	started := make(chan struct{})
	var ctxCancelled atomic.Bool
	var runs atomic.Int32
	env.register("slow", func(ctx context.Context, r *scriptReceiver) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		select {
		case <-time.After(700 * time.Millisecond):
		case <-ctx.Done():
			ctxCancelled.Store(true)
			return ctx.Err()
		}
		return nil
	})

	done := env.runBus(t.Context())
	env.add("slow")
	waitClosed(t, started, 5*time.Second, "receiver start")

	stopCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	began := time.Now()
	require.NoError(t, env.b.Stop(stopCtx))
	require.GreaterOrEqual(t, time.Since(began), 300*time.Millisecond, "Stop must have waited for the receiver")
	waitClosed(t, done, 5*time.Second, "Start to return")

	require.False(t, ctxCancelled.Load(), "a receiver that is allowed to finish keeps a live context")
	require.EqualValues(t, 1, env.b.Stats().Processed)
	require.EqualValues(t, 0, env.pending(), "the finished message must be acknowledged")
	require.EqualValues(t, 0, env.b.inFlight.Load())

	// Nothing is read after Stop.
	env.add("slow")
	time.Sleep(1500 * time.Millisecond)
	require.EqualValues(t, 1, runs.Load(), "no message may be read after Stop")
	require.EqualValues(t, 0, env.pending())
	snap, err := env.b.Snapshot(t.Context())
	require.NoError(t, err)
	for _, g := range snap.Groups {
		if g.Name == env.b.group {
			require.EqualValues(t, 1, g.Lag, "the unread message stays in the stream")
		}
	}

	// A new run of the service picks up the message that was not read.
	restart := env.restarted()
	restart.register("slow", func(ctx context.Context, r *scriptReceiver) error { runs.Add(1); return nil })
	done2 := restart.runBus(t.Context())
	require.Eventually(t, func() bool { return restart.b.Stats().Processed == 1 }, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, restart.b.Stop(t.Context()))
	waitClosed(t, done2, 5*time.Second, "second Start to return")
	require.EqualValues(t, 2, runs.Load())
}

// restarted returns a second Bus on the same stream and group, like the service after a restart.
func (e *testEnv) restarted() *testEnv {
	e.t.Helper()
	cfg, _ := redisConfigFor(redisMode(e.t))
	b := &Bus{}
	require.NoError(e.t, b.Init(e.t.Context(), &BusConfiguration{
		Redis: cfg, Stream: e.b.stream, Group: e.b.group, WorkersCount: 2, NumRetries: 3, RetryIdleTime: 1,
	}))
	e.t.Cleanup(func() { _ = b.Close(context.Background()) })
	b.SetLogger(e.log)
	return &testEnv{t: e.t, b: b, rdb: e.rdb, hook: e.hook, log: e.log}
}

// A receiver that hangs and ignores its context must not hang Stop: Stop returns when its context
// expires, the message stays pending, and nothing new is read.
func redisStopWithHungReceiver(t *testing.T) {
	env := newTestEnv(t, nil)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	var sawCancel atomic.Bool
	var runs atomic.Int32
	env.register("hung", func(ctx context.Context, r *scriptReceiver) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		go func() {
			<-ctx.Done()
			sawCancel.Store(true)
		}()
		<-release // ignores ctx
		return nil
	})
	t.Cleanup(releaseAll)

	done := env.runBus(t.Context())
	env.add("hung")
	waitClosed(t, started, 5*time.Second, "receiver start")

	stopCtx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	began := time.Now()
	err := env.b.Stop(stopCtx)
	require.ErrorIs(t, err, ErrorStopTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(began), 3*time.Second, "Stop must return when its context expires")
	waitClosed(t, done, 5*time.Second, "Start to return after a forced stop")

	require.Eventually(t, sawCancel.Load, 2*time.Second, 10*time.Millisecond, "the receiver's context must be cancelled so it can abort")
	require.EqualValues(t, 1, env.pending(), "the unfinished message must stay pending")
	require.EqualValues(t, 0, env.b.Stats().Processed)

	// Nothing new is executed after the forced stop. A read that was already blocked in the second
	// worker may still bring a message; it is left pending, not executed.
	env.add("hung")
	time.Sleep(1500 * time.Millisecond)
	env.add("hung")
	time.Sleep(500 * time.Millisecond)
	require.EqualValues(t, 1, runs.Load(), "no message may be executed after Stop gave up")
	require.GreaterOrEqual(t, env.pending(), int64(1))

	// Once the receiver returns, the worker finishes and a patient Stop succeeds.
	releaseAll()
	require.NoError(t, env.b.Stop(t.Context()))
	require.EqualValues(t, 1, env.b.Stats().Processed)
}

// Stop before Start and Stop without Start do not block, Close is idempotent.
func redisStopWithoutStartAndClose(t *testing.T) {
	env := newTestEnv(t, nil)
	require.NoError(t, env.b.Stop(t.Context()))
	require.NoError(t, env.b.Stop(t.Context()))

	done := env.runBus(t.Context())
	waitClosed(t, done, 5*time.Second, "Start after Stop must return at once")

	env2 := newTestEnv(t, nil)
	done2 := env2.runBus(t.Context())
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, env2.b.Close(t.Context()))
	require.NoError(t, env2.b.Close(t.Context()))
	waitClosed(t, done2, 5*time.Second, "Start to return after Close")
}

// With ShutdownTimeout, cancelling the context of Start (or SIGTERM) is graceful too: the running
// receiver gets time to finish and acknowledge.
func redisShutdownTimeoutIsGraceful(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.ShutdownTimeout = 10 * time.Second })

	started := make(chan struct{})
	var ctxCancelled atomic.Bool
	env.register("slow", func(ctx context.Context, r *scriptReceiver) error {
		close(started)
		select {
		case <-time.After(600 * time.Millisecond):
			return nil
		case <-ctx.Done():
			ctxCancelled.Store(true)
			return ctx.Err()
		}
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := env.runBus(ctx)
	env.add("slow")
	waitClosed(t, started, 5*time.Second, "receiver start")
	cancel()
	waitClosed(t, done, 10*time.Second, "Start to return")

	require.False(t, ctxCancelled.Load())
	require.EqualValues(t, 1, env.b.Stats().Processed)
	require.EqualValues(t, 0, env.pending())
}

// ShutdownTimeout expiring cancels the receiver, leaves the message pending and releases Start even
// if the receiver does not return.
func redisShutdownTimeoutExpires(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.ShutdownTimeout = 300 * time.Millisecond })

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	env.register("hung", func(ctx context.Context, r *scriptReceiver) error {
		close(started)
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := env.runBus(ctx)
	env.add("hung")
	waitClosed(t, started, 5*time.Second, "receiver start")
	cancel()
	waitClosed(t, done, 5*time.Second, "Start to return after ShutdownTimeout")
	require.EqualValues(t, 1, env.pending())
	require.NotEmpty(t, env.log.errorsMentioning("still being processed"))
}

// Default behaviour is unchanged: without ShutdownTimeout the receivers' context is cancelled at once.
func redisDefaultStopCancelsReceivers(t *testing.T) {
	env := newTestEnv(t, nil)

	started := make(chan struct{})
	var ctxCancelled atomic.Bool
	env.register("waiting", func(ctx context.Context, r *scriptReceiver) error {
		close(started)
		<-ctx.Done()
		ctxCancelled.Store(true)
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := env.runBus(ctx)
	env.add("waiting")
	waitClosed(t, started, 5*time.Second, "receiver start")
	cancel()
	waitClosed(t, done, 5*time.Second, "Start to return")

	require.True(t, ctxCancelled.Load())
	require.EqualValues(t, 1, env.pending(), "a cancelled receiver leaves its message pending")
}

func TestStop_WaitsForRunningReceiver(t *testing.T) { eachRedis(t, redisStopWaitsForRunningReceiver) }
func TestStop_HungReceiver(t *testing.T)            { eachRedis(t, redisStopWithHungReceiver) }
func TestStop_WithoutStartAndClose(t *testing.T)    { eachRedis(t, redisStopWithoutStartAndClose) }
func TestShutdownTimeout_Graceful(t *testing.T)     { eachRedis(t, redisShutdownTimeoutIsGraceful) }
func TestShutdownTimeout_Expires(t *testing.T)      { eachRedis(t, redisShutdownTimeoutExpires) }
func TestShutdown_DefaultCancelsReceivers(t *testing.T) {
	eachRedis(t, redisDefaultStopCancelsReceivers)
}

// ---------------------------------------------------------------------------
// Redelivery of idle messages
// ---------------------------------------------------------------------------

// An idle message must be redelivered even when the head of the pending list is occupied by many
// recently redelivered (not idle) messages. XAUTOCLAIM looks only at the first Count*10 entries of the
// list, so a message behind ten fresh ones used to wait until they were acknowledged or dead-lettered.
func redisRedeliveryBehindFreshPendingEntries(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) {
		c.RetryIdleTime = 2
		c.NumRetries = 1000 // the failing messages must stay pending for the whole test
	})

	ids := make([]string, 11)
	for i := range ids {
		ids[i] = env.add("event")
	}
	// Deliver all 11 (they become pending) and let them all go idle.
	var entries []string
	for range ids {
		entries = append(entries, env.deliver().ID)
	}
	time.Sleep(2200 * time.Millisecond)
	// Ten messages at the head of the list have just been redelivered: their idle time starts anew,
	// but they keep their place at the head. The last one stays idle.
	for _, id := range entries[:10] {
		_, ok := env.redeliver(id)
		require.True(t, ok)
	}

	var tail atomic.Int32
	env.register("event", func(ctx context.Context, r *scriptReceiver) error {
		if r.id.String() == ids[10] {
			tail.Add(1)
			return nil
		}
		return errors.New("always fails")
	})
	stop := env.startBus()
	defer stop()

	require.Eventually(t, func() bool { return tail.Load() == 1 }, 1500*time.Millisecond, 20*time.Millisecond,
		"the idle message behind ten fresh pending entries must be redelivered at once")
}

func TestRedelivery_BehindFreshPendingEntries(t *testing.T) {
	eachRedis(t, redisRedeliveryBehindFreshPendingEntries)
}

// idleEnv returns an environment with n delivered messages that have all been idle for RetryIdleTime.
func idleEnv(t *testing.T, n int) (*testEnv, []string) {
	t.Helper()
	env := newTestEnv(t, nil) // RetryIdleTime = 1 s
	var ids []string
	for range n {
		env.add("event")
	}
	for range n {
		ids = append(ids, env.deliver().ID)
	}
	time.Sleep(1200 * time.Millisecond)
	return env, ids
}

// Many workers racing for one idle message: exactly one gets it, with the right delivery counter.
func redisClaimIdleOnlyOneWorkerWins(t *testing.T) {
	env, ids := idleEnv(t, 1)

	var wins atomic.Int32
	var deliveries atomic.Int64
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			msg, d, found, err := env.b.claimIdle(t.Context(), "racer-"+string(rune('a'+i)))
			require.NoError(t, err)
			if found {
				require.Equal(t, ids[0], msg.ID)
				wins.Add(1)
				deliveries.Store(d)
			}
		})
	}
	wg.Wait()

	require.EqualValues(t, 1, wins.Load(), "an idle message must be claimed by exactly one worker")
	require.EqualValues(t, 2, deliveries.Load(), "the first delivery plus this one")
	res, err := env.rdb.Client().XPendingExt(t.Context(), &re.XPendingExtArgs{
		Stream: env.b.stream, Group: env.b.group, Start: ids[0], End: ids[0], Count: 1,
	}).Result()
	require.NoError(t, err)
	require.EqualValues(t, deliveries.Load(), res[0].RetryCount, "the counter handed to the worker equals Redis' own")

	// Now the message is fresh: nothing to redeliver.
	_, _, found, err := env.b.claimIdle(t.Context(), "late")
	require.NoError(t, err)
	require.False(t, found)
}

// A pending entry whose message is gone from the stream does not stop the others from being redelivered.
func redisClaimIdleSkipsTrimmedEntry(t *testing.T) {
	env, ids := idleEnv(t, 3)
	require.NoError(t, env.rdb.Client().XDel(t.Context(), env.b.stream, ids[0]).Err())

	var got []string
	for range 3 {
		msg, _, found, err := env.b.claimIdle(t.Context(), "w")
		require.NoError(t, err)
		if !found {
			break
		}
		got = append(got, msg.ID)
	}
	require.Equal(t, ids[1:], got, "the deleted entry is skipped, the other two are redelivered in order")
}

func TestRedelivery_OnlyOneWorkerWins(t *testing.T) { eachRedis(t, redisClaimIdleOnlyOneWorkerWins) }
func TestRedelivery_SkipsTrimmedEntry(t *testing.T) { eachRedis(t, redisClaimIdleSkipsTrimmedEntry) }

// A constant flow of new messages, enough to keep every worker busy all the time, does not hold back the
// redelivery of an idle pending message: the search for idle messages comes before reading new ones.
func redisRedeliveryUnderConstantLoad(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.WorkersCount = 1 })

	idleID := env.add("event") // uuid of the message
	env.deliver()              // delivered and never acknowledged: it becomes idle after RetryIdleTime (1 s)
	deliveredAt := time.Now()

	var redelivered, fresh atomic.Int64
	redeliveredAt := make(chan time.Time, 1)
	env.register("event", func(ctx context.Context, r *scriptReceiver) error {
		if r.id.String() == idleID {
			redelivered.Add(1)
			redeliveredAt <- time.Now()
			return nil
		}
		fresh.Add(1)
		time.Sleep(5 * time.Millisecond) // the single worker is busy all the time
		return nil
	})

	// Publish faster than one worker can process: the backlog only grows.
	pubCtx, stopPub := context.WithCancel(t.Context())
	var pub sync.WaitGroup
	pub.Go(func() {
		for pubCtx.Err() == nil {
			_, _ = env.b.AddMessage(pubCtx, "event", ReceiverPayload{Title: "t", Foo: "f"}, "")
			time.Sleep(time.Millisecond)
		}
	})
	defer func() { stopPub(); pub.Wait() }()

	stop := env.startBus()
	defer stop()

	select {
	case at := <-redeliveredAt:
		since := at.Sub(deliveredAt)
		require.GreaterOrEqual(t, since, time.Second, "not before RetryIdleTime")
		require.Less(t, since, 2500*time.Millisecond, "redelivered within RetryIdleTime plus the search interval, in spite of the load")
	case <-time.After(6 * time.Second):
		t.Fatal("the idle message was not redelivered while the stream was under load")
	}
	require.EqualValues(t, 1, redelivered.Load())
	require.Eventually(t, func() bool { return fresh.Load() > 50 }, 5*time.Second, 20*time.Millisecond,
		"new messages must keep being processed as well")
}

func TestRedelivery_UnderConstantLoad(t *testing.T) { eachRedis(t, redisRedeliveryUnderConstantLoad) }
