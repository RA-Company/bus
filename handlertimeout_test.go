package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The default limit is ten times RetryIdleTime, any explicit one is accepted, a negative one switches it off.
func TestHandlerTimeout_Config(t *testing.T) {
	eachRedis(t, func(t *testing.T) {
		env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 40 })
		require.Equal(t, 400*time.Second, env.b.handlerTimeout)
		require.Equal(t, 40*time.Second/3, env.b.leaseEvery)

		env = newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 40; c.HandlerTimeout = 5 * time.Minute })
		require.Equal(t, 5*time.Minute, env.b.handlerTimeout, "it may be longer than RetryIdleTime: the lease is renewed")

		env = newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 40; c.HandlerTimeout = -1 })
		require.Zero(t, env.b.handlerTimeout)
	})
}

// A receiver that honours its context is stopped at the limit, the failure is recorded before the message
// is redelivered, and the redelivery is an ordinary retry.
func redisHandlerTimeoutCancelsReceiver(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 2; c.HandlerTimeout = 300 * time.Millisecond })

	var runs atomic.Int32
	var firstErr atomic.Value
	env.register("event", func(ctx context.Context, _ *scriptReceiver) error {
		if runs.Add(1) == 1 {
			<-ctx.Done()
			firstErr.Store(context.Cause(ctx))
			return ctx.Err()
		}
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()

	require.Eventually(t, func() bool { return env.b.Stats().TimedOut == 1 }, 3*time.Second, 10*time.Millisecond)
	// TimedOut is counted at the deadline, Failed when the receiver has returned.
	require.Eventually(t, func() bool { return env.b.Stats().Failed == 1 }, 3*time.Second, 10*time.Millisecond, "a timeout is a failed run")
	st := env.b.Stats()
	require.EqualValues(t, 0, st.Processed, "the failure is known before the message is redelivered")
	require.EqualValues(t, 1, runs.Load())
	require.ErrorIs(t, firstErr.Load().(error), ErrorHandlerTimeout, "the receiver learns why its context was cancelled")

	require.Eventually(t, func() bool { return env.b.Stats().Processed == 1 }, 10*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 2, runs.Load())
	require.EqualValues(t, 1, env.b.Stats().Retried)
	require.Zero(t, env.b.Stats().Stuck)
}

// A receiver that ignores its context keeps its worker, and nothing more is started because of it: even when
// the messages time out and are retried again and again, no more than WorkersCount receivers run at once.
func redisHandlerTimeoutBoundsConcurrency(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) {
		c.WorkersCount = 2
		c.NumRetries = 100
		c.RetryIdleTime = 1
		c.HandlerTimeout = 200 * time.Millisecond
	})

	release := make(chan struct{})
	var released onceFunc
	defer released.do(func() { close(release) })
	var running, peak, starts atomic.Int32
	env.register("hang", func(context.Context, *scriptReceiver) error {
		n := running.Add(1)
		defer running.Add(-1)
		starts.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release // ignores ctx
		return nil
	})
	for range 5 {
		env.add("hang")
	}
	stop := env.startBus()
	defer stop()

	// Several redelivery cycles (RetryIdleTime is one second) go by while both workers are held.
	require.Eventually(t, func() bool { return env.b.Stats().Stuck == 2 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(3 * time.Second)
	require.EqualValues(t, 2, starts.Load(), "a held worker takes nothing new, so nothing piles up")
	require.EqualValues(t, 2, peak.Load())
	require.EqualValues(t, 2, env.b.Stats().Stuck)
	require.EqualValues(t, 2, env.b.Stats().TimedOut)

	released.do(func() { close(release) })
	// The runs that were held are failures, even though the receivers returned nil; the messages are redone.
	require.Eventually(t, func() bool { return env.b.Stats().Stuck == 0 && env.b.Stats().Processed == 5 }, 20*time.Second, 50*time.Millisecond)
	require.LessOrEqual(t, peak.Load(), int32(2))
}

// A message that always hangs is dead-lettered after NumRetries runs, each of them cut at the limit.
func redisHandlerTimeoutExhaustsRetries(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) {
		c.NumRetries = 2
		c.RetryIdleTime = 1
		c.HandlerTimeout = 200 * time.Millisecond
	})
	env.register("event", func(ctx context.Context, _ *scriptReceiver) error { <-ctx.Done(); return ctx.Err() })
	env.add("event")
	stop := env.startBus()
	defer stop()

	require.Eventually(t, func() bool { return env.b.Stats().DeadLettered == 1 }, 15*time.Second, 50*time.Millisecond)
	require.EqualValues(t, 2, env.b.Stats().TimedOut)
}

// Without a limit a long receiver is left alone, as before.
func redisHandlerTimeoutOff(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 600; c.HandlerTimeout = -1 })
	var runs atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error {
		time.Sleep(700 * time.Millisecond)
		runs.Add(1)
		return nil
	})
	env.add("event")
	stop := env.startBus()
	defer stop()
	require.Eventually(t, func() bool { return env.b.Stats().Processed == 1 }, 5*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 0, env.b.Stats().TimedOut)
}

// A receiver that finishes in time is not touched, and the limit is not mistaken for a shutdown.
func redisHandlerTimeoutFastReceiver(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.RetryIdleTime = 600; c.HandlerTimeout = time.Second })
	env.register("event", func(context.Context, *scriptReceiver) error { return nil })
	for range 5 {
		env.add("event")
	}
	stop := env.startBus()
	defer stop()
	require.Eventually(t, func() bool { return env.b.Stats().Processed == 5 }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, env.b.Stats().TimedOut)
	require.Zero(t, env.b.Stats().Stuck)
}

type onceFunc struct{ done atomic.Bool }

func (o *onceFunc) do(f func()) {
	if o.done.CompareAndSwap(false, true) {
		f()
	}
}

func TestHandlerTimeout_CancelsReceiver(t *testing.T) {
	eachRedis(t, redisHandlerTimeoutCancelsReceiver)
}
func TestHandlerTimeout_BoundsConcurrency(t *testing.T) {
	eachRedis(t, redisHandlerTimeoutBoundsConcurrency)
}
func TestHandlerTimeout_ExhaustsRetries(t *testing.T) {
	eachRedis(t, redisHandlerTimeoutExhaustsRetries)
}
func TestHandlerTimeout_Off(t *testing.T)          { eachRedis(t, redisHandlerTimeoutOff) }
func TestHandlerTimeout_FastReceiver(t *testing.T) { eachRedis(t, redisHandlerTimeoutFastReceiver) }
