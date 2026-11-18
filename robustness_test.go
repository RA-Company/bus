package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Backoff after failed reads, recreation of a lost group
// ---------------------------------------------------------------------------

func TestIsNoGroup(t *testing.T) {
	require.True(t, isNoGroup(errors.New("NOGROUP No such key 's' or consumer group 'g' in XREADGROUP with GROUP option")))
	require.False(t, isNoGroup(errors.New("connection refused")))
	require.False(t, isNoGroup(nil))
}

func TestPause(t *testing.T) {
	t.Run("waits for the backoff", func(t *testing.T) {
		b := &Bus{readBackoff: 50 * time.Millisecond}
		b.initLifecycle()
		start := time.Now()
		b.pause(t.Context())
		require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	})
	t.Run("returns at once when the bus stops", func(t *testing.T) {
		b := &Bus{readBackoff: time.Hour}
		b.beginStop()
		start := time.Now()
		b.pause(t.Context())
		require.Less(t, time.Since(start), time.Second)
	})
	t.Run("returns at once when ctx is cancelled", func(t *testing.T) {
		b := &Bus{readBackoff: time.Hour}
		b.initLifecycle()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		start := time.Now()
		b.pause(ctx)
		require.Less(t, time.Since(start), time.Second)
	})
}

// A failing XREADGROUP must not turn the workers into a busy loop: with a 300 ms backoff two workers make
// a handful of attempts per second, not thousands.
func redisReadErrorBacksOff(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 300 * time.Millisecond
	env.hook.set(func(cmd string) bool { return cmd == "xreadgroup" || cmd == "xpending" })

	ctx, cancel := context.WithCancel(t.Context())
	done := env.runBus(ctx)
	time.Sleep(1200 * time.Millisecond)
	cancel()
	waitClosed(t, done, 10*time.Second, "bus to stop")

	reads := len(env.log.errorsMentioning("failed to read from stream"))
	require.Positive(t, reads, "the read failures must be logged")
	// 2 workers x (1.2 s / 0.3 s + 1) = 10 at most; a busy loop would make thousands.
	require.LessOrEqual(t, reads, 12, "workers must pause after a failed read")
}

// When the stream key disappears (DEL, expiry) Redis answers NOGROUP; the worker creates the group again
// and the bus keeps working.
func redisNoGroupOnReadRecreatesGroup(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	ctx := t.Context()
	require.NoError(t, env.rdb.Del(ctx, env.b.stream))

	env.b.lastClaim.Store(time.Now().UnixNano()) // go straight to XREADGROUP
	env.b.workerJob(ctx, "w1")

	requireGroup(t, env)
	require.NotEmpty(t, env.log.errorsMentioning("NOGROUP"))
}

func redisNoGroupOnClaimRecreatesGroup(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	ctx := t.Context()
	require.NoError(t, env.rdb.Del(ctx, env.b.stream))

	env.b.lastClaim.Store(0) // the claim of idle messages runs first and gets NOGROUP
	env.b.workerJob(ctx, "w1")

	requireGroup(t, env)
}

func requireGroup(t *testing.T, env *testEnv) {
	t.Helper()
	groups, err := env.rdb.Client().XInfoGroups(t.Context(), env.b.stream).Result()
	require.NoError(t, err, "the stream must exist again")
	var names []string
	for _, g := range groups {
		names = append(names, g.Name)
	}
	require.Contains(t, names, env.b.group, "the group must have been created again")
}

func TestWorker_ReadErrorBacksOff(t *testing.T) { eachRedis(t, redisReadErrorBacksOff) }

func TestWorker_NoGroupOnReadRecreatesGroup(t *testing.T) {
	eachRedis(t, redisNoGroupOnReadRecreatesGroup)
}

func TestWorker_NoGroupOnClaimRecreatesGroup(t *testing.T) {
	eachRedis(t, redisNoGroupOnClaimRecreatesGroup)
}

// A panic in the worker's own code pauses the worker instead of turning it into a busy loop.
func TestSafeWorker_PausesAfterPanic(t *testing.T) {
	b := &Bus{readBackoff: 200 * time.Millisecond}
	b.initLifecycle()
	log := &captureLogger{}
	b.SetLogger(log)
	// No Redis client: workerJob panics on every step.
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	b.safeWorker(ctx, "w")

	panics := len(log.errorsMentioning("panic recovered"))
	require.Positive(t, panics)
	require.LessOrEqual(t, panics, 5, "about one panic per backoff (700 ms / 200 ms), not thousands")
}
