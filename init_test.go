package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

func TestInit_NilConfiguration(t *testing.T) {
	var b Bus
	require.ErrorIs(t, b.Init(t.Context(), nil), ErrorNilConfiguration)
}

func redisSecondInitIsRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("r", func(context.Context, *scriptReceiver) error { return nil })
	client := env.b.redis

	err := env.b.Init(t.Context(), &BusConfiguration{Redis: redisConfig(t), Stream: "another"})
	require.ErrorIs(t, err, ErrorAlreadyInitialized)
	require.Same(t, client, env.b.redis, "the Redis client is kept")
	require.Equal(t, 1, env.b.ReceiversCount(), "the registered receivers are kept")
	require.NotEqual(t, "another", env.b.stream)
}

func TestInit_SecondInitIsRejected(t *testing.T) { eachRedis(t, redisSecondInitIsRejected) }

// Receivers registered before Init are kept.
func redisRegisterBeforeInit(t *testing.T) {
	_, cfg := redisStart(t)
	b := &Bus{}
	b.RegisterReceiverWithOptions(t.Context(), "early", func() ReceiverInterface { return &plainReceiver{} },
		ReceiverOptions{NumRetries: 2})
	require.NoError(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: "bus-test-" + uuid.NewString()}))
	t.Cleanup(func() { _ = b.redis.Del(context.Background(), b.stream) })

	_, ok := b.GetReceiver("early")
	require.True(t, ok, "a receiver registered before Init must survive it")
	require.EqualValues(t, 2, b.options["early"].NumRetries, "its options too")
}

func TestInit_RegisterBeforeInit(t *testing.T) { eachRedis(t, redisRegisterBeforeInit) }

// A failed Init closes its Redis connection, and Init can be called again.
func redisFailedInitClosesConnectionAndCanBeRetried(t *testing.T) {
	rdb, cfg := redisStart(t)
	stream := "bus-test-" + uuid.NewString()
	require.NoError(t, rdb.Set(t.Context(), stream, "not a stream", 60)) // XGROUP CREATE fails with WRONGTYPE
	t.Cleanup(func() { _ = rdb.Del(context.Background(), stream) })

	b := &Bus{}
	require.Error(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: stream}))
	first := b.redis
	require.ErrorIs(t, first.Client().Ping(t.Context()).Err(), re.ErrClosed, "the connection of a failed Init is closed")

	require.NoError(t, rdb.Del(t.Context(), stream))
	require.NoError(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: stream}), "Init can be retried")
	require.NotSame(t, first, b.redis)
	require.NoError(t, b.redis.Client().Ping(t.Context()).Err())
	require.ErrorIs(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: stream}), ErrorAlreadyInitialized)
}

func TestInit_FailedInitClosesConnectionAndCanBeRetried(t *testing.T) {
	eachRedis(t, redisFailedInitClosesConnectionAndCanBeRetried)
}

// An Init that runs while another one is in progress is rejected (the check and the claim are one step).
func TestInit_ConcurrentInitIsRejected(t *testing.T) {
	var b Bus
	b.initializing = true
	require.ErrorIs(t, b.Init(t.Context(), &BusConfiguration{}), ErrorAlreadyInitialized)
	require.Nil(t, b.redis, "the rejected Init must not touch the bus")
}

func TestSetLogger_BeforeInit(t *testing.T) {
	var b Bus
	log := &captureLogger{}
	require.NotPanics(t, func() { b.SetLogger(log) })
	require.Same(t, log, b.logger.(*captureLogger))
}

func redisSetLoggerBeforeInit(t *testing.T) {
	_, cfg := redisStart(t)
	log := &captureLogger{}
	b := &Bus{}
	b.SetLogger(log)
	require.NoError(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: "bus-test-" + uuid.NewString()}))
	t.Cleanup(func() { _ = b.redis.Del(context.Background(), b.stream) })

	b.Infof(t.Context(), "hello from the bus")
	log.mu.Lock()
	defer log.mu.Unlock()
	found := false
	for _, e := range log.entries {
		found = found || strings.Contains(e.text, "hello from the bus")
	}
	require.True(t, found, "the logger set before Init must be used")
}

func TestSetLogger_BeforeInitIsKept(t *testing.T) { eachRedis(t, redisSetLoggerBeforeInit) }

// ---------------------------------------------------------------------------
// A bus without a successful Init returns errors instead of panicking
// ---------------------------------------------------------------------------

func TestNotInitialized_PublicMethodsReturnError(t *testing.T) {
	var b Bus
	require.NotPanics(t, func() {
		_, err := b.AddMessage(t.Context(), "x", 1, "")
		require.ErrorIs(t, err, ErrorNotInitialized)
		require.ErrorIs(t, b.EnsureGroups(t.Context(), "g"), ErrorNotInitialized)
		_, err = b.Counters(t.Context(), "", 1)
		require.ErrorIs(t, err, ErrorNotInitialized)
		_, err = b.Snapshot(t.Context())
		require.ErrorIs(t, err, ErrorNotInitialized)
	})
}

func TestNotInitialized_StartReturnsAtOnce(t *testing.T) {
	var b Bus
	log := &captureLogger{}
	b.SetLogger(log)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Start(t.Context())
	}()
	waitClosed(t, done, 2*time.Second, "Start without Init to return")
	require.NotEmpty(t, log.errorsMentioning("not initialized"))
}

// After a failed Init the bus is not initialized either: Start does not run workers on the closed connection.
func redisFailedInitDoesNotStart(t *testing.T) {
	rdb, cfg := redisStart(t)
	stream := "bus-test-" + uuid.NewString()
	require.NoError(t, rdb.Set(t.Context(), stream, "not a stream", 60))
	t.Cleanup(func() { _ = rdb.Del(context.Background(), stream) })

	b := &Bus{}
	err := b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: stream})
	require.Error(t, err)
	require.NotErrorIs(t, err, re.ErrClosed, "closing the connection of a failed Init succeeds")

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Start(t.Context())
	}()
	waitClosed(t, done, 2*time.Second, "Start after a failed Init to return")
	_, err = b.AddMessage(t.Context(), "x", 1, "")
	require.ErrorIs(t, err, ErrorNotInitialized)
}

func TestNotInitialized_FailedInitDoesNotStart(t *testing.T) {
	eachRedis(t, redisFailedInitDoesNotStart)
}

func TestLifecycle_StopAndCloseBeforeInit(t *testing.T) {
	var b Bus
	require.NotPanics(t, func() {
		require.NoError(t, b.Stop(t.Context()))
		require.NoError(t, b.Stop(t.Context()), "Stop is idempotent")
		require.NoError(t, b.Close(t.Context()))
	})
}

func TestLifecycle_StartAfterStopWithoutInit(t *testing.T) {
	var b Bus
	require.NoError(t, b.Stop(t.Context()))
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Start(t.Context())
	}()
	waitClosed(t, done, 5*time.Second, "Start of a stopped bus to return")
}

// Close after a failed Init succeeds on a single node and on a cluster alike: the connection is closed already.
func redisCloseAfterFailedInit(t *testing.T) {
	rdb, cfg := redisStart(t)
	stream := "bus-test-" + uuid.NewString()
	require.NoError(t, rdb.Set(t.Context(), stream, "not a stream", 60))
	t.Cleanup(func() { _ = rdb.Del(context.Background(), stream) })

	b := &Bus{}
	require.Error(t, b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: stream}))
	require.NoError(t, b.Close(t.Context()))
}

func TestClose_AfterFailedInit(t *testing.T) { eachRedis(t, redisCloseAfterFailedInit) }
