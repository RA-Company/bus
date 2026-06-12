package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/ra-company/database/redis"
	"github.com/ra-company/env"
	"github.com/ra-company/logging"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type ReceiverPayload struct {
	Title string `json:"title"`
	Foo   string `json:"foo"`
}

type TestReceiver struct {
	Payload ReceiverPayload
	Receiver
}

func (t *TestReceiver) Execute(ctx context.Context) error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	logging.Logs.Infof(ctx, "Executing TestReceiver with payload: %+v", t.Payload)
	return nil
}

type TestErrorReceiver struct {
	Payload ReceiverPayload
	Receiver
	Redis redis.RedisClient
}

func (t *TestErrorReceiver) Execute(ctx context.Context) error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	if str, err := t.Redis.Get(ctx, "error_test", ""); err != nil {
		logging.Logs.Errorf(ctx, "Receiver %q get redis key error: %v", t.task, err)
		return err
	} else {
		str = fmt.Sprintf("%sb", str)
		if err = t.Redis.Set(ctx, "error_test", str, 0); err != nil {
			logging.Logs.Errorf(ctx, "Receiver %q set redis key error: %v", t.task, err)
			return err
		}
	}

	logging.Logs.Infof(ctx, "Executing TestReceiver with payload: %+v", t.Payload)
	return fmt.Errorf("simulated error in TestErrorReceiver")
}

func newTestErrorReceiver(ctx context.Context, config redis.Config) ReceiverInterface {
	result := TestErrorReceiver{}
	result.Redis.Start(ctx, &config)
	return &result
}

type TestPanicReceiver struct {
	Payload ReceiverPayload
	Receiver
}

func (t *TestPanicReceiver) Execute(ctx context.Context) error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	logging.Logs.Infof(ctx, "Executing TestReceiver with payload: %+v", t.Payload)
	panic("simulated panic in TestPanicReceiver")
}

func Test(t *testing.T) {
	ctx := t.Context()
	faker := gofakeit.New(0)

	stream := faker.Word()
	group := faker.Word()
	workersCount := faker.Number(1, 10)
	numRetries := faker.Number(1, 5)
	bus := &Bus{}

	config := redis.Config{
		Hosts:    env.GetEnvStr("REDIS_HOSTS", ""),
		Password: env.GetEnvStr("REDIS_PWD", "-"),
		DB:       env.GetEnvInt("REDIS_DB", -1),
	}
	require.NotEmpty(t, config.Hosts, "REDIS_HOSTS should not be empty")
	require.GreaterOrEqual(t, config.DB, 0, "REDIS_DB should be defined")

	redis := &redis.RedisClient{}
	redis.Start(ctx, &config)

	err := redis.Set(ctx, "error_test", "b", 0)
	require.NoError(t, err, "Should set test key in Redis")
	defer redis.Del(ctx, "error_test")

	bus.Init(ctx, &BusConfiguration{
		Redis:         config,
		Stream:        stream,
		Group:         group,
		WorkersCount:  workersCount,
		NumRetries:    numRetries,
		RetryIdleTime: 1,
	})
	require.NotNil(t, bus.redis, "Redis client should be initialized")
	require.NotEmpty(t, bus.stream, "Stream should be set")
	require.NotEmpty(t, bus.group, "Group should be set")
	require.Greater(t, bus.workersCount, 0, "Workers count should be greater than 0")
	require.Greater(t, bus.numRetries, 0, "Number of retries should be greater than 0")

	defer redis.XGroupDestroy(ctx, bus.stream, bus.group)

	bus.RegisterReceiver(ctx, "receiver", func() ReceiverInterface { return &TestReceiver{} })
	receiver, ok := bus.GetReceiver("receiver")
	require.True(t, ok, "Receiver should be registered")
	require.NotNil(t, receiver, "Receiver should not be nil")
	bus.RegisterReceiver(ctx, "error_receiver", func() ReceiverInterface { return newTestErrorReceiver(ctx, config) })
	errorReceiver, ok := bus.GetReceiver("error_receiver")
	require.True(t, ok, "Error Receiver should be registered")
	require.NotNil(t, errorReceiver, "Error Receiver should not be nil")
	bus.RegisterReceiver(ctx, "panic_receiver", func() ReceiverInterface { return &TestPanicReceiver{} })
	panicReceiver, ok := bus.GetReceiver("panic_receiver")
	require.True(t, ok, "Panic Receiver should be registered")
	require.NotNil(t, panicReceiver, "Panic Receiver should not be nil")

	timerCtx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	go bus.Start(timerCtx)

	bus.AddMessage(ctx, "receiver", ReceiverPayload{
		Title: faker.Sentence(5),
		Foo:   faker.Word(),
	})

	bus.AddMessage(ctx, "error_receiver", ReceiverPayload{
		Title: faker.Sentence(5),
		Foo:   faker.Word(),
	})
	bus.AddMessage(ctx, "panic_receiver", ReceiverPayload{
		Title: faker.Sentence(5),
		Foo:   faker.Word(),
	})

	require.Eventually(t, func() bool {
		str, err := redis.Get(t.Context(), "error_test", "")
		return err == nil && len(str) == numRetries+1
	}, 20*time.Second, 500*time.Millisecond,
		"error receiver should have retried %d times", numRetries)
}

// redisConfig reads Redis connection parameters from environment variables and
// skips the test if they are not configured.
func redisConfig(t *testing.T) redis.Config {
	t.Helper()
	cfg := redis.Config{
		Hosts:    env.GetEnvStr("REDIS_HOSTS", ""),
		Password: env.GetEnvStr("REDIS_PWD", "-"),
		DB:       env.GetEnvInt("REDIS_DB", -1),
	}
	if cfg.Hosts == "" || cfg.DB < 0 {
		t.Skip("REDIS_HOSTS / REDIS_DB not configured")
	}
	return cfg
}

func redisStart(t *testing.T) (*redis.RedisClient, *redis.Config) {
	cfg := redisConfig(t)
	redis := &redis.RedisClient{}
	redis.Start(t.Context(), &cfg)
	return redis, &cfg
}

// TestAddMessage_EmptyReceiver verifies the early-return error path that does
// not touch Redis, so no connection is needed.
func TestAddMessage_EmptyReceiver(t *testing.T) {
	_, err := (&Bus{}).AddMessage(t.Context(), "", struct{}{})
	require.ErrorIs(t, err, ErrorEmptyReceiverTitle)
}

// TestAddMessage_NonSerializablePayload verifies that json.Marshal failure is
// propagated before XAdd is called.
func TestAddMessage_NonSerializablePayload(t *testing.T) {
	redis, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: uuid.New().String()})
	defer redis.XGroupDestroy(context.Background(), b.stream, b.group)

	_, err := b.AddMessage(t.Context(), "recv", make(chan int))
	require.Error(t, err)
}

// TestAddMessage_Success verifies that a valid message is stored and a non-empty
// UUID-shaped ID is returned.
func TestAddMessage_Success(t *testing.T) {
	redis, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: uuid.New().String()})
	defer redis.XGroupDestroy(context.Background(), b.stream, b.group)

	id, err := b.AddMessage(t.Context(), "recv", ReceiverPayload{Title: "hello"})
	require.NoError(t, err)
	require.NotEmpty(t, id)
	_, parseErr := uuid.Parse(id)
	require.NoError(t, parseErr, "returned ID should be a valid UUID")
}

// TestBus_RegisterReceiver covers all branches of RegisterReceiver without Redis.
func TestBus_RegisterReceiver(t *testing.T) {
	ctx := t.Context()
	factory := func() ReceiverInterface { return &TestReceiver{} }

	t.Run("empty title is a no-op", func(t *testing.T) {
		b := &Bus{receivers: make(map[string]ReceiverFactory)}
		b.RegisterReceiver(ctx, "", factory)
		require.Equal(t, 0, b.ReceiversCount(), "No receiver should be registered for empty title")
	})

	t.Run("registers factory under given title", func(t *testing.T) {
		b := &Bus{receivers: make(map[string]ReceiverFactory)}
		b.RegisterReceiver(ctx, "recv", factory)
		receiver, ok := b.GetReceiver("recv")
		require.True(t, ok, "Receiver should be registered")
		require.NotNil(t, receiver, "Receiver should not be nil")
	})

	t.Run("second call overwrites existing entry", func(t *testing.T) {
		b := &Bus{receivers: make(map[string]ReceiverFactory)}
		b.RegisterReceiver(ctx, "recv", factory)
		factory2 := func() ReceiverInterface { return &TestErrorReceiver{} }
		b.RegisterReceiver(ctx, "recv", factory2)
		require.Equal(t, 1, b.ReceiversCount(), "Only one receiver should be registered")
		receiver, ok := b.GetReceiver("recv")
		require.True(t, ok, "Receiver should be registered")
		require.NotNil(t, receiver, "Receiver should not be nil")
	})

	t.Run("nil map is initialised on first call", func(t *testing.T) {
		b := &Bus{}
		b.RegisterReceiver(ctx, "recv", factory)
		receiver, ok := b.GetReceiver("recv")
		require.True(t, ok, "Receiver should be registered")
		require.NotNil(t, receiver, "Receiver should not be nil")
	})
}

// TestBus_Init_Defaults verifies that every zero/missing BusConfiguration field
// falls back to the documented default value.
func TestBus_Init_Defaults(t *testing.T) {
	redis, cfg := redisStart(t)

	cases := []struct {
		name   string
		input  BusConfiguration
		assert func(t *testing.T, b *Bus)
	}{
		{
			name:   "zero WorkersCount → 10",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, 10, b.workersCount) },
		},
		{
			name:   "WorkersCount > 100 capped at 100",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String(), WorkersCount: 999},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, 100, b.workersCount) },
		},
		{
			name:   "zero NumRetries → 5",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, 5, b.numRetries) },
		},
		{
			name:   "zero StreamSize → 10000",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, 10000, b.streamSize) },
		},
		{
			name:   "zero RetryIdleTime → 60",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, 60, b.retryIdleTime) },
		},
		{
			name:   "empty Stream → default-stream",
			input:  BusConfiguration{Redis: *cfg, Group: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, "default-stream", b.stream) },
		},
		{
			name:   "empty Group → default-group",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, "default-group", b.group) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Bus{}
			b.Init(t.Context(), &tc.input)
			defer redis.XGroupDestroy(context.Background(), b.stream, b.group)
			tc.assert(t, b)
		})
	}
}

// TestBus_RetryManagement tests the SetRetry / GetRetry / DelRetry round-trip
// using a real Redis connection.
func TestBus_RetryManagement(t *testing.T) {
	redis, cfg := redisStart(t)
	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{
		Redis:         *cfg,
		Stream:        uuid.New().String(),
		NumRetries:    5,
		RetryIdleTime: 10,
	})
	defer redis.XGroupDestroy(context.Background(), b.stream, b.group)

	ctx := t.Context()
	key := uuid.New().String()

	t.Run("unknown key returns 0", func(t *testing.T) {
		require.Equal(t, 0, b.GetRetry(ctx, key))
	})

	t.Run("SetRetry then GetRetry returns same value", func(t *testing.T) {
		b.SetRetry(ctx, key, 3)
		require.Equal(t, 3, b.GetRetry(ctx, key))
	})

	t.Run("DelRetry removes the key so GetRetry returns 0", func(t *testing.T) {
		b.SetRetry(ctx, key, 2)
		b.DelRetry(ctx, key)
		require.Equal(t, 0, b.GetRetry(ctx, key))
	})
}

// TestProcessMessage_InvalidReceiverField verifies that a message whose
// "receiver" field is not a string is ACKed and returns ErrorInvalidReceiverName.
func TestProcessMessage_InvalidReceiverField(t *testing.T) {
	rdb, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: uuid.New().String()})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	msg := re.XMessage{
		ID:     "1-0",
		Values: map[string]any{"receiver": 42}, // not a string
	}
	err := b.processMessage(t.Context(), msg)
	require.ErrorIs(t, err, ErrorInvalidReceiverName)
}

// TestProcessMessage_UnregisteredReceiver verifies that a message for an unknown
// receiver is ACKed and returns ErrorReceiverNotRegistered.
func TestProcessMessage_UnregisteredReceiver(t *testing.T) {
	rdb, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{Redis: *cfg, Stream: uuid.New().String()})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	msg := re.XMessage{
		ID:     "1-0",
		Values: map[string]any{"receiver": "no-such-receiver"},
	}
	err := b.processMessage(t.Context(), msg)
	require.ErrorIs(t, err, ErrorReceiverNotRegistered)
}

// TestProcessMessage_RetryExceeded verifies that once the retry counter exceeds
// numRetries the message is ACKed, the counter is cleaned up, and nil is returned.
func TestProcessMessage_RetryExceeded(t *testing.T) {
	rdb, cfg := redisStart(t)

	numRetries := 3
	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{
		Redis:         *cfg,
		Stream:        uuid.New().String(),
		NumRetries:    numRetries,
		RetryIdleTime: 10,
	})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	b.mu.Lock()
	b.receivers = make(map[string]ReceiverFactory)
	b.receivers["recv"] = func() ReceiverInterface { return &TestReceiver{} }
	b.mu.Unlock()

	ctx := t.Context()
	msgID := "1-0"
	b.SetRetry(ctx, msgID, numRetries) // already at max

	msg := re.XMessage{
		ID:     msgID,
		Values: map[string]any{"receiver": "recv"},
	}
	err := b.processMessage(ctx, msg)
	require.NoError(t, err, "exceeded retry should return nil, not an error")
	// counter must be removed
	require.Equal(t, 0, b.GetRetry(ctx, msgID), "retry counter should be deleted after exhaustion")
}

// TestReceiver_PreloadVariables_MissingReceiverKey verifies that a task map
// without the "receiver" key returns ErrorInvalidReceiverName.
func TestReceiver_PreloadVariables_MissingReceiverKey(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{"payload": "x"})
	require.ErrorIs(t, err, ErrorInvalidReceiverName)
}

// TestReceiver_PreloadVariables_InvalidEventTime verifies that an unparseable
// event time does not return an error but falls back to current time.
func TestReceiver_PreloadVariables_InvalidEventTime(t *testing.T) {
	before := time.Now().UTC()
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"event":    "not-a-time",
	})
	require.NoError(t, err)
	require.False(t, r.event.Before(before), "fallback event should not be before test start")
}

// TestReceiver_PreloadVariables_InvalidUUID verifies that an unparseable id
// does not return an error but falls back to a fresh UUID.
func TestReceiver_PreloadVariables_InvalidUUID(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"id":       "not-a-uuid",
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, r.id, "fallback id should be non-nil UUID")
}

// TestReceiver_Payload verifies the Payload accessor.
func TestReceiver_Payload(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"payload":  `{"x":1}`,
	})
	require.NoError(t, err)
	require.Equal(t, `{"x":1}`, r.Payload())
}

// TestGetString covers all branches of the getString helper.
func TestGetString(t *testing.T) {
	require.Equal(t, "hello", getString("hello"))
	require.Equal(t, "", getString(42))
	require.Equal(t, "", getString(nil))
	require.Equal(t, "", getString(struct{}{}))
}

// TestReceiver_PreloadVariables exercises all branches of PreloadVariables,
// including error paths and default-value generation, with no Redis needed.
func TestReceiver_PreloadVariables(t *testing.T) {
	ctx := t.Context()

	t.Run("nil task is a no-op", func(t *testing.T) {
		r := &Receiver{}
		_, err := r.PreloadVariables(ctx, nil)
		require.ErrorIs(t, err, ErrorEmptyTask)
	})

	t.Run("valid full task populates all fields", func(t *testing.T) {
		id := uuid.New()
		now := time.Now().UTC()
		r := &Receiver{}
		_, err := r.PreloadVariables(ctx, map[string]any{
			"receiver": "my-receiver",
			"payload":  `{"k":"v"}`,
			"event":    now.Format(time.RFC3339Nano),
			"id":       id.String(),
		})
		require.NoError(t, err)
		require.Equal(t, "my-receiver", r.task)
		require.Equal(t, `{"k":"v"}`, r.payload)
		require.Equal(t, id, r.id)
		require.True(t, now.Equal(r.event), "event time mismatch: %v vs %v", now, r.event)
	})

	t.Run("missing event defaults to current time", func(t *testing.T) {
		before := time.Now().UTC()
		r := &Receiver{}
		_, err := r.PreloadVariables(ctx, map[string]any{"receiver": "receiver"})
		require.NoError(t, err)
		require.False(t, r.event.Before(before), "default event should not be before test start")
	})

	t.Run("missing id gets a fresh non-nil uuid", func(t *testing.T) {
		r := &Receiver{}
		_, err := r.PreloadVariables(ctx, map[string]any{"receiver": "receiver"})
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, r.id)
	})
}
