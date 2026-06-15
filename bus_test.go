package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
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
		WorkersCount:  int64(workersCount),
		NumRetries:    int64(numRetries),
		RetryIdleTime: 1,
	})
	require.NotNil(t, bus.redis, "Redis client should be initialized")
	require.NotEmpty(t, bus.stream, "Stream should be set")
	require.NotEmpty(t, bus.group, "Group should be set")
	require.Greater(t, bus.workersCount, int64(0), "Workers count should be greater than 0")
	require.Greater(t, bus.numRetries, int64(0), "Number of retries should be greater than 0")

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
			assert: func(t *testing.T, b *Bus) { require.Equal(t, int64(10), b.workersCount) },
		},
		{
			name:   "WorkersCount > 100 capped at 100",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String(), WorkersCount: 999},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, int64(100), b.workersCount) },
		},
		{
			name:   "zero NumRetries → 5",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, int64(5), b.numRetries) },
		},
		{
			name:   "zero StreamSize → 10000",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, int64(10000), b.streamSize) },
		},
		{
			name:   "zero RetryIdleTime → 60",
			input:  BusConfiguration{Redis: *cfg, Stream: uuid.New().String()},
			assert: func(t *testing.T, b *Bus) { require.Equal(t, int64(60), b.retryIdleTime) },
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

	numRetries := int64(3)
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
	b.SetRetry(ctx, msgID, int(numRetries)) // already at max

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

// TestReceiver_GetPayload verifies the GetPayload accessor returns the raw JSON string.
func TestReceiver_GetPayload(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"payload":  `{"x":1}`,
	})
	require.NoError(t, err)
	require.Equal(t, `{"x":1}`, r.GetPayload())
}

// TestReceiver_PreloadVariables_PayloadAutoUnmarshal verifies that a valid JSON
// payload is automatically unmarshalled into the Payload any field.
func TestReceiver_PreloadVariables_PayloadAutoUnmarshal(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"payload":  `{"title":"hello","foo":"bar"}`,
	})
	require.NoError(t, err)
	require.NotNil(t, r.Payload, "Payload should be populated after PreloadVariables")
	m, ok := r.Payload.(map[string]any)
	require.True(t, ok, "Payload should be map[string]any for untyped receiver")
	require.Equal(t, "hello", m["title"])
}

// TestReceiver_PreloadVariables_InvalidJSONPayload verifies that an invalid JSON
// payload causes PreloadVariables to return an error.
func TestReceiver_PreloadVariables_InvalidJSONPayload(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
		"payload":  `not-valid-json`,
	})
	require.Error(t, err, "invalid JSON payload should cause PreloadVariables to fail")
}

// TestReceiver_PreloadVariables_EmptyPayload verifies that an empty payload
// does not trigger JSON unmarshalling and leaves Payload nil.
func TestReceiver_PreloadVariables_EmptyPayload(t *testing.T) {
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "recv",
	})
	require.NoError(t, err)
	require.Nil(t, r.Payload, "Payload should remain nil when not provided")
}

// TestReceiver_InFlight verifies that InFlight reads from the injected counter.
// The counter is managed by Bus.processMessage; Receiver only reads it.
func TestReceiver_InFlight(t *testing.T) {
	var counter atomic.Int64
	r := &Receiver{}
	r.SetBusContext(&counter, 5)

	require.Equal(t, "[0/5]", r.InFlight())
	counter.Add(1)
	require.Equal(t, "[1/5]", r.InFlight())
	counter.Add(-1)
	require.Equal(t, "[0/5]", r.InFlight())
}

// TestReceiver_InFlight_NilCounter verifies the fallback format when
// SetBusContext has not been called.
func TestReceiver_InFlight_NilCounter(t *testing.T) {
	r := &Receiver{}
	require.Equal(t, "[?/0]", r.InFlight())
}

// testTitleImpl is a minimal TitleInterface implementation for testing.
type testTitleImpl struct{ name string }

func (ti *testTitleImpl) Title() string { return ti.name }

// TestReceiver_title_WithSelf verifies that a non-nil Self returns its Title().
func TestReceiver_title_WithSelf(t *testing.T) {
	r := &Receiver{Self: &testTitleImpl{name: "MyReceiver"}}
	require.Equal(t, "MyReceiver", r.title())
}

// TestReceiver_title_WithoutSelf verifies that a nil Self returns an empty string.
func TestReceiver_title_WithoutSelf(t *testing.T) {
	r := &Receiver{}
	require.Equal(t, "", r.title())
}

// TestCaller verifies that Caller returns a non-empty, formatted string
// containing a filename, line number, and function name.
func TestCaller(t *testing.T) {
	s := Caller()
	require.NotEmpty(t, s)
	require.Contains(t, s, ":", "Caller should contain file:line separator")
	require.Contains(t, s, "->", "Caller should contain -> function separator")
}

// testParamsImpl is a minimal ParamsInterface implementation for testing.
type testParamsImpl struct{}

func (p *testParamsImpl) Title() string        { return "ParamsReceiver" }
func (p *testParamsImpl) DrawParams() []string { return []string{"key=val", "x=1"} }

// TestReceiver_Start_LogsParams verifies that Start does not error when Payload
// implements ParamsInterface.
//
// NOTE: the current check in Start is r.Payload.(ParamsInterface), not
// r.Self.(ParamsInterface). Since r.Payload is auto-unmarshalled from JSON
// (typically map[string]any), the check will never fire in practice. The
// intended check is most likely r.Self.(ParamsInterface).
func TestReceiver_Start_LogsParams(t *testing.T) {
	// Pre-set Payload to a ParamsInterface implementor; task has no "payload"
	// key so PreloadVariables leaves r.Payload untouched.
	r := &Receiver{Payload: &testParamsImpl{}}
	ctx, err := r.Start(t.Context(), map[string]any{"receiver": "recv"})
	require.NoError(t, err)
	require.NotNil(t, ctx)
	r.Finish(ctx)
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

// TestBus_GetReceiver_NilMap verifies that GetReceiver returns false when
// the bus has not been initialised (receivers map is nil).
func TestBus_GetReceiver_NilMap(t *testing.T) {
	b := &Bus{}
	_, ok := b.GetReceiver("anything")
	require.False(t, ok, "GetReceiver on uninitialised bus should return false")
}

// TestBus_ReceiversCount_NilMap verifies that ReceiversCount returns 0 when
// the bus has not been initialised.
func TestBus_ReceiversCount_NilMap(t *testing.T) {
	b := &Bus{}
	require.Equal(t, 0, b.ReceiversCount())
}

// TestReceiver_Execute_Panics verifies that the base Receiver.Execute panics
// (it is a placeholder that must be overridden by concrete receivers).
func TestReceiver_Execute_Panics(t *testing.T) {
	r := &Receiver{}
	require.Panics(t, func() { _ = r.Execute(t.Context()) })
}

// TestProcessMessage_StartError verifies that processMessage propagates the
// error returned by receiver.Start. An invalid JSON payload triggers the
// error inside PreloadVariables without requiring a special receiver type.
func TestProcessMessage_StartError(t *testing.T) {
	rdb, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{
		Redis:         *cfg,
		Stream:        uuid.New().String(),
		NumRetries:    5,
		RetryIdleTime: 10,
	})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	b.mu.Lock()
	b.receivers = map[string]ReceiverFactory{
		"recv": func() ReceiverInterface { return &TestReceiver{} },
	}
	b.mu.Unlock()

	msg := re.XMessage{
		ID: "1-0",
		Values: map[string]any{
			"receiver": "recv",
			"payload":  "not-valid-json",
		},
	}
	err := b.processMessage(t.Context(), msg)
	require.Error(t, err, "processMessage should surface the Start error")
}

// TestProcessMessage_ExecuteError verifies that processMessage propagates the
// error returned by receiver.Execute. TestErrorReceiver always returns a
// non-nil error from Execute.
func TestProcessMessage_ExecuteError(t *testing.T) {
	rdb, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{
		Redis:         *cfg,
		Stream:        uuid.New().String(),
		NumRetries:    5,
		RetryIdleTime: 10,
	})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	b.mu.Lock()
	b.receivers = map[string]ReceiverFactory{
		"recv": func() ReceiverInterface {
			return newTestErrorReceiver(t.Context(), *cfg)
		},
	}
	b.mu.Unlock()

	msg := re.XMessage{
		ID: "2-0",
		Values: map[string]any{
			"receiver": "recv",
			"payload":  `{"title":"t","foo":"f"}`,
		},
	}
	err := b.processMessage(t.Context(), msg)
	require.Error(t, err, "processMessage should surface the Execute error")
}

// TestBus_GetRetry_CorruptValue verifies that GetRetry returns retryFailed
// when the Redis value cannot be parsed as an integer (simulated corruption).
func TestBus_GetRetry_CorruptValue(t *testing.T) {
	rdb, cfg := redisStart(t)

	b := &Bus{}
	b.Init(t.Context(), &BusConfiguration{
		Redis:         *cfg,
		Stream:        uuid.New().String(),
		NumRetries:    5,
		RetryIdleTime: 10,
	})
	defer rdb.XGroupDestroy(context.Background(), b.stream, b.group)

	ctx := t.Context()
	key := uuid.New().String()
	redisKey := fmt.Sprintf("%s:retry:%s", b.stream, key)

	err := rdb.Set(ctx, redisKey, "not-a-number", 60)
	require.NoError(t, err)
	defer rdb.Del(ctx, redisKey)

	got := b.GetRetry(ctx, key)
	require.Equal(t, retryFailed, got, "corrupt stored value should map to retryFailed")
}
