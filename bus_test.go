package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/ra-company/database/redis"
	"github.com/ra-company/env"
	"github.com/ra-company/logging"
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

func (t *TestReceiver) Execute() error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(t.Ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	logging.Logs.Infof(t.Ctx, "Executing TestReceiver with payload: %+v", t.Payload)
	return nil
}

type TestErrorReceiver struct {
	Payload ReceiverPayload
	Receiver
}

func (t *TestErrorReceiver) Execute() error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(t.Ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	logging.Logs.Infof(t.Ctx, "Executing TestReceiver with payload: %+v", t.Payload)
	return fmt.Errorf("simulated error in TestErrorReceiver")
}

type TestPanicReceiver struct {
	Payload ReceiverPayload
	Receiver
}

func (t *TestPanicReceiver) Execute() error {
	if err := json.Unmarshal([]byte(t.payload), &t.Payload); err != nil {
		logging.Logs.Errorf(t.Ctx, "Receiver %q load payload: %v", t.task, err)
		return nil
	}

	logging.Logs.Infof(t.Ctx, "Executing TestReceiver with payload: %+v", t.Payload)
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

	redisHosts := env.GetEnvStr("REDIS_HOSTS", "localhost:6379")
	require.NotEmpty(t, redisHosts, "REDIS_HOSTS should not be empty")
	redisPassword := env.GetEnvStr("REDIS_PWD", "-")
	redisDB := env.GetEnvInt("REDIS_DB", -1)
	require.GreaterOrEqual(t, redisDB, 0, "REDIS_DB should be defined")

	redis.Redis.Start(ctx, redisHosts, redisPassword, redisDB)

	bus.Init(ctx, &BusConfiguration{
		Redis: RedisConfiguration{
			Hosts:           redisHosts,
			Password:        redisPassword,
			DB:              redisDB,
			DoNotLogQueries: false, // Enable query logging for tests
		},
		Stream:       stream,
		Group:        group,
		WorkersCount: workersCount,
		NumRetries:   numRetries,
	})
	require.NotNil(t, bus.redis, "Redis client should be initialized")
	require.NotEmpty(t, bus.stream, "Stream should be set")
	require.NotEmpty(t, bus.group, "Group should be set")
	require.Greater(t, bus.workersCount, 0, "Workers count should be greater than 0")
	require.Greater(t, bus.numRetries, 0, "Number of retries should be greater than 0")

	defer redis.Redis.XGroupDestroy(ctx, bus.stream, bus.group)

	bus.RegisterReceiver(ctx, "receiver", func() ReceiverInterface { return &TestReceiver{} })
	require.NotNil(t, bus.receivers["receiver"], "Receiver should be registered")
	bus.RegisterReceiver(ctx, "error_receiver", func() ReceiverInterface { return &TestErrorReceiver{} })
	require.NotNil(t, bus.receivers["error_receiver"], "Error Receiver should be registered")
	bus.RegisterReceiver(ctx, "panic_receiver", func() ReceiverInterface { return &TestPanicReceiver{} })
	require.NotNil(t, bus.receivers["panic_receiver"], "Panic Receiver should be registered")

	timerCtx, cancel := context.WithTimeout(ctx, time.Second*2)
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

	time.Sleep(time.Second * 3)
}
