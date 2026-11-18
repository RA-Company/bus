package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Every failure is logged once, by the worker, with the message ids
// ---------------------------------------------------------------------------

func redisExecuteErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("fail", func(context.Context, *scriptReceiver) error { return errors.New("boom-execute-7f3") })
	env.add("fail")

	env.b.lastClaim.Store(time.Now().UnixNano())
	env.b.workerJob(t.Context(), "w1")

	require.Len(t, env.log.errorsMentioning("boom-execute-7f3"), 1, "an execution error must be logged exactly once")
}

func redisStartErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("bad_payload", func(context.Context, *scriptReceiver) error { return nil })
	_, err := env.rdb.Client().XAdd(t.Context(), &re.XAddArgs{
		Stream: env.b.stream,
		Values: map[string]any{"receiver": "bad_payload", "id": uuid.NewString(), "payload": "not-json"},
	}).Result()
	require.NoError(t, err)

	env.b.lastClaim.Store(time.Now().UnixNano())
	env.b.workerJob(t.Context(), "w1")

	logged := env.log.errorsMentioning("unmarshal payload")
	require.Len(t, logged, 1, "a start error must be logged exactly once")
	require.Contains(t, logged[0], "bad_payload", "the log line must name the receiver")
}

func redisPermanentErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("perm", func(context.Context, *scriptReceiver) error {
		return Permanent(errors.New("boom-permanent-a91"))
	})
	env.add("perm")

	env.b.lastClaim.Store(time.Now().UnixNano())
	env.b.workerJob(t.Context(), "w1")

	require.Len(t, env.log.errorsMentioning("boom-permanent-a91"), 1, "a permanent error must be logged exactly once")
}

func TestLogging_ExecuteErrorOnce(t *testing.T) { eachRedis(t, redisExecuteErrorLoggedOnce) }

func TestLogging_StartErrorOnce(t *testing.T) { eachRedis(t, redisStartErrorLoggedOnce) }

func TestLogging_PermanentErrorOnce(t *testing.T) { eachRedis(t, redisPermanentErrorLoggedOnce) }

func redisAckErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")

	env.hook.set(denyAck)
	env.readOnce()
	env.hook.set(nil)

	require.Len(t, env.log.errorsMentioning("acknowledge message"), 1)
}

func redisDeliveryCountErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")
	msg := env.deliver()

	env.hook.set(denyAllExceptStreamDelivery)
	require.Error(t, env.b.processMessage(t.Context(), msg))
	env.hook.set(nil)

	require.Len(t, env.log.errorsMentioning("delivery count"), 1)
}

func redisDeadLetterErrorLoggedOnce(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.Unregistered = UnregisteredDeadLetter })
	env.add("ghost")
	msg := env.deliver()

	env.hook.set(func(cmd string) bool { return cmd == "xadd" })
	require.Error(t, env.b.processMessage(t.Context(), msg))
	env.hook.set(nil)

	require.Len(t, env.log.errorsMentioning("dead-letter"), 1)
	require.EqualValues(t, 1, env.pendingCount(), "a message that could not be moved stays pending")
}

func redisInvalidReceiverLoggedOnce(t *testing.T) {
	env := newTestEnv(t, nil)
	_, err := env.rdb.Client().XAdd(t.Context(), &re.XAddArgs{
		Stream: env.b.stream, Values: map[string]any{"id": "x", "payload": "{}"},
	}).Result()
	require.NoError(t, err)

	env.readOnce()
	require.Len(t, env.log.errorsMentioning("invalid receiver name"), 1)
	require.Zero(t, env.pendingCount(), "a message without a receiver is acknowledged")
}

// A failed acknowledgement of an ignored message is still reported.
func redisUnregisteredAckErrorIsLogged(t *testing.T) {
	env := newTestEnv(t, nil)
	env.add("ghost")

	env.hook.set(denyAck)
	env.readOnce()
	env.hook.set(nil)

	require.Len(t, env.log.errorsMentioning("acknowledge message"), 1)
}

func redisTimeoutLoggedOnceAsError(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.HandlerTimeout = 50 * time.Millisecond })
	env.register("slow", func(ctx context.Context, _ *scriptReceiver) error { <-ctx.Done(); return ctx.Err() })
	env.add("slow")

	env.readOnce()
	require.Len(t, env.log.errorsMentioning("did not finish"), 1, "the timeout is one error; the moment it fires is a warning")
}

func TestLogging_AckErrorOnce(t *testing.T) { eachRedis(t, redisAckErrorLoggedOnce) }

func TestLogging_DeliveryCountErrorOnce(t *testing.T) {
	eachRedis(t, redisDeliveryCountErrorLoggedOnce)
}

func TestLogging_DeadLetterErrorOnce(t *testing.T) { eachRedis(t, redisDeadLetterErrorLoggedOnce) }

func TestLogging_InvalidReceiverOnce(t *testing.T) { eachRedis(t, redisInvalidReceiverLoggedOnce) }

func TestLogging_UnregisteredAckError(t *testing.T) { eachRedis(t, redisUnregisteredAckErrorIsLogged) }

func TestLogging_TimeoutOnce(t *testing.T) { eachRedis(t, redisTimeoutLoggedOnceAsError) }
