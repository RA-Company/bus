package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// The mark of a recreated stream
// ---------------------------------------------------------------------------

// dropStream deletes the stream key and its mark, as if the stream had been created long ago and deleted now.
func (e *testEnv) dropStream() {
	e.t.Helper()
	require.NoError(e.t, e.rdb.Del(e.t.Context(), e.b.stream))
	require.NoError(e.t, e.rdb.Del(e.t.Context(), e.b.rebornKey()))
}

func (e *testEnv) rebornTTL() time.Duration {
	e.t.Helper()
	d, err := e.rdb.Client().PTTL(e.t.Context(), e.b.rebornKey()).Result()
	require.NoError(e.t, err)
	return d // negative when the key does not exist
}

// The stream is deleted, producers recreate it before the workers notice: the group starts at the beginning
// of the new stream and loses nothing.
func redisRebirthByProducerLosesNothing(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	var runs atomic.Int32
	env.register("r", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })

	env.dropStream()
	env.add("r") // recreates the stream and marks it
	env.add("r")
	require.Positive(t, env.rebornTTL(), "the producer that created the stream must mark it")

	env.readOnce() // NOGROUP: the group is created again, at "0" because of the mark
	require.Equal(t, "0-0", env.groupInfo().LastDeliveredID)
	env.readOnce()
	env.readOnce()
	require.EqualValues(t, 2, runs.Load(), "the messages added to the recreated stream must not be lost")
}

// A producer that publishes to an existing stream does not mark it.
func redisPublishToExistingStreamDoesNotMark(t *testing.T) {
	env := newTestEnv(t, nil)
	require.NoError(t, env.rdb.Del(t.Context(), env.b.rebornKey())) // Init created the stream and marked it
	env.add("r")
	require.Negative(t, env.rebornTTL())
}

// The service is down while the stream is deleted and recreated: Init finds no group and starts it at the
// beginning of the new stream.
func redisRebirthWhileDownIsCaughtByInit(t *testing.T) {
	env := newTestEnv(t, nil)
	env.dropStream()
	env.add("r")

	b := &Bus{}
	require.NoError(t, b.Init(t.Context(), &BusConfiguration{Redis: redisConfig(t), Stream: env.b.stream, Group: "restarted"}))
	streams, err := env.rdb.XReadGroup(t.Context(), &re.XReadGroupArgs{
		Group: "restarted", Consumer: "c", Streams: []string{env.b.stream, ">"}, Count: 10, Block: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Messages, 1, "a group created after the stream was recreated must see its messages")
}

// A worker that creates the stream before any producer marks it too, so another group that notices the loss later
// does not skip what producers added meanwhile.
func redisRebirthByConsumerIsMarked(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	env.dropStream()

	env.readOnce() // NOGROUP: the worker creates the stream (empty) and the group
	require.Positive(t, env.rebornTTL(), "the consumer that created the stream must mark it")

	env.add("r") // the stream exists now: the producer does not mark it again
	require.NoError(t, env.b.EnsureGroups(t.Context(), "other-service"))
	streams, err := env.rdb.XReadGroup(t.Context(), &re.XReadGroupArgs{
		Group: "other-service", Consumer: "c", Streams: []string{env.b.stream, ">"}, Count: 10, Block: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Messages, 1)
}

func redisRebirthTTLIsConfigurable(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StreamRebirthTTL = 5 * time.Minute })
	env.dropStream()
	env.add("r")
	ttl := env.rebornTTL()
	require.Positive(t, ttl)
	require.LessOrEqual(t, ttl, 5*time.Minute)
}

func redisRebirthMarkOff(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StreamRebirthTTL = -1 })
	env.dropStream()
	env.add("r")
	require.Negative(t, env.rebornTTL(), "no mark when StreamRebirthTTL is negative")
}

// A stream name whose braces do not form a hash tag cannot have a mark in its slot: the bus still works, a new
// stream starts at "0" and an existing one at "$".
func redisRebirthWithoutTag(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.Stream = "bus-test-" + uuid.NewString() + "}x" })
	require.Empty(t, env.b.tag)
	env.b.readBackoff = 10 * time.Millisecond
	var runs atomic.Int32
	env.register("r", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })

	env.add("r")
	env.readOnce()
	require.EqualValues(t, 1, runs.Load())

	require.NoError(t, env.rdb.Del(t.Context(), env.b.stream))
	env.readOnce() // NOGROUP: new empty stream, group at "0"
	require.Equal(t, "0-0", env.groupInfo().LastDeliveredID)
}

func TestRebirth_ByProducerLosesNothing(t *testing.T) {
	eachRedis(t, redisRebirthByProducerLosesNothing)
}

func TestRebirth_PublishToExistingStreamDoesNotMark(t *testing.T) {
	eachRedis(t, redisPublishToExistingStreamDoesNotMark)
}

func TestRebirth_WhileDownIsCaughtByInit(t *testing.T) {
	eachRedis(t, redisRebirthWhileDownIsCaughtByInit)
}

func TestRebirth_ByConsumerIsMarked(t *testing.T) { eachRedis(t, redisRebirthByConsumerIsMarked) }

func TestRebirth_TTLIsConfigurable(t *testing.T) { eachRedis(t, redisRebirthTTLIsConfigurable) }

func TestRebirth_MarkOff(t *testing.T) { eachRedis(t, redisRebirthMarkOff) }

func TestRebirth_WithoutTag(t *testing.T) { eachRedis(t, redisRebirthWithoutTag) }

// ---------------------------------------------------------------------------
// Only the group was destroyed: it is recreated without running the stream again
// ---------------------------------------------------------------------------

func (e *testEnv) groupInfo() re.XInfoGroup {
	e.t.Helper()
	groups, err := e.rdb.Client().XInfoGroups(e.t.Context(), e.b.stream).Result()
	require.NoError(e.t, err)
	for _, g := range groups {
		if g.Name == e.b.group {
			return g
		}
	}
	e.t.Fatalf("group %q not found", e.b.group)
	return re.XInfoGroup{}
}

// Only the group was destroyed: the entries in the stream were processed already and must not run again.
func redisRecreatedGroupSkipsExistingEntries(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	var runs int
	env.register("r", func(context.Context, *scriptReceiver) error { runs++; return nil })
	env.add("r")
	env.add("r")

	// An old stream: its mark of creation has expired.
	require.NoError(t, env.rdb.Del(t.Context(), env.b.rebornKey()))
	require.NoError(t, env.rdb.XGroupDestroy(t.Context(), env.b.stream, env.b.group))
	env.readOnce() // NOGROUP: the group is created again

	entries, err := env.rdb.Client().XRange(t.Context(), env.b.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, entries[1].ID, env.groupInfo().LastDeliveredID, "the group must start at the end of the stream")

	env.readOnce()
	require.Zero(t, runs, "old entries must not be executed again")

	env.add("r") // new messages are delivered as usual
	env.readOnce()
	require.Equal(t, 1, runs)
}

// The stream key was deleted: the group starts at the beginning, so a message added meanwhile is processed.
func redisRecreatedGroupOnNewStreamStartsAtBeginning(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.readBackoff = 10 * time.Millisecond
	require.NoError(t, env.rdb.Del(t.Context(), env.b.stream))

	env.readOnce()
	require.Equal(t, "0-0", env.groupInfo().LastDeliveredID)
}

func TestRecreateGroup_SkipsExistingEntries(t *testing.T) {
	eachRedis(t, redisRecreatedGroupSkipsExistingEntries)
}

func TestRecreateGroup_NewStreamStartsAtBeginning(t *testing.T) {
	eachRedis(t, redisRecreatedGroupOnNewStreamStartsAtBeginning)
}
