package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func (e *testEnv) counters(group string, days int) []DayCounters {
	e.t.Helper()
	out, err := e.b.Counters(e.t.Context(), group, days)
	require.NoError(e.t, err)
	return out
}

// Every outcome of a delivery is counted in Redis, in total and per receiver.
func redisStatsCountOutcomes(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.NumRetries = 2 })
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.register("bad", func(context.Context, *scriptReceiver) error { return errors.New("always fails") })
	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("invalid payload")) })

	env.add("ok")
	env.add("ok")
	env.add("bad")
	env.add("perm")
	env.add("ghost") // nobody registered a receiver for it

	for range 5 {
		env.drain(env.deliver())
	}

	today := env.counters("", 1)[0]
	require.Equal(t, time.Now().UTC().Format("2006-01-02"), today.Date)
	require.Equal(t, Counters{
		Published:    5,
		Received:     7, // ok 2, bad 3 (two executions and the move to the dead-letter stream), perm 1, ghost 1
		Processed:    2,
		Failed:       3, // bad twice, perm once
		Retried:      1, // the second execution of bad
		DeadLettered: 2, // bad (retries exhausted) and perm
		Unregistered: 1,
	}, today.Total)

	require.Equal(t, Counters{Published: 2, Received: 2, Processed: 2}, today.ByReceiver["ok"])
	require.Equal(t, Counters{Published: 1, Received: 3, Failed: 2, Retried: 1, DeadLettered: 1}, today.ByReceiver["bad"])
	require.Equal(t, Counters{Published: 1, Received: 1, Failed: 1, DeadLettered: 1}, today.ByReceiver["perm"])
	require.Equal(t, Counters{Published: 1, Received: 1, Unregistered: 1}, today.ByReceiver["ghost"])

	// The in-process snapshot agrees with Redis.
	st := env.b.Stats()
	require.EqualValues(t, today.Total.Processed, st.Processed)
	require.EqualValues(t, today.Total.Failed, st.Failed)
	require.EqualValues(t, today.Total.DeadLettered, st.DeadLettered)
}

// Counters are shared by all instances of a group and are not lost with a process: a second Bus on the same
// stream and group adds to the same numbers, and a reader that is not a consumer sees them.
func redisStatsAreSharedAndPersistent(t *testing.T) {
	env := newTestEnv(t, nil)
	other := env.restarted()
	for _, e := range []*testEnv{env, other} {
		e.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	}

	env.add("ok")
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
	require.NoError(t, other.b.processMessage(t.Context(), env.deliver()))

	for _, b := range []*Bus{env.b, other.b} {
		got, err := b.Counters(t.Context(), "", 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, got[0].Total.Processed, "both instances' work is counted")
		require.EqualValues(t, 2, got[0].Total.Published)
	}
}

// Counters of any group can be read, published counters belong to the stream, days come newest first.
func redisStatsOtherGroupAndDays(t *testing.T) {
	env := newTestEnv(t, nil)
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))

	got := env.counters("someone-else", 3)
	require.Len(t, got, 3)
	require.Equal(t, time.Now().UTC().Format("2006-01-02"), got[0].Date)
	require.Equal(t, time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02"), got[1].Date)
	require.EqualValues(t, 1, got[0].Total.Published, "published counters are per stream")
	require.EqualValues(t, 0, got[0].Total.Processed, "another group has processed nothing")
	require.Equal(t, Counters{}, got[1].Total)
	require.Len(t, env.counters("", 0), 1, "days is at least 1")
}

// Duplicates skipped by a dedup key are counted.
func redisStatsCountDeduplicated(t *testing.T) {
	env := newTestEnv(t, nil)
	for range 3 {
		_, err := env.b.AddMessage(t.Context(), "event", ReceiverPayload{Title: "t"}, "same")
		require.NoError(t, err)
	}
	got := env.counters("", 1)[0]
	require.EqualValues(t, 1, got.Total.Published)
	require.EqualValues(t, 2, got.Total.Deduplicated)
	require.EqualValues(t, 2, got.ByReceiver["event"].Deduplicated)
}

// Counters expire.
func redisStatsExpire(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StatsRetention = time.Hour })
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))

	now := time.Now()
	for _, key := range []string{env.b.groupStatsKey(env.b.group, statsDate(now)), env.b.publishedStatsKey(statsDate(now))} {
		ttl, err := env.rdb.Client().PTTL(t.Context(), key).Result()
		require.NoError(t, err)
		require.Greater(t, ttl, time.Duration(0), key)
		require.LessOrEqual(t, ttl, time.Hour, key)
	}
}

// A negative StatsRetention switches the counters off: plain XADD and XACK, no keys.
func redisStatsOff(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StatsRetention = -1 })
	hook := &countingHook{}
	env.b.redis.Client().(interface{ AddHook(re.Hook) }).AddHook(hook)
	hook.enabled.Store(true)

	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })
	env.add("ok")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))

	snap := hook.snapshot()
	require.EqualValues(t, 1, snap["xadd"])
	require.EqualValues(t, 1, snap["xack"])
	require.EqualValues(t, 0, snap["evalsha"]+snap["eval"], "no scripts without counters")
	keys, err := scanAll(t.Context(), env.rdb.Client(), "{"+env.b.stream+"}:stats:*")
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Equal(t, Counters{}, env.counters("", 1)[0].Total)
	require.EqualValues(t, 1, env.b.Stats().Processed, "the in-process stats work as before")
}

// A broken counter (wrong type of the key) must neither fail the publication nor leave a message unacknowledged.
func redisStatsFailureDoesNotAffectDelivery(t *testing.T) {
	env := newTestEnv(t, nil)
	now := time.Now()
	for _, key := range []string{env.b.groupStatsKey(env.b.group, statsDate(now)), env.b.publishedStatsKey(statsDate(now))} {
		require.NoError(t, env.rdb.Set(t.Context(), key, "not a hash", 60))
	}
	env.register("ok", func(context.Context, *scriptReceiver) error { return nil })

	env.add("ok")
	require.EqualValues(t, 1, env.pendingOrLen(), "the message was published")
	require.NoError(t, env.b.processMessage(t.Context(), env.deliver()))
	require.EqualValues(t, 0, env.pending(), "the message was acknowledged")
}

func (e *testEnv) pendingOrLen() int64 {
	e.t.Helper()
	n, err := e.rdb.Client().XLen(e.t.Context(), e.b.stream).Result()
	require.NoError(e.t, err)
	return n
}

// Receiver names without a registered receiver get their own counters only up to a limit.
func redisStatsNameLimit(t *testing.T) {
	env := newTestEnv(t, nil)
	const extra = 20
	for i := range statNameLimit + extra {
		_, err := env.b.AddMessage(t.Context(), fmt.Sprintf("event-%d", i), nil, "")
		require.NoError(t, err)
	}
	got := env.counters("", 1)[0]
	require.EqualValues(t, statNameLimit+extra, got.Total.Published)
	require.Len(t, got.ByReceiver, statNameLimit+1)
	require.EqualValues(t, extra, got.ByReceiver[statNameOther].Published)
}

// Snapshot shows the size of the stream, the dead-letter stream and the state of every group.
func redisSnapshot(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.StreamSize = 100 })
	require.NoError(t, env.b.EnsureGroups(t.Context(), "other"))
	env.register("perm", func(context.Context, *scriptReceiver) error { return Permanent(errors.New("invalid")) })

	env.add("later") // stays pending
	env.add("perm")  // goes to the dead-letter stream
	env.add("event") // undelivered
	env.deliver()
	require.True(t, IsPermanent(env.b.processMessage(t.Context(), env.deliver()))) // dead-lettered and acknowledged
	time.Sleep(100 * time.Millisecond)

	snap, err := env.b.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, env.b.stream, snap.Stream)
	require.EqualValues(t, 3, snap.Size)
	require.EqualValues(t, 100, snap.SizeLimit)
	require.EqualValues(t, 1, snap.DeadLetterSize)
	require.Len(t, snap.Groups, 2)

	byName := map[string]GroupSnapshot{}
	for _, g := range snap.Groups {
		byName[g.Name] = g
	}
	own := byName[env.b.group]
	require.EqualValues(t, 1, own.Pending)
	require.GreaterOrEqual(t, own.OldestPending, 100*time.Millisecond, "the pending message is older than the pause")
	require.Less(t, own.OldestPending, time.Minute)
	other := byName["other"]
	require.EqualValues(t, 0, other.Pending, "a group that has not started has nothing pending")
	require.Zero(t, other.OldestPending)

	require.EqualValues(t, 1, own.Lag, "one message is not delivered to the bus' group")
	require.EqualValues(t, 3, other.Lag, "the other group has seen nothing")
}

func TestStats_CountOutcomes(t *testing.T)       { eachRedis(t, redisStatsCountOutcomes) }
func TestStats_SharedAndPersistent(t *testing.T) { eachRedis(t, redisStatsAreSharedAndPersistent) }
func TestStats_OtherGroupAndDays(t *testing.T)   { eachRedis(t, redisStatsOtherGroupAndDays) }
func TestStats_CountDeduplicated(t *testing.T)   { eachRedis(t, redisStatsCountDeduplicated) }
func TestStats_Expire(t *testing.T)              { eachRedis(t, redisStatsExpire) }
func TestStats_Off(t *testing.T)                 { eachRedis(t, redisStatsOff) }
func TestStats_FailureDoesNotAffectDelivery(t *testing.T) {
	eachRedis(t, redisStatsFailureDoesNotAffectDelivery)
}
func TestStats_NameLimit(t *testing.T) { eachRedis(t, redisStatsNameLimit) }
func TestSnapshot(t *testing.T)        { eachRedis(t, redisSnapshot) }
