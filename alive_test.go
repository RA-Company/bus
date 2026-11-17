package bus

import (
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func (e *testEnv) groupSnapshot(group string) GroupSnapshot {
	e.t.Helper()
	snap, err := e.b.Snapshot(e.t.Context())
	require.NoError(e.t, err)
	for _, g := range snap.Groups {
		if g.Name == group {
			return g
		}
	}
	e.t.Fatalf("group %q is not in the snapshot", group)
	return GroupSnapshot{}
}

func (e *testEnv) liveConsumers() int { return e.groupSnapshot(e.b.group).LiveConsumers }

// Live consumers are the workers of running instances, summed over instances, and they are gone as soon as
// an instance stops, whatever records the stream keeps.
func redisLiveConsumers(t *testing.T) {
	env := newTestEnv(t, nil) // 2 workers
	other := env.restarted()  // a second instance (another server) of the same service: 2 workers

	require.Equal(t, 0, env.liveConsumers(), "nobody runs yet")

	done1 := env.runBus(t.Context())
	require.Eventually(t, func() bool { return env.liveConsumers() == 2 }, 5*time.Second, 20*time.Millisecond)
	snap := env.groupSnapshot(env.b.group)
	require.Len(t, snap.Instances, 1)
	require.Equal(t, 2, snap.Instances[0].Workers)
	require.Contains(t, snap.Instances[0].Name, "/", "host/pid/suffix")

	done2 := other.runBus(t.Context())
	require.Eventually(t, func() bool { return env.liveConsumers() == 4 }, 5*time.Second, 20*time.Millisecond)
	require.Len(t, env.groupSnapshot(env.b.group).Instances, 2)

	require.NoError(t, other.b.Stop(t.Context()))
	waitClosed(t, done2, 5*time.Second, "second Start to return")
	require.Equal(t, 2, env.liveConsumers(), "a stopped instance leaves the registry at once")

	require.NoError(t, env.b.Stop(t.Context()))
	waitClosed(t, done1, 5*time.Second, "first Start to return")
	require.Equal(t, 0, env.liveConsumers())
}

// A restart does not inflate the number: the stream keeps the records of the old workers, the live count does not.
func redisLiveConsumersAfterRestart(t *testing.T) {
	env := newTestEnv(t, nil)
	consumers := func() int {
		cs, err := env.rdb.Client().XInfoConsumers(t.Context(), env.b.stream, env.b.group).Result()
		require.NoError(t, err)
		return len(cs)
	}

	done := env.runBus(t.Context())
	require.Eventually(t, func() bool { return consumers() >= 2 }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, env.b.Stop(t.Context()))
	waitClosed(t, done, 5*time.Second, "Start to return")

	again := env.restarted()
	done = again.runBus(t.Context())
	require.Eventually(t, func() bool { return consumers() >= 4 }, 5*time.Second, 20*time.Millisecond,
		"the records of the old workers are still in the group")
	require.Equal(t, 2, env.liveConsumers(), "but only the workers that run now are alive")
	require.NoError(t, again.b.Stop(t.Context()))
	waitClosed(t, done, 5*time.Second, "second Start to return")
}

// An instance that stops renewing its heartbeat (crash, power loss) is dead after aliveTTL, and old entries are removed.
func redisLiveConsumersExpire(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.heartbeatEvery = 100 * time.Millisecond
	env.b.aliveTTL = 500 * time.Millisecond
	key := env.b.aliveKey(env.b.group)
	zadd := func(member string, age time.Duration) {
		require.NoError(t, env.rdb.Client().ZAdd(t.Context(), key, re.Z{
			Score: float64(time.Now().Add(-age).UnixMilli()), Member: member,
		}).Err())
	}

	// A crashed instance that was alive a moment ago, and one that was lost long ago.
	zadd("crashed/1/aaaa|w1", 0)
	zadd("crashed/1/aaaa|w2", 0)
	zadd("ancient/2/bbbb|w1", time.Hour)
	require.Equal(t, 2, env.liveConsumers())

	done := env.runBus(t.Context())
	require.Eventually(t, func() bool { return env.liveConsumers() == 2 }, 5*time.Second, 20*time.Millisecond,
		"the crashed workers expire, the two running ones stay")
	time.Sleep(time.Second) // longer than aliveTTL: the running instance is kept alive by its heartbeat
	require.Equal(t, 2, env.liveConsumers())

	_, err := env.rdb.Client().ZScore(t.Context(), key, "ancient/2/bbbb|w1").Result()
	require.ErrorIs(t, err, re.Nil, "an entry that has been dead for long is removed")

	require.NoError(t, env.b.Stop(t.Context()))
	waitClosed(t, done, 5*time.Second, "Start to return")
}

// Consumer records of dead workers are deleted; a record with pending messages, a live one and a recent one stay.
func redisPurgeConsumers(t *testing.T) {
	env := newTestEnv(t, nil)
	env.b.purgeIdle = 150 * time.Millisecond

	read := func(consumer string) {
		_, err := env.rdb.Client().XReadGroup(t.Context(), &re.XReadGroupArgs{
			Group: env.b.group, Consumer: consumer, Streams: []string{env.b.stream, ">"}, Count: 1, Block: -1,
		}).Result()
		if err != nil {
			require.ErrorIs(t, err, re.Nil)
		}
	}
	env.add("event")
	read("holds-pending") // takes the message: pending
	read("dead-idle")     // creates the record, nothing pending
	read("alive-idle")
	require.NoError(t, env.rdb.Client().ZAdd(t.Context(), env.b.aliveKey(env.b.group),
		re.Z{Score: float64(time.Now().UnixMilli()), Member: "somewhere/1/cccc|alive-idle"}).Err())
	time.Sleep(300 * time.Millisecond)
	read("recent")

	env.b.purgeConsumers(t.Context())

	cs, err := env.rdb.Client().XInfoConsumers(t.Context(), env.b.stream, env.b.group).Result()
	require.NoError(t, err)
	var names []string
	for _, c := range cs {
		names = append(names, c.Name)
	}
	require.NotContains(t, names, "dead-idle")
	require.Contains(t, names, "holds-pending", "deleting it would drop its pending message")
	require.Contains(t, names, "alive-idle", "a worker in the registry is alive")
	require.Contains(t, names, "recent")
	require.EqualValues(t, 1, env.pending(), "nothing pending was lost")
}

// Close alone, without waiting for Start to return, leaves the registry before the connection is closed.
func redisCloseLeavesRegistry(t *testing.T) {
	env := newTestEnv(t, nil)
	key := env.b.aliveKey(env.b.group)
	done := env.runBus(t.Context())
	require.Eventually(t, func() bool { return env.liveConsumers() == 2 }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, env.b.Close(t.Context()))
	n, err := env.rdb.Client().ZCard(t.Context(), key).Result()
	require.NoError(t, err)
	require.Zero(t, n, "the workers are removed from the registry before Close returns")
	waitClosed(t, done, 5*time.Second, "Start to return")
}

func TestAlive_CloseLeavesRegistry(t *testing.T)       { eachRedis(t, redisCloseLeavesRegistry) }
func TestAlive_LiveConsumers(t *testing.T)             { eachRedis(t, redisLiveConsumers) }
func TestAlive_LiveConsumersAfterRestart(t *testing.T) { eachRedis(t, redisLiveConsumersAfterRestart) }
func TestAlive_Expire(t *testing.T)                    { eachRedis(t, redisLiveConsumersExpire) }
func TestAlive_PurgeConsumers(t *testing.T)            { eachRedis(t, redisPurgeConsumers) }
