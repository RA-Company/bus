package bus

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// addKey publishes with a dedup key ("" = none) and reports whether the message was really added.
func (e *testEnv) addKey(receiver, key string) (string, bool) {
	e.t.Helper()
	before := e.b.Stats().Deduplicated
	id, err := e.b.AddMessage(e.t.Context(), receiver, ReceiverPayload{Title: "t", Foo: "f"}, key)
	require.NoError(e.t, err)
	return id, e.b.Stats().Deduplicated == before
}

func (e *testEnv) streamIDs() []string {
	e.t.Helper()
	entries, err := e.rdb.Client().XRange(e.t.Context(), e.b.stream, "-", "+").Result()
	require.NoError(e.t, err)
	ids := make([]string, 0, len(entries))
	for _, m := range entries {
		ids = append(ids, m.ID)
	}
	return ids
}

func (e *testEnv) cleanDedup() {
	e.t.Cleanup(func() {
		ctx := context.Background()
		keys, _ := scanAll(ctx, e.rdb.Client(), "{"+e.b.stream+"}:dedup:*")
		for _, k := range keys {
			_ = e.rdb.Del(ctx, k)
		}
	})
}

func redisDedupDuplicateIsNotAdded(t *testing.T) {
	env := newTestEnv(t, nil)
	env.cleanDedup()

	first, added := env.addKey("event", "hook-1")
	require.True(t, added)
	second, added := env.addKey("event", "hook-1")
	require.False(t, added, "the same key within the TTL is a duplicate")
	require.Equal(t, first, second, "a duplicate returns the id of the first message")
	require.Len(t, env.streamIDs(), 1)

	// The stored message is an ordinary one with the id that was returned.
	entries := env.deadlessEntries()
	require.Equal(t, first, entries[0].Values["id"])
	require.Equal(t, "event", entries[0].Values["receiver"])
	require.Len(t, entries[0].Values, 4, "message format is unchanged: receiver, id, payload, time")

	_, added = env.addKey("event", "hook-2")
	require.True(t, added, "another key is another message")
	_, added = env.addKey("other", "hook-1")
	require.True(t, added, "keys are per receiver")
	require.Len(t, env.streamIDs(), 3)
}

func (e *testEnv) deadlessEntries() []xmsg {
	e.t.Helper()
	entries, err := e.rdb.Client().XRange(e.t.Context(), e.b.stream, "-", "+").Result()
	require.NoError(e.t, err)
	out := make([]xmsg, len(entries))
	for i, m := range entries {
		out[i] = xmsg{ID: m.ID, Values: m.Values}
	}
	return out
}

type xmsg struct {
	ID     string
	Values map[string]any
}

func redisDedupNoKeyBehavesLikeAddMessage(t *testing.T) {
	env := newTestEnv(t, nil)
	_, added := env.addKey("event", "")
	require.True(t, added)
	_, added = env.addKey("event", "")
	require.True(t, added, "without a key there is no deduplication")
	require.Len(t, env.streamIDs(), 2)

	_, err := env.b.AddMessage(t.Context(), "", nil, "k")
	require.ErrorIs(t, err, ErrorEmptyReceiverTitle)
	_, err = env.b.AddMessage(t.Context(), "event", func() {}, "k")
	require.Error(t, err, "a payload that cannot be serialised is an error")
}

func redisDedupKeyExpires(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.DedupTTL = 300 * time.Millisecond })
	env.cleanDedup()

	_, added := env.addKey("event", "k")
	require.True(t, added)

	keys, err := scanAll(t.Context(), env.rdb.Client(), "{"+env.b.stream+"}:dedup:*")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	ttl, err := env.rdb.Client().PTTL(t.Context(), keys[0]).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 300*time.Millisecond)

	_, added = env.addKey("event", "k")
	require.False(t, added)
	require.Eventually(t, func() bool {
		_, added := env.addKey("event", "k")
		return added
	}, 5*time.Second, 50*time.Millisecond, "after the TTL the same key is accepted again")
	require.Len(t, env.streamIDs(), 2)
}

// Concurrent producers publishing the same key add exactly one message, and the duplicate-free
// stream is consumed once by the real workers.
func redisDedupConcurrentProducers(t *testing.T) {
	env := newTestEnv(t, nil)
	env.cleanDedup()

	var runs atomic.Int32
	env.register("event", func(context.Context, *scriptReceiver) error { runs.Add(1); return nil })

	var wg sync.WaitGroup
	ids := make(chan string, 40)
	for range 40 {
		wg.Go(func() {
			id, err := env.b.AddMessage(t.Context(), "event", ReceiverPayload{Title: "t", Foo: "f"}, "same")
			if err == nil {
				ids <- id
			}
		})
	}
	wg.Wait()
	close(ids)

	require.EqualValues(t, 39, env.b.Stats().Deduplicated)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	require.Len(t, seen, 1, "everybody gets the id of the one message")
	require.Len(t, env.streamIDs(), 1)

	stop := env.startBus()
	require.Eventually(t, func() bool { return runs.Load() == 1 }, 10*time.Second, 20*time.Millisecond)
	stop()
	require.EqualValues(t, 1, runs.Load())
}

// StreamSize still applies to messages added with a key. The trimming is approximate (whole nodes of
// stream-node-max-entries, 100 by default), so enough messages are added to fill several nodes.
func redisDedupKeepsStreamLimits(t *testing.T) {
	const size, added, node = 3, 350, 100
	env := newTestEnv(t, func(c *BusConfiguration) { c.StreamSize = size })
	env.cleanDedup()
	for i := range added {
		_, ok := env.addKey("event", strconv.Itoa(i))
		require.True(t, ok)
	}
	n := len(env.streamIDs())
	require.GreaterOrEqual(t, n, size)
	require.LessOrEqual(t, n, size+node, "the stream must be trimmed to about StreamSize")
}

func TestDedup_DuplicateIsNotAdded(t *testing.T) { eachRedis(t, redisDedupDuplicateIsNotAdded) }
func TestDedup_NoKeyBehavesLikeAddMessage(t *testing.T) {
	eachRedis(t, redisDedupNoKeyBehavesLikeAddMessage)
}
func TestDedup_KeyExpires(t *testing.T)          { eachRedis(t, redisDedupKeyExpires) }
func TestDedup_ConcurrentProducers(t *testing.T) { eachRedis(t, redisDedupConcurrentProducers) }
func TestDedup_KeepsStreamLimits(t *testing.T)   { eachRedis(t, redisDedupKeepsStreamLimits) }

func TestDedup_DefaultTTL(t *testing.T) {
	eachRedis(t, func(t *testing.T) {
		env := newTestEnv(t, nil)
		require.Equal(t, 10*time.Minute, env.b.dedupTTL)
		env = newTestEnv(t, func(c *BusConfiguration) { c.DedupTTL = time.Hour })
		require.Equal(t, time.Hour, env.b.dedupTTL)
	})
}

func TestDedup_KeyName(t *testing.T) {
	b := &Bus{stream: "plain"}
	k, err := b.dedupKeyName("r", "k")
	require.NoError(t, err)
	require.Contains(t, k, "{plain}:dedup:")
	k2, _ := b.dedupKeyName("r2", "k")
	require.NotEqual(t, k, k2)

	b.stream = "pre{tag}post"
	k, err = b.dedupKeyName("r", "k")
	require.NoError(t, err)
	require.Contains(t, k, "{tag}:dedup:")

	for _, bad := range []string{"a{b", "a{}b", "a}b"} {
		b.stream = bad
		_, err = b.dedupKeyName("r", "k")
		require.ErrorIs(t, err, ErrorDedupStreamName, bad)
	}
}
