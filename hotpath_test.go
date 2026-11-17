package bus

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Hot path: how many Redis commands one message costs.
// ---------------------------------------------------------------------------

// countingHook counts the commands the bus sends, by name.
type countingHook struct {
	on sync.Map // string -> *atomic.Int64
	// enabled gates counting, so setup commands are not counted.
	enabled atomic.Bool
}

func (h *countingHook) count(name string) {
	if !h.enabled.Load() {
		return
	}
	v, _ := h.on.LoadOrStore(name, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (h *countingHook) snapshot() map[string]int64 {
	out := map[string]int64{}
	h.on.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

func (h *countingHook) DialHook(next re.DialHook) re.DialHook { return next }

func (h *countingHook) ProcessHook(next re.ProcessHook) re.ProcessHook {
	return func(ctx context.Context, cmd re.Cmder) error {
		h.count(cmd.Name())
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next re.ProcessPipelineHook) re.ProcessPipelineHook {
	return func(ctx context.Context, cmds []re.Cmder) error {
		for _, cmd := range cmds {
			h.count(cmd.Name())
		}
		return next(ctx, cmds)
	}
}

// hotPathResult is what measureHotPath reports.
type hotPathResult struct {
	elapsed  time.Duration
	commands map[string]int64 // by command name, while the messages were being processed
}

func (r hotPathResult) total() (n int64) {
	for _, v := range r.commands {
		n += v
	}
	return n
}

func (r hotPathResult) String() string {
	names := make([]string, 0, len(r.commands))
	for k := range r.commands {
		names = append(names, k)
	}
	sort.Strings(names)
	s := ""
	for _, k := range names {
		s += k + "=" + itoa(r.commands[k]) + " "
	}
	return s
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for ; n > 0; n /= 10 {
		i--
		b[i] = byte('0' + n%10)
	}
	return string(b[i:])
}

// measureHotPath publishes n messages with a trivial receiver, runs the real worker loop until all of
// them are processed and returns the Redis commands the bus issued meanwhile (publishing excluded).
func measureHotPath(tb testing.TB, mode string, workers int64, n int) hotPathResult {
	tb.Helper()
	cfg, ok := redisConfigFor(mode)
	if !ok {
		tb.Skipf("%s Redis is not configured", mode)
	}

	ctx := context.Background()
	b := &Bus{}
	require.NoError(tb, b.Init(ctx, &BusConfiguration{
		Redis:         cfg,
		Stream:        "bus-hot-" + uuid.NewString(),
		Group:         "group",
		WorkersCount:  workers,
		RetryIdleTime: 60,
	}))
	tb.Cleanup(func() {
		_ = b.redis.XGroupDestroy(ctx, b.stream, b.group)
		_ = b.redis.Del(ctx, b.stream)
	})

	hook := &countingHook{}
	b.redis.Client().(interface{ AddHook(re.Hook) }).AddHook(hook)

	b.RegisterReceiver(ctx, "event", func() ReceiverInterface {
		return &scriptReceiver{run: func(context.Context, *scriptReceiver) error { return nil }}
	})
	for range n {
		_, err := b.AddMessage(ctx, "event", ReceiverPayload{Title: "t", Foo: "f"}, "")
		require.NoError(tb, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	hook.enabled.Store(true)
	started := time.Now()
	go func() {
		b.Start(runCtx)
		close(done)
	}()

	require.Eventually(tb, func() bool { return b.Stats().Processed >= int64(n) }, 60*time.Second, time.Millisecond)
	res := hotPathResult{elapsed: time.Since(started), commands: hook.snapshot()}
	hook.enabled.Store(false)

	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		tb.Fatal("bus did not stop")
	}
	return res
}

func redisHotPathCommands(t *testing.T) {
	const n = 300
	res := measureHotPath(t, redisMode(t), 4, n)
	t.Logf("%d messages, 4 workers: %s | total %d = %.2f per message | %v", n, res, res.total(), float64(res.total())/n, res.elapsed)

	// Every message costs one XREADGROUP and one acknowledgement (with the counters), and nothing else on the way: its delivery
	// counter is known to be 1 without asking Redis, and the search for idle messages (XPENDING IDLE) is
	// made by time, not before every message.
	// XACK and the counters are one script call per message (EVALSHA, plus EVAL the first time on each node).
	require.EqualValues(t, 0, res.commands["xack"])
	acks := res.commands["evalsha"] + res.commands["eval"]
	require.GreaterOrEqual(t, acks, int64(n))
	require.LessOrEqual(t, acks, int64(n)+8, "one acknowledgement call per message: %s", res)
	require.EqualValues(t, 0, res.commands["xclaim"], "nothing is idle, so nothing is claimed")
	require.EqualValues(t, 0, res.commands["xautoclaim"])
	require.LessOrEqual(t, res.commands["xpending"], int64(n)/10, "the search for idle messages must not run before every message")
	require.LessOrEqual(t, res.total(), int64(n)*2+int64(n)/10+10)
}

func TestHotPath_CommandsPerMessage(t *testing.T) { eachRedis(t, redisHotPathCommands) }

// BenchmarkHotPath reports the Redis commands and the time per message, for a single node and a cluster:
//
//	go test -run '^$' -bench HotPath -benchtime 300x
func BenchmarkHotPath(b *testing.B) {
	for _, mode := range []string{modeSingle, modeCluster} {
		b.Run(mode, func(b *testing.B) {
			if _, ok := redisConfigFor(mode); !ok {
				b.Skipf("%s Redis is not configured", mode)
			}
			b.StopTimer()
			var total int64
			var elapsed time.Duration
			var msgs int
			for range b.N {
				res := measureHotPath(b, mode, 4, 300)
				total += res.total()
				elapsed += res.elapsed
				msgs += 300
			}
			b.ReportMetric(float64(total)/float64(msgs), "redis-cmds/msg")
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(msgs), "ns/msg")
		})
	}
}
