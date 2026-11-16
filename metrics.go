package bus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ErrorLagUnavailable is returned by Lag when Redis cannot report the lag of the group:
// either the server is older than 7.0 (XINFO GROUPS has no "lag" field there) or Redis
// answered NULL because the lag cannot be computed (for example after entries were deleted).
var ErrorLagUnavailable = errors.New("group lag is not available")

// Stats is a snapshot of the in-process counters of a Bus. The counters live in memory and
// start from zero when the process restarts; keep history and graphs in the application's
// monitoring, fed through MetricsHook.
type Stats struct {
	// Processed counts messages whose receiver succeeded and which were acknowledged.
	Processed int64 `json:"processed"`
	// Retried counts executions of a message that was delivered before (deliveries > 1).
	Retried int64 `json:"retried"`
	// DeadLettered counts messages moved to the dead-letter stream and acknowledged.
	DeadLettered int64 `json:"dead_lettered"`
	// Failed counts failed executions: the receiver failed to start, returned an error or panicked.
	// A message that is retried and fails again is counted every time.
	Failed int64 `json:"failed"`
	// AckErrors counts failed XACK calls (the message stays pending and will be redelivered).
	AckErrors int64 `json:"ack_errors"`
	// InFlight is the number of messages being executed right now.
	InFlight int64 `json:"in_flight"`
}

// MetricsHook lets an application export bus events to its monitoring (Prometheus, StatsD, ...)
// without the library depending on any of them. Methods are called synchronously from worker
// goroutines, possibly concurrently, so they must be fast and safe for concurrent use.
// A panic in a hook is recovered and logged. Embed NopMetricsHook to implement only some methods.
type MetricsHook interface {
	// Processed is called when a message was executed successfully and acknowledged.
	Processed(receiver string, took time.Duration)
	// Retried is called before an execution of a message that was delivered before.
	Retried(receiver string)
	// Failed is called after an execution that failed to start, returned an error or panicked.
	Failed(receiver string, took time.Duration)
	// DeadLettered is called after a message was moved to the dead-letter stream and acknowledged.
	// reason is one of DeadReasonRetriesExhausted or DeadReasonPermanent.
	DeadLettered(receiver, reason string)
	// AckError is called when acknowledging a message failed.
	AckError(receiver string)
}

// NopMetricsHook implements MetricsHook with empty methods.
type NopMetricsHook struct{}

func (NopMetricsHook) Processed(string, time.Duration) {}
func (NopMetricsHook) Retried(string)                  {}
func (NopMetricsHook) Failed(string, time.Duration)    {}
func (NopMetricsHook) DeadLettered(string, string)     {}
func (NopMetricsHook) AckError(string)                 {}

type hookBox struct{ hook MetricsHook }

type counters struct {
	processed, retried, deadLettered, failed, ackErrors atomic.Int64
}

// SetMetricsHook installs a hook that receives bus events. Pass nil to remove it.
// It is safe to call at any time, but normally it is set before Start.
func (b *Bus) SetMetricsHook(hook MetricsHook) {
	if hook == nil {
		b.hook.Store(nil)
		return
	}
	b.hook.Store(&hookBox{hook: hook})
}

// Stats returns a snapshot of the in-process counters. See Stats for what each counter means.
func (b *Bus) Stats() Stats {
	return Stats{
		Processed:    b.counters.processed.Load(),
		Retried:      b.counters.retried.Load(),
		DeadLettered: b.counters.deadLettered.Load(),
		Failed:       b.counters.failed.Load(),
		AckErrors:    b.counters.ackErrors.Load(),
		InFlight:     b.inFlight.Load(),
	}
}

// emit runs fn with the installed hook, if any, shielding the bus from panics in the hook.
func (b *Bus) emit(ctx context.Context, fn func(h MetricsHook)) {
	box := b.hook.Load()
	if box == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			b.Errorf(ctx, "Metrics hook panic recovered: %v", r)
		}
	}()
	fn(box.hook)
}

// Lag returns the number of messages in the stream that have not been delivered to the
// consumer group yet (Redis XINFO GROUPS "lag"). Messages that were delivered but not
// acknowledged are not counted; they are pending and are retried.
//
// It needs Redis 7.0 or newer. On older servers, and when Redis cannot compute the lag, Lag
// returns -1 and ErrorLagUnavailable.
func (b *Bus) Lag(ctx context.Context) (int64, error) {
	if major, ok := b.serverMajorVersion(ctx); ok && major < 7 {
		return -1, fmt.Errorf("%w: Redis %d.x does not report it, 7.0 or newer is required", ErrorLagUnavailable, major)
	}

	groups, err := b.redis.Client().XInfoGroups(ctx, b.stream).Result()
	if err != nil {
		return -1, err
	}
	for _, g := range groups {
		if g.Name != b.group {
			continue
		}
		if g.Lag < 0 {
			return -1, fmt.Errorf("%w: Redis cannot compute it for this stream", ErrorLagUnavailable)
		}
		return g.Lag, nil
	}
	return -1, fmt.Errorf("group %q of stream %q not found", b.group, b.stream)
}

// serverMajorVersion reads the major version of the Redis server (a random node in a cluster).
func (b *Bus) serverMajorVersion(ctx context.Context) (int, bool) {
	info, err := b.redis.Client().Info(ctx, "server").Result()
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			major, _, _ := strings.Cut(v, ".")
			n, err := strconv.Atoi(major)
			return n, err == nil
		}
	}
	return 0, false
}
