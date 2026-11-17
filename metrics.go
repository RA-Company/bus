package bus

import "sync/atomic"

// Stats is a snapshot of the in-process counters of a Bus. The counters live in memory and
// start from zero when the process restarts. Counters and Snapshot give the history and the state
// of the whole stream, kept in Redis.
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
	// Deduplicated counts publications skipped as duplicates (AddMessage with a dedup key).
	Deduplicated int64 `json:"deduplicated"`
	// TimedOut counts executions that ran longer than HandlerTimeout, counted when the time is up
	// (they are counted in Failed as well, when the receiver returns).
	TimedOut int64 `json:"timed_out"`
	// Stuck is the number of receivers running past their HandlerTimeout right now: they ignore their
	// context and hold their workers. A value that does not go back to zero is a receiver without a timeout
	// of its own; when it reaches WorkersCount, nothing is processed.
	Stuck int64 `json:"stuck"`
	// InFlight is the number of messages being executed right now.
	InFlight int64 `json:"in_flight"`
}

type counters struct {
	processed, retried, deadLettered, failed, ackErrors, deduplicated, timedOut atomic.Int64
}

// Stats returns a snapshot of the in-process counters. See Stats for what each counter means.
func (b *Bus) Stats() Stats {
	return Stats{
		Processed:    b.counters.processed.Load(),
		Retried:      b.counters.retried.Load(),
		DeadLettered: b.counters.deadLettered.Load(),
		Failed:       b.counters.failed.Load(),
		AckErrors:    b.counters.ackErrors.Load(),
		Deduplicated: b.counters.deduplicated.Load(),
		TimedOut:     b.counters.timedOut.Load(),
		Stuck:        b.stuck.Load(),
		InFlight:     b.inFlight.Load(),
	}
}
