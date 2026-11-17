package bus

import (
	"context"
	"fmt"
	"time"

	re "github.com/redis/go-redis/v9"
)

// Reasons stored in the "dead_reason" field of a dead-letter entry.
const (
	// DeadReasonRetriesExhausted: the message was delivered more than the allowed number of times.
	DeadReasonRetriesExhausted = "retries_exhausted"
	// DeadReasonPermanent: the receiver returned an error marked with Permanent.
	DeadReasonPermanent = "permanent"
)

const (
	defaultDeadLetterSize      = 10000
	defaultDeadLetterRetention = 30 * 24 * time.Hour
)

// deadLetter copies the message to the dead-letter stream. The entry keeps every field of the
// original message (receiver, id, payload, time) unchanged and adds:
//
//	dead_reason      DeadReasonRetriesExhausted or DeadReasonPermanent
//	dead_error       text of the error, when there is one
//	dead_stream      stream the message came from
//	dead_group       consumer group that gave up on it
//	dead_message_id  stream entry id in the original stream
//	dead_deliveries  how many times Redis delivered it
//	dead_at          when it was moved (RFC 3339 with nanoseconds, UTC)
//
// Old entries are trimmed on every write: by age (DeadLetterRetention, XADD MINID ~) and by
// count (DeadLetterSize, XTRIM MAXLEN). A failure of the trimming is only logged.
func (b *Bus) deadLetter(ctx context.Context, msg re.XMessage, reason, errText string, deliveries int64) error {
	values := make(map[string]any, len(msg.Values)+7)
	for k, v := range msg.Values {
		values[k] = v
	}
	values["dead_reason"] = reason
	values["dead_error"] = errText
	values["dead_stream"] = b.stream
	values["dead_group"] = b.group
	values["dead_message_id"] = msg.ID
	values["dead_deliveries"] = deliveries
	values["dead_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	// Moving a message must finish even when the bus is being stopped.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	minID := fmt.Sprintf("%d-0", time.Now().Add(-b.deadRetention).UnixMilli())
	if _, err := b.redis.XAdd(ctx, &re.XAddArgs{
		Stream: b.deadStream,
		Values: values,
		MinID:  minID,
		Approx: true,
	}); err != nil {
		b.Errorf(ctx, "Failed to move message %q to dead-letter stream %q, it stays pending: %v", msg.ID, b.deadStream, err)
		return fmt.Errorf("dead-letter message %s: %w", msg.ID, err)
	}

	if b.deadSize > 0 {
		if err := b.redis.Client().XTrimMaxLen(ctx, b.deadStream, b.deadSize).Err(); err != nil {
			b.Warnf(ctx, "Failed to trim dead-letter stream %q: %v", b.deadStream, err)
		}
	}
	return nil
}

// discard moves the message to the dead-letter stream and then acknowledges it. The order is
// deliberate: if the process dies in between, the message is delivered again and ends up in the
// dead-letter stream twice (at-least-once) instead of being lost. If moving fails, the message is
// not acknowledged and stays pending.
//
// kinds are counters to bump together with the dead-letter one (the failed execution of a permanent error).
func (b *Bus) discard(ctx context.Context, msg re.XMessage, reason, errText string, deliveries int64, kinds ...string) error {
	if err := b.deadLetter(ctx, msg, reason, errText, deliveries); err != nil {
		return err
	}
	if err := b.ack(ctx, msg, append(kinds, kindDead)...); err != nil {
		return err
	}
	b.counters.deadLettered.Add(1)
	return nil
}
