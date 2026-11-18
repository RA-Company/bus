package bus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	re "github.com/redis/go-redis/v9"
)

// Counters of the bus live in Redis, like the statistics of a queue in asynq: they are shared by all
// instances of a service, survive restarts and can be read by any process, for example a monitoring
// service, without touching the workers. They are kept per day (UTC) in hashes with an expiry.
//
//	{<tag>}:stats:g:<group>:<YYYYMMDD>   what the consumer group did: received, processed, failed, ...
//	{<tag>}:stats:p:<YYYYMMDD>           what was published to the stream: published, deduplicated
//
// <tag> is the stream's hash tag (its own, or its whole name), so in a cluster the counters live in the
// slot of the stream and are updated in the same script call as the stream itself. A hash has a field
// "<metric>" for the total and "<metric>:<receiver>" for every receiver.
//
// Counters are written together with XACK (consumer) and XADD (producer), so the number of Redis round trips
// per message does not change. A failure to update a counter never fails the delivery or the publication.
const (
	kindReceived     = "received"     // handling of a delivered message finished, whatever the outcome, except a duplicate
	kindProcessed    = "processed"    // receiver succeeded, message acknowledged
	kindFailed       = "failed"       // execution failed (error, panic, failed to start)
	kindRetried      = "retried"      // execution of a message that had been delivered before
	kindDead         = "dead"         // moved to the dead-letter stream
	kindUnregistered = "unregistered" // no receiver for it in this service, acknowledged and ignored
	kindInvalid      = "invalid"      // no valid receiver name in the message, acknowledged and dropped
	kindPublished    = "published"
	kindDeduplicated = "deduplicated"
	kindDuplicate    = "duplicate" // the run finished, but the message had been acknowledged by another run already
)

const (
	defaultStatsRetention = 30 * 24 * time.Hour
	// statNameLimit bounds the number of distinct receiver names that get their own counters in this
	// process besides the registered ones; the rest is counted under statNameOther. Names come from the
	// stream, so without a bound a producer's bug could create endless fields.
	statNameLimit = 500
	statNameOther = "(other)"
)

// ErrorStatsUnavailable is returned by Counters when the stream name does not allow keeping counters
// in the slot of the stream (braces that do not form a hash tag).
var ErrorStatsUnavailable = errors.New("counters are not available for this stream name")

// outcomeScript acknowledges a message (optional) and bumps counters in one step.
//
//	KEYS[1] stream, KEYS[2] counters hash of the day (same hash slot)
//	ARGV[1] "1" to XACK, ARGV[2] group, ARGV[3] entry id, ARGV[4] counters TTL in ms,
//	ARGV[5] receiver name for the per-receiver fields ("" = totals only), ARGV[6...] kinds
//
// Every kind is counted as a total and per receiver, together with "received". If the acknowledgement finds the
// message acknowledged already (another run of it finished first: a late success after HandlerTimeout, or an
// at-least-once duplicate), the outcome is not counted again: only "duplicate" is.
//
// The acknowledgement goes first and the counters are updated with pcall: a script is not rolled back, and a
// problem with a counter must neither leave the message unacknowledged nor fail the call.
var outcomeScript = re.NewScript(`
local acked = 0
if ARGV[1] == '1' then
  acked = redis.call('XACK', KEYS[1], ARGV[2], ARGV[3])
end
local function bump(kind)
  redis.pcall('HINCRBY', KEYS[2], kind, 1)
  if ARGV[5] ~= '' then
    redis.pcall('HINCRBY', KEYS[2], kind .. ':' .. ARGV[5], 1)
  end
end
if ARGV[1] == '1' and acked == 0 then
  bump('duplicate')
else
  bump('received')
  for i = 6, #ARGV do
    bump(ARGV[i])
  end
end
redis.pcall('PEXPIRE', KEYS[2], ARGV[4], 'NX')
return acked
`)

// publishScript adds a message to the stream, optionally unless a deduplication key is remembered, and counts it.
// If this XADD creates the stream (it did not exist), the stream is marked as recreated (see rebirth.go).
//
//	KEYS[1] stream, KEYS[2] published counters of the day, KEYS[3] mark of a recreated stream,
//	KEYS[4] dedup key (only with a deduplication key); all in the same hash slot
//	ARGV    MAXLEN (approximate), counters TTL in ms ("0" = no counters), message id, receiver, payload, time,
//	        receiver name for the counters, dedup TTL in ms, mark TTL in ms ("0" = no mark)
//
// It returns {1, id} when the message was added and {0, id of the first message} for a duplicate.
// XADD goes before SET: a script is not rolled back on an error, and a failed XADD must not leave a key that
// would suppress the next attempt. Counters are updated with pcall, they never fail the call.
var publishScript = re.NewScript(`
local function count(name)
  if ARGV[2] ~= '0' then
    redis.pcall('HINCRBY', KEYS[2], name, 1)
    if ARGV[7] ~= '' then
      redis.pcall('HINCRBY', KEYS[2], name .. ':' .. ARGV[7], 1)
    end
    redis.pcall('PEXPIRE', KEYS[2], ARGV[2], 'NX')
  end
end

if KEYS[4] then
  local first = redis.call('GET', KEYS[4])
  if first then
    count('deduplicated')
    return {0, first}
  end
end
local new = ARGV[9] ~= '0' and redis.call('EXISTS', KEYS[1]) == 0
redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*',
  'receiver', ARGV[4], 'id', ARGV[3], 'payload', ARGV[5], 'time', ARGV[6])
if new then
  redis.call('SET', KEYS[3], '1', 'PX', ARGV[9])
end
if KEYS[4] then
  redis.call('SET', KEYS[4], ARGV[3], 'PX', ARGV[8])
end
count('published')
return {1, ARGV[3]}
`)

// publish runs publishScript. It returns the id of the message that is in the stream (the first one for a
// duplicate) and whether this call added one.
func (b *Bus) publish(ctx context.Context, receiver, payload, id, stamp, dedupKey string) (string, bool, error) {
	keys := []string{b.stream, b.publishedStatsKey(statsDate(time.Now())), b.rebornKey()}
	dedupTTL := int64(0)
	if dedupKey != "" {
		key, err := b.dedupKeyName(receiver, dedupKey)
		if err != nil {
			return "", false, err
		}
		keys = append(keys, key)
		dedupTTL = max(b.dedupTTL.Milliseconds(), 1)
	}
	statsTTL := int64(0)
	if b.statsOn() {
		statsTTL = b.statsTTL.Milliseconds()
	}
	rebirthTTL := int64(0)
	if b.rebirthTTL > 0 {
		rebirthTTL = max(b.rebirthTTL.Milliseconds(), 1)
	}

	res, err := publishScript.Run(ctx, b.redis.Client(), keys,
		b.streamSize, statsTTL, id, receiver, payload, stamp, b.statName(receiver), dedupTTL, rebirthTTL).Slice()
	if err != nil {
		return "", false, err
	}
	added, _ := res[0].(int64)
	first, _ := res[1].(string)
	return first, added == 1, nil
}

// streamTag returns the part of the key that decides its Redis Cluster slot for the stream: the stream's own
// hash tag, or its whole name. Names with braces that do not form a tag cannot be put in braces.
func (b *Bus) streamTag() (string, error) {
	tag := b.stream
	if i := strings.IndexByte(b.stream, '{'); i >= 0 {
		j := strings.IndexByte(b.stream[i+1:], '}')
		if j <= 0 { // no closing brace or an empty tag: the whole name is hashed, but it cannot be put in braces
			return "", ErrorDedupStreamName
		}
		tag = b.stream[i+1 : i+1+j]
	} else if strings.ContainsRune(b.stream, '}') {
		return "", ErrorDedupStreamName
	}
	return tag, nil
}

func statsDate(t time.Time) string { return t.UTC().Format("20060102") }

func (b *Bus) groupStatsKey(group, date string) string {
	return "{" + b.tag + "}:stats:g:" + group + ":" + date
}

func (b *Bus) publishedStatsKey(date string) string {
	return "{" + b.tag + "}:stats:p:" + date
}

// statsOn tells whether counters are written to Redis.
func (b *Bus) statsOn() bool { return b.statsTTL > 0 && b.tag != "" }

// statName returns the receiver name to use in a counter field, "" for no per-receiver counter.
func (b *Bus) statName(receiver string) string {
	if receiver == "" {
		return ""
	}
	b.mu.RLock()
	_, registered := b.receivers[receiver]
	b.mu.RUnlock()
	if registered {
		return receiver
	}

	b.namesMu.Lock()
	defer b.namesMu.Unlock()
	if _, ok := b.names[receiver]; ok {
		return receiver
	}
	if len(b.names) >= statNameLimit {
		return statNameOther
	}
	if b.names == nil {
		b.names = make(map[string]struct{})
	}
	b.names[receiver] = struct{}{}
	return receiver
}

// runOutcome acknowledges the message (when ack is set) and bumps the counters in one script call. It returns
// whether the acknowledgement removed the message from the pending list (false: acknowledged before).
func (b *Bus) runOutcome(ctx context.Context, msg re.XMessage, ack bool, kinds []string) (bool, error) {
	receiver, _ := msg.Values["receiver"].(string)
	flag := "0"
	if ack {
		flag = "1"
	}
	args := make([]any, 0, 5+len(kinds))
	args = append(args, flag, b.group, msg.ID, b.statsTTL.Milliseconds(), b.statName(receiver))
	for _, kind := range kinds {
		args = append(args, kind)
	}
	keys := []string{b.stream, b.groupStatsKey(b.group, statsDate(time.Now()))}
	n, err := outcomeScript.Run(ctx, b.redis.Client(), keys, args...).Int64()
	return n == 1, err
}

// recordFailure counts a failed execution. The message is not acknowledged. A failure to write the counter
// is only logged.
func (b *Bus) recordFailure(ctx context.Context, msg re.XMessage, kinds ...string) {
	if !b.statsOn() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if _, err := b.runOutcome(ctx, msg, false, kinds); err != nil {
		b.Warnf(ctx, "Failed to update counters of message %q: %v", msg.ID, err)
	}
}

// retriedIf appends kindRetried when the message has been delivered before.
func retriedIf(deliveries int64, kinds ...string) []string {
	if deliveries > 1 {
		return append(kinds, kindRetried)
	}
	return kinds
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

// Counters is a set of event counts.
type Counters struct {
	Published    int64 `json:"published"`    // messages added to the stream
	Deduplicated int64 `json:"deduplicated"` // publications skipped as duplicates (AddMessage with a dedup key)
	Received     int64 `json:"received"`     // deliveries whose handling finished, whatever the outcome, except Duplicates
	Processed    int64 `json:"processed"`    // receiver succeeded and the message was acknowledged
	Failed       int64 `json:"failed"`       // executions that failed to start, returned an error or panicked
	Retried      int64 `json:"retried"`      // executions of a message that had been delivered before
	DeadLettered int64 `json:"dead_lettered"`
	Unregistered int64 `json:"unregistered"` // messages for receivers this service has not registered
	Invalid      int64 `json:"invalid"`      // messages without a valid receiver name
	// Duplicates are runs whose message had been acknowledged by another run already when this one acknowledged
	// it (a late success after HandlerTimeout, or an at-least-once duplicate); they are not counted in any other
	// field again.
	Duplicates int64 `json:"duplicates"`
}

func (c *Counters) add(metric string, v int64) {
	switch metric {
	case kindPublished:
		c.Published = v
	case kindDeduplicated:
		c.Deduplicated = v
	case kindReceived:
		c.Received = v
	case kindProcessed:
		c.Processed = v
	case kindFailed:
		c.Failed = v
	case kindRetried:
		c.Retried = v
	case kindDead:
		c.DeadLettered = v
	case kindUnregistered:
		c.Unregistered = v
	case kindInvalid:
		c.Invalid = v
	case kindDuplicate:
		c.Duplicates = v
	}
}

// DayCounters are the counters of one day (UTC).
type DayCounters struct {
	Date       string              `json:"date"` // YYYY-MM-DD
	Total      Counters            `json:"total"`
	ByReceiver map[string]Counters `json:"by_receiver"`
}

// Counters returns the counters of the last days (today first) for a consumer group: what the group
// received, processed, failed, moved to the dead-letter stream and ignored for lack of a receiver, plus what
// producers published to the stream (the same for every group). An empty group means the bus' own group.
// days is clamped to 1..366. The counters are stored in Redis for StatsRetention, so they cover all
// instances of a service and survive restarts; a day without events is returned with zeros.
//
// "Received" counts a delivery when its handling has finished, so a message that is being executed now, or
// whose worker died, is counted when it finishes or is redelivered.
func (b *Bus) Counters(ctx context.Context, group string, days int) ([]DayCounters, error) {
	if err := b.ready(); err != nil {
		return nil, err
	}
	if b.tag == "" {
		return nil, ErrorStatsUnavailable
	}
	if group == "" {
		group = b.group
	}
	days = min(max(days, 1), 366)

	now := time.Now().UTC()
	pipe := b.redis.Client().Pipeline()
	groupCmds := make([]*re.MapStringStringCmd, days)
	pubCmds := make([]*re.MapStringStringCmd, days)
	dates := make([]time.Time, days)
	for i := range days {
		dates[i] = now.AddDate(0, 0, -i)
		groupCmds[i] = pipe.HGetAll(ctx, b.groupStatsKey(group, statsDate(dates[i])))
		pubCmds[i] = pipe.HGetAll(ctx, b.publishedStatsKey(statsDate(dates[i])))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("read counters: %w", err)
	}

	out := make([]DayCounters, days)
	for i := range days {
		day := DayCounters{Date: dates[i].Format("2006-01-02"), ByReceiver: map[string]Counters{}}
		for _, hash := range []map[string]string{groupCmds[i].Val(), pubCmds[i].Val()} {
			for field, raw := range hash {
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					continue
				}
				metric, receiver, perReceiver := strings.Cut(field, ":")
				if !perReceiver {
					day.Total.add(metric, v)
					continue
				}
				c := day.ByReceiver[receiver]
				c.add(metric, v)
				day.ByReceiver[receiver] = c
			}
		}
		out[i] = day
	}
	return out, nil
}

// GroupSnapshot is the state of one consumer group of the stream.
type GroupSnapshot struct {
	Name string `json:"name"`
	// Pending is the number of messages delivered to the group and not acknowledged yet.
	Pending int64 `json:"pending"`
	// OldestPending is the age of the oldest pending message (since it was added to the stream), 0 if none.
	OldestPending time.Duration `json:"oldest_pending"`
	// Lag is the number of messages not delivered to the group yet, -1 if Redis cannot tell (before 7.0).
	Lag int64 `json:"lag"`
	// LastDeliveredID is the id of the last stream entry delivered to the group.
	LastDeliveredID string `json:"last_delivered_id"`
	// LiveConsumers is the number of workers of the group that are running right now, summed over all instances
	// on all servers. It comes from the heartbeat registry, not from the consumer records of the stream, which
	// outlive their workers. 0 with a growing Lag means that nobody is consuming. It is also 0 for a group whose
	// services do not run a version of this library with the registry.
	LiveConsumers int `json:"live_consumers"`
	// Instances are the running processes (Start calls) behind LiveConsumers.
	Instances []Instance `json:"instances"`
}

// Snapshot is the state of the stream and of all its consumer groups right now, read from Redis.
type Snapshot struct {
	Stream string `json:"stream"`
	// Size is the number of messages in the stream and SizeLimit the StreamSize of this bus: when Size reaches
	// it, every new message pushes the oldest one out, processed or not.
	Size      int64 `json:"size"`
	SizeLimit int64 `json:"size_limit"`
	// DeadLetterSize is the number of entries in the dead-letter stream.
	DeadLetterSize int64           `json:"dead_letter_size"`
	Groups         []GroupSnapshot `json:"groups"`
}

// Snapshot reads the size of the stream and the dead-letter stream and the state of every consumer group
// of the stream, not only the bus' own one, so one service can watch all consumers. It costs a few Redis
// commands, plus one per group that has pending messages.
func (b *Bus) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := b.ready(); err != nil {
		return Snapshot{}, err
	}
	client := b.redis.Client()
	snap := Snapshot{Stream: b.stream, SizeLimit: b.streamSize}

	var err error
	if snap.Size, err = client.XLen(ctx, b.stream).Result(); err != nil {
		return snap, fmt.Errorf("stream length: %w", err)
	}
	if snap.DeadLetterSize, err = client.XLen(ctx, b.deadStream).Result(); err != nil {
		return snap, fmt.Errorf("dead-letter stream length: %w", err)
	}

	groups, err := client.XInfoGroups(ctx, b.stream).Result()
	if err != nil {
		return snap, fmt.Errorf("stream groups: %w", err)
	}
	now := time.Now()
	for _, g := range groups {
		gs := GroupSnapshot{Name: g.Name, Pending: g.Pending, Lag: g.Lag, LastDeliveredID: g.LastDeliveredID}
		if g.Lag < 0 { // Redis cannot compute it, for example after entries were deleted
			gs.Lag = -1
		}
		live, _, err := b.readAlive(ctx, g.Name)
		if err != nil {
			return snap, fmt.Errorf("live consumers of group %q: %w", g.Name, err)
		}
		gs.Instances = live
		for _, in := range live {
			gs.LiveConsumers += in.Workers
		}
		if g.Pending > 0 {
			summary, err := client.XPending(ctx, b.stream, g.Name).Result()
			if err != nil && !errors.Is(err, re.Nil) {
				return snap, fmt.Errorf("pending of group %q: %w", g.Name, err)
			}
			if summary != nil {
				gs.OldestPending = entryAge(summary.Lower, now)
			}
		}
		snap.Groups = append(snap.Groups, gs)
	}
	return snap, nil
}

// entryAge returns how long ago the stream entry with this id was created (ids start with milliseconds).
func entryAge(id string, now time.Time) time.Duration {
	ms, _, _ := strings.Cut(id, "-")
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return 0
	}
	return max(now.Sub(time.UnixMilli(n)), 0)
}
