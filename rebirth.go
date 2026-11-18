package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ra-company/database/redis"
	re "github.com/redis/go-redis/v9"
)

// Where does a consumer group start when it has to be created again?
//
// A group lives inside the stream key. When Redis answers NOGROUP (or Init finds no group), the position of the
// group is gone, and Redis alone cannot tell what happened:
//
//   - only the group was destroyed: the stream still holds its entries, which were very likely processed
//     already, so the group must start at the end ("$") not to run them again;
//   - the stream key was deleted (or expired) and created anew by the next XADD: every entry in it is new,
//     so the group must start at the beginning ("0") not to lose them.
//
// So whoever creates the stream key through this library leaves a mark: the key {<tag>}:reborn, kept for
// StreamRebirthTTL. A producer sets it in the same script call as the XADD that creates the stream, a consumer
// in the same script call as the XGROUP CREATE MKSTREAM that does. A group created while the mark exists starts
// at "0", otherwise at its usual position. The mark is shared by all groups of the stream and is not removed
// when it is read: every group (service) that notices the loss within StreamRebirthTTL finds it.
//
// Limits: a stream created by a writer outside this library has no mark, and a group that notices the loss later
// than StreamRebirthTTL (for example a service that was down longer) starts at its usual position.

// defaultStreamRebirthTTL is how long the mark of a recreated stream is kept when BusConfiguration.StreamRebirthTTL is
// not set.
const defaultStreamRebirthTTL = time.Hour

func (b *Bus) rebornKey() string { return "{" + b.tag + "}:reborn" }

// createGroupScript creates a consumer group, and the stream with its mark if the stream does not exist.
//
//	KEYS[1] stream, KEYS[2] mark of a recreated stream (same hash slot)
//	ARGV[1] group, ARGV[2] position when the stream is not new, ARGV[3] mark TTL in ms ("0" = no mark)
//
// It returns {1, position} when the group was created and {0, position} when it existed already.
var createGroupScript = re.NewScript(`
local start = ARGV[2]
if redis.call('EXISTS', KEYS[1]) == 0 then
  start = '0'
  if ARGV[3] ~= '0' then
    redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
  end
elseif redis.call('EXISTS', KEYS[2]) == 1 then
  start = '0'
end
local res = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], start, 'MKSTREAM')
if type(res) == 'table' and res.err then
  if string.find(res.err, 'BUSYGROUP', 1, true) then
    return {0, start}
  end
  return redis.error_reply(res.err)
end
return {1, start}
`)

// createGroup creates the consumer group unless it exists. start is the position of a group on a stream that was
// not recreated recently: "$" for the bus' own group (Init, NOGROUP), "0" for EnsureGroups. On a new or
// recently recreated stream the group starts at "0" (see the top of this file). It returns the position used
// and whether the group was created.
func (b *Bus) createGroup(ctx context.Context, group, start string) (string, bool, error) {
	if b.tag == "" {
		// No key can share the stream's slot, so there is no mark: a new stream still starts at "0". The check and
		// the creation are two calls here: a producer that creates the stream between them makes the group start
		// at "$" and skip its first messages. This path is only for stream names whose braces form no hash tag.
		n, err := b.redis.Client().Exists(ctx, b.stream).Result()
		if err != nil {
			return "", false, err
		}
		if n == 0 {
			start = "0"
		}
		err = b.redis.XGroupCreateMkStream(ctx, b.stream, group, start)
		if errors.Is(err, redis.ErrorGroupAlreadyExists) {
			return start, false, nil
		}
		return start, err == nil, err
	}

	ttl := int64(0)
	if b.rebirthTTL > 0 {
		ttl = max(b.rebirthTTL.Milliseconds(), 1)
	}
	res, err := createGroupScript.Run(ctx, b.redis.Client(), []string{b.stream, b.rebornKey()}, group, start, ttl).Slice()
	if err != nil {
		return "", false, fmt.Errorf("create group %q of stream %q: %w", group, b.stream, err)
	}
	created, _ := res[0].(int64)
	used, _ := res[1].(string)
	return used, created == 1, nil
}

// recreateGroup creates the bus' group again after Redis reported NOGROUP, at the position chosen by createGroup.
func (b *Bus) recreateGroup(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	start, created, err := b.createGroup(callCtx, b.group, "$")
	switch {
	case err != nil:
		b.Errorf(ctx, "Failed to create group %q of stream %q again: %v", b.group, b.stream, err)
	case created && start == "0":
		b.Warnf(ctx, "Group %q of stream %q was missing; the stream is new or was recreated, so the group starts at its beginning", b.group, b.stream)
	case created:
		b.Warnf(ctx, "Group %q of stream %q was missing and has been created again at the end of the stream; entries already in it are not delivered", b.group, b.stream)
	}
}

// isNoGroup reports whether err is the NOGROUP error Redis returns when the stream key or the group is gone.
func isNoGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOGROUP")
}
