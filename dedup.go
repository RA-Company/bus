package bus

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// defaultDedupTTL is how long a dedup key is remembered when BusConfiguration.DedupTTL is not set.
const defaultDedupTTL = 10 * time.Minute

// ErrorDedupStreamName is returned by AddMessage with a dedup key when the stream name contains braces
// in a way that does not form a Redis Cluster hash tag, so a key in the same slot cannot be built.
var ErrorDedupStreamName = fmt.Errorf("stream name with braces cannot be used with deduplication")

// dedupKeyName builds the Redis key of a dedup entry. The part in braces is the hash tag of the stream
// (its own tag, or its whole name), so in a cluster the key lives in the stream's slot and one script
// can touch both. The producer's key is hashed together with the receiver: the key space is per receiver,
// and its length and characters do not matter.
func (b *Bus) dedupKeyName(receiver, key string) (string, error) {
	tag, err := b.streamTag()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(receiver + "\x00" + key))
	return "{" + tag + "}:dedup:" + hex.EncodeToString(sum[:]), nil
}
