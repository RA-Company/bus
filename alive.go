package bus

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
)

// Which consumers are alive?
//
// A consumer of a Redis stream group is only a record (a name and its pending list): it stays in the group after
// the process that used it has gone, and the idle time Redis reports grows for a live worker that is busy with a
// long receiver. So the group cannot tell live workers from dead ones, and with a new consumer name per start
// it would show hundreds of "consumers" for a service that runs a few workers.
//
// The bus therefore keeps its own registry. While Start runs, every instance renews its workers in a sorted set
// {<tag>}:alive:<group> every heartbeatEvery (one Redis call per instance, none per message); the score is the
// time of the last renewal by the Redis server's clock, so servers with different clocks agree. A worker that
// has not been renewed for aliveTTL is dead. A graceful stop removes the instance's workers at once.
//
// The same loop removes consumer records of dead workers from the group (see purgeConsumers), so Redis does not
// keep them forever either.
const (
	defaultHeartbeatEvery = 5 * time.Second
	defaultAliveTTL       = 15 * time.Second
	// aliveKeep is how long a dead worker stays in the registry before its entry is removed.
	aliveKeep = 10 * time.Minute
	// purgeEvery is how often a heartbeat loop looks for dead consumer records.
	purgeEvery = 10 * time.Minute
	// defaultPurgeIdle is how long a consumer must have been silent, with nothing pending, to be deleted. A live
	// worker reads at least once a second, so this is far above any pause of a live one.
	defaultPurgeIdle = 10 * time.Minute
)

// heartbeatScript renews workers and drops long-dead ones.
//
//	KEYS[1] registry of the group
//	ARGV[1] how long dead entries are kept (ms), ARGV[2] expiry of the whole key (ms), ARGV[3...] "<instance>|<worker>"
var heartbeatScript = re.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
for i = 3, #ARGV do
  redis.call('ZADD', KEYS[1], now, ARGV[i])
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - tonumber(ARGV[1]))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return now
`)

// aliveScript reads the registry together with the server's clock.
var aliveScript = re.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
return {tostring(now), redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')}
`)

func (b *Bus) aliveKey(group string) string { return "{" + b.tag + "}:alive:" + group }

// newInstanceName names this Start for the registry: host, process and a random suffix, so that two starts in
// one process or two containers with the same host name differ.
func newInstanceName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/" + strconv.Itoa(os.Getpid()) + "/" + uuid.NewString()[:8]
}

// heartbeat renews the workers of this instance until they have all finished, then removes them from the
// registry. It never touches deliveries: a failure is only logged. done is closed when it has finished.
func (b *Bus) heartbeat(ctx context.Context, instance string, workers []string, done chan<- struct{}) {
	defer close(done)
	if b.tag == "" {
		return
	}
	members := make([]any, 0, len(workers)+2)
	members = append(members, aliveKeep.Milliseconds(), (time.Hour).Milliseconds())
	for _, w := range workers {
		members = append(members, instance+"|"+w)
	}
	key := b.aliveKey(b.group)

	beat := func() {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
		defer cancel()
		if err := heartbeatScript.Run(callCtx, b.redis.Client(), []string{key}, members...).Err(); err != nil {
			b.Warnf(ctx, "Failed to renew the heartbeat of workers in group %q: %v", b.group, err)
		}
	}

	beat()
	b.purgeConsumers(ctx)
	ticker := time.NewTicker(b.heartbeatEvery)
	defer ticker.Stop()
	purge := time.NewTicker(purgeEvery)
	defer purge.Stop()
	for {
		select {
		case <-ticker.C:
			beat()
		case <-purge.C:
			b.purgeConsumers(ctx)
		case <-b.allDone: // every worker has returned
			b.leave(ctx, key, members[2:])
			return
		case <-b.forced: // the bus has given up on the workers that are left
			b.leave(ctx, key, members[2:])
			return
		}
	}
}

// leave removes the instance's workers from the registry.
func (b *Bus) leave(ctx context.Context, key string, members []any) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if len(members) == 0 {
		return
	}
	if err := b.redis.Client().ZRem(callCtx, key, members...).Err(); err != nil {
		b.Warnf(ctx, "Failed to remove workers from the registry of group %q: %v", b.group, err)
	}
}

// purgeConsumers deletes consumer records of the group that belong to dead workers: not in the registry, nothing
// pending and silent for purgeIdle (10 minutes). A consumer with pending messages is never deleted, because deleting it
// drops its pending list and those messages would be lost; their owner's name is irrelevant to redelivery.
func (b *Bus) purgeConsumers(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	consumers, err := b.redis.Client().XInfoConsumers(callCtx, b.stream, b.group).Result()
	if err != nil {
		b.Warnf(ctx, "Failed to list consumers of group %q: %v", b.group, err)
		return
	}
	live, _, err := b.readAlive(callCtx, b.group)
	if err != nil {
		b.Warnf(ctx, "Failed to read the registry of group %q: %v", b.group, err)
		return
	}
	alive := map[string]bool{}
	for _, in := range live {
		for _, w := range in.workerNames {
			alive[w] = true
		}
	}
	removed := 0
	for _, c := range consumers {
		if c.Pending > 0 || c.Idle < b.purgeIdle || alive[c.Name] {
			continue
		}
		if err := b.redis.XGroupDelConsumer(callCtx, b.stream, b.group, c.Name); err != nil {
			b.Warnf(ctx, "Failed to delete dead consumer %q of group %q: %v", c.Name, b.group, err)
			continue
		}
		removed++
	}
	if removed > 0 {
		b.Infof(ctx, "Deleted %d dead consumer(s) of group %q", removed, b.group)
	}
}

// Instance is one running Start of a service: a process with its workers.
type Instance struct {
	// Name is "host/pid/suffix" of the process.
	Name string `json:"name"`
	// Workers is the number of its live workers (consumers).
	Workers int `json:"workers"`
	// LastSeen is how long ago the instance last renewed its heartbeat.
	LastSeen time.Duration `json:"last_seen"`

	workerNames []string
}

// readAlive reads the registry of a group and returns the live instances (renewed within aliveTTL) and the
// number of recently dead workers still in the registry.
func (b *Bus) readAlive(ctx context.Context, group string) (live []Instance, dead int, err error) {
	res, err := aliveScript.Run(ctx, b.redis.Client(), []string{b.aliveKey(group)}).Slice()
	if err != nil {
		return nil, 0, err
	}
	nowStr, _ := res[0].(string)
	now, _ := strconv.ParseInt(nowStr, 10, 64)
	flat, _ := res[1].([]any)

	byInstance := map[string]*Instance{}
	for i := 0; i+1 < len(flat); i += 2 {
		member, _ := flat[i].(string)
		scoreStr, _ := flat[i+1].(string)
		score, _ := strconv.ParseFloat(scoreStr, 64)
		age := time.Duration(now-int64(score)) * time.Millisecond
		if age > b.aliveTTL {
			dead++
			continue
		}
		name, worker := member, ""
		if j := strings.LastIndexByte(member, '|'); j >= 0 {
			name, worker = member[:j], member[j+1:]
		}
		in := byInstance[name]
		if in == nil {
			in = &Instance{Name: name, LastSeen: age}
			byInstance[name] = in
		}
		in.Workers++
		in.workerNames = append(in.workerNames, worker)
		in.LastSeen = min(in.LastSeen, max(age, 0))
	}
	for _, in := range byInstance {
		live = append(live, *in)
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Name < live[j].Name })
	return live, dead, nil
}
