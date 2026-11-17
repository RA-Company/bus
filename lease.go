package bus

import (
	"context"
	"time"

	re "github.com/redis/go-redis/v9"
)

// Leases: a message that is being executed must not be redelivered to another worker while its own worker is
// alive, however long the receiver takes.
//
// Redis considers a pending message abandoned when it has been idle (not delivered, not claimed) for RetryIdleTime.
// A receiver that runs for 59 seconds with RetryIdleTime = 60 s would be taken over a second later, and one that runs
// longer would run twice at once. So the bus renews the lease: one goroutine per Bus (none per message) looks at the
// messages that are being executed, and for those that have been running for leaseEvery (a third of RetryIdleTime)
// since the last renewal it resets their idle time with XCLAIM ... JUSTID, one script call for all of them. JUSTID
// keeps the delivery counter, so a renewal is not a retry. If the process dies, nobody renews, and the message is
// redelivered RetryIdleTime later, which is what RetryIdleTime now means: how long a worker may be silent.
//
// A message that is done within leaseEvery is never renewed, so the hot path pays only for a map insert and delete.
// The renewal stops when HandlerTimeout is over (a receiver that does not obey its context must not hold the message
// for ever) and when the bus gives up on the workers.

// leaseScript renews the leases of the messages that still belong to the workers that execute them.
//
//	KEYS[1] stream
//	ARGV[1] group, then pairs: ARGV[2k] entry id, ARGV[2k+1] worker (consumer) name
//
// An entry that is no longer pending, or is pending for another consumer (it was taken over meanwhile), is not
// touched, so a renewal never takes a message back. The positions (from 0) of such pairs are returned.
var leaseScript = re.NewScript(`
local lost = {}
for i = 2, #ARGV, 2 do
  local id, who = ARGV[i], ARGV[i + 1]
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], id, id, 1)
  if p[1] and p[1][2] == who then
    redis.call('XCLAIM', KEYS[1], ARGV[1], who, 0, id, 'JUSTID')
  else
    lost[#lost + 1] = (i - 2) / 2
  end
end
return lost
`)

// workerKey is the context key under which a worker passes its consumer name to handle.
type workerKey struct{}

// leaseKey identifies one run: the same message can be run by two workers one after the other (the first
// one past its HandlerTimeout, the second after the redelivery), and they must not end each other's lease.
type leaseKey struct{ id, worker string }

type lease struct {
	renewed time.Time // the start of the run, or the last successful renewal
}

func workerOf(ctx context.Context) string {
	worker, _ := ctx.Value(workerKey{}).(string)
	return worker
}

// leaseStart registers a message that is about to be executed by the worker named in ctx. Without a worker
// (a message handled directly, not by Start) there is nothing to renew. The returned function ends the lease.
func (b *Bus) leaseStart(ctx context.Context, id string) (end func()) {
	worker := workerOf(ctx)
	if worker == "" {
		return func() {}
	}
	b.leaseMu.Lock()
	if b.leases == nil {
		b.leases = make(map[leaseKey]lease)
	}
	b.leases[leaseKey{id, worker}] = lease{renewed: time.Now()}
	b.leaseMu.Unlock()
	return func() { b.leaseEnd(worker, id) }
}

// leaseEnd stops renewing the run of the message by the worker. It is safe to call several times.
func (b *Bus) leaseEnd(worker, id string) {
	b.leaseMu.Lock()
	delete(b.leases, leaseKey{id, worker})
	b.leaseMu.Unlock()
}

// renewLeases renews due leases until every worker has returned or the bus has given up on them.
func (b *Bus) renewLeases(done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(max(b.leaseEvery/2, 10*time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.renewDue()
		case <-b.allDone:
			return
		case <-b.forced:
			return
		}
	}
}

// renewDue renews, in one script call, the leases that have not been renewed for leaseEvery.
func (b *Bus) renewDue() {
	now := time.Now()
	var args []any
	var keys []leaseKey
	b.leaseMu.Lock()
	for k, l := range b.leases {
		if now.Sub(l.renewed) >= b.leaseEvery {
			keys = append(keys, k)
			args = append(args, k.id, k.worker)
		}
	}
	b.leaseMu.Unlock()
	if len(keys) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	lost, err := leaseScript.Run(ctx, b.redis.Client(), []string{b.stream}, append([]any{b.group}, args...)...).Int64Slice()
	if err != nil {
		// The leases are tried again at the next tick; if Redis stays unreachable they expire after
		// RetryIdleTime, as they would if the process had died.
		b.Warnf(ctx, "Failed to renew the leases of %d running message(s) of group %q: %v", len(keys), b.group, err)
		return
	}

	gone := make(map[leaseKey]bool, len(lost))
	for _, pos := range lost {
		if pos >= 0 && int(pos) < len(keys) {
			gone[keys[pos]] = true
		}
	}
	b.leaseMu.Lock()
	defer b.leaseMu.Unlock()
	for _, k := range keys {
		l, running := b.leases[k]
		switch {
		case !running:
			// The receiver has finished since the snapshot (the lease is ended before the acknowledgement):
			// a message that is no longer pending is then normal, not a lost lease.
		case gone[k]:
			delete(b.leases, k)
			b.Warnf(ctx, "Message %q is no longer pending for the worker that runs it (acknowledged, trimmed or taken over); its lease is not renewed", k.id)
		default:
			l.renewed = now
			b.leases[k] = l
		}
	}
}
