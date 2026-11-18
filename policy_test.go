package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Unregistered receivers: policy
// ---------------------------------------------------------------------------

func TestUnregisteredPolicy_String(t *testing.T) {
	require.Equal(t, "ack", UnregisteredAck.String())
	require.Equal(t, "leave", UnregisteredLeave.String())
	require.Equal(t, "dead-letter", UnregisteredDeadLetter.String())
	require.Equal(t, "UnregisteredPolicy(42)", UnregisteredPolicy(42).String())
}

func (e *testEnv) pendingCount() int64 {
	e.t.Helper()
	p, err := e.rdb.Client().XPending(e.t.Context(), e.b.stream, e.b.group).Result()
	require.NoError(e.t, err)
	return p.Count
}

// readOnce makes one worker step that goes straight to XREADGROUP.
func (e *testEnv) readOnce() {
	e.b.lastClaim.Store(time.Now().UnixNano())
	e.b.workerJob(e.t.Context(), "w1")
}

// claimOnce waits until the pending messages are idle and makes one worker step that claims one of them.
func (e *testEnv) claimOnce() {
	time.Sleep(time.Duration(e.b.retryIdleTime)*time.Second + 100*time.Millisecond)
	e.b.lastClaim.Store(0)
	e.b.workerJob(e.t.Context(), "w2")
}

func redisUnknownPolicyFallsBackToAck(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.Unregistered = UnregisteredPolicy(42) })
	require.Equal(t, UnregisteredAck, env.b.unregistered)
}

// UnregisteredLeave: an instance without the receiver leaves the message pending, and an instance that has
// the receiver (a newer version during a rolling deploy) processes it after RetryIdleTime.
func redisUnregisteredLeaveIsPickedUpLater(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.Unregistered = UnregisteredLeave })
	env.add("new_event")

	env.readOnce() // the "old" instance: no receiver
	require.EqualValues(t, 1, env.pendingCount(), "the message must stay pending")
	require.Empty(t, env.deadEntries())
	require.Empty(t, env.log.errorsMentioning("not registered"), "an unregistered receiver is not an error")

	var executed atomic.Int32
	env.register("new_event", func(context.Context, *scriptReceiver) error { executed.Add(1); return nil })

	env.claimOnce() // the "new" instance takes it over
	require.EqualValues(t, 1, executed.Load(), "the receiver must run once it is registered")
	require.Zero(t, env.pendingCount())
	require.Empty(t, env.deadEntries())
}

// UnregisteredLeave: if nobody has the receiver, the message ends in the dead-letter stream after NumRetries
// deliveries instead of being lost.
func redisUnregisteredLeaveExhaustedGoesToDeadLetter(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) {
		c.Unregistered = UnregisteredLeave
		c.NumRetries = 2
	})
	env.add("ghost")

	env.readOnce()  // delivery 1
	env.claimOnce() // delivery 2
	require.EqualValues(t, 1, env.pendingCount())
	require.Empty(t, env.deadEntries())

	env.claimOnce() // delivery 3 > NumRetries
	require.Zero(t, env.pendingCount(), "the message must be acknowledged after moving it")
	dead := env.deadEntries()
	require.Len(t, dead, 1)
	require.Equal(t, DeadReasonUnregistered, dead[0].Values["dead_reason"])
	require.Equal(t, "ghost", dead[0].Values["receiver"])
	require.Equal(t, "3", dead[0].Values["dead_deliveries"])
	require.EqualValues(t, 1, env.b.Stats().DeadLettered)
}

func redisUnregisteredDeadLetterAtOnce(t *testing.T) {
	env := newTestEnv(t, func(c *BusConfiguration) { c.Unregistered = UnregisteredDeadLetter })
	env.add("ghost")

	env.readOnce()
	require.Zero(t, env.pendingCount())
	dead := env.deadEntries()
	require.Len(t, dead, 1)
	require.Equal(t, DeadReasonUnregistered, dead[0].Values["dead_reason"])
	require.Equal(t, "1", dead[0].Values["dead_deliveries"])
}

// The default is to acknowledge and drop.
func redisUnregisteredAckIsDefault(t *testing.T) {
	env := newTestEnv(t, nil)
	require.Equal(t, UnregisteredAck, env.b.unregistered)
	env.add("ghost")

	env.readOnce()
	require.Zero(t, env.pendingCount())
	require.Empty(t, env.deadEntries())
}

// The counters show the unregistered deliveries for every policy.
func redisUnregisteredIsCounted(t *testing.T) {
	for _, p := range []UnregisteredPolicy{UnregisteredAck, UnregisteredLeave, UnregisteredDeadLetter} {
		t.Run(p.String(), func(t *testing.T) {
			env := newTestEnv(t, func(c *BusConfiguration) { c.Unregistered = p })
			env.add("ghost")
			env.readOnce()
			days, err := env.b.Counters(t.Context(), "", 1)
			require.NoError(t, err)
			require.EqualValues(t, 1, days[0].Total.Unregistered)
			require.EqualValues(t, 1, days[0].ByReceiver["ghost"].Unregistered)
		})
	}
}

func TestUnregistered_UnknownPolicyFallsBackToAck(t *testing.T) {
	eachRedis(t, redisUnknownPolicyFallsBackToAck)
}
func TestUnregistered_LeaveIsPickedUpLater(t *testing.T) {
	eachRedis(t, redisUnregisteredLeaveIsPickedUpLater)
}
func TestUnregistered_LeaveExhaustedGoesToDeadLetter(t *testing.T) {
	eachRedis(t, redisUnregisteredLeaveExhaustedGoesToDeadLetter)
}
func TestUnregistered_DeadLetterAtOnce(t *testing.T) { eachRedis(t, redisUnregisteredDeadLetterAtOnce) }
func TestUnregistered_AckIsDefault(t *testing.T)     { eachRedis(t, redisUnregisteredAckIsDefault) }
func TestUnregistered_IsCounted(t *testing.T)        { eachRedis(t, redisUnregisteredIsCounted) }

// ---------------------------------------------------------------------------
// Receivers are bound to the bus automatically
// ---------------------------------------------------------------------------

// titledReceiver redefines Title of Receiver.
type titledReceiver struct {
	Receiver
	seenTitle, seenInFlight string
}

func (r *titledReceiver) Title() string { return "Titled" }
func (r *titledReceiver) Execute(context.Context) error {
	r.seenTitle, r.seenInFlight = r.title(), r.InFlight()
	return nil
}

// renamedReceiver is a component built on titledReceiver that redefines its title again.
type renamedReceiver struct{ titledReceiver }

func (r *renamedReceiver) Title() string { return "Renamed" }

// inheritedReceiver is built on titledReceiver and keeps its title.
type inheritedReceiver struct{ titledReceiver }

func run(t *testing.T, b *Bus, r ReceiverInterface) {
	t.Helper()
	require.NoError(t, b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "r"}}))
}

func TestBind_OwnTitleIsUsed(t *testing.T) {
	b := &Bus{workersCount: 3}
	r := &titledReceiver{}
	run(t, b, r)
	require.Equal(t, "Titled", r.seenTitle)
	require.Equal(t, "[1/3]", r.seenInFlight, "the bus' counter and workers count must be bound")
	require.EqualValues(t, 0, b.inFlight.Load())
}

// The outermost redefinition wins, even when the method that logs the title belongs to an inner type.
func TestBind_TitleRedefinedInDescendant(t *testing.T) {
	b := &Bus{workersCount: 1}
	r := &renamedReceiver{}
	run(t, b, r)
	require.Equal(t, "Renamed", r.seenTitle)
}

func TestBind_TitleInheritedFromParent(t *testing.T) {
	b := &Bus{workersCount: 1}
	r := &inheritedReceiver{}
	run(t, b, r)
	require.Equal(t, "Titled", r.seenTitle)
}

// A receiver without its own Title gets the empty default title.
func TestBind_WithoutOwnTitle(t *testing.T) {
	b := &Bus{workersCount: 1}
	var title string
	r := &scriptReceiver{run: func(_ context.Context, s *scriptReceiver) error { title = s.title(); return nil }}
	require.NoError(t, b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "s"}}))
	require.Equal(t, "", title)
}

// The counter a receiver sees is read-only: it is a func, not the bus' atomic.
func TestBind_InFlightIsReadOnly(t *testing.T) {
	var counter atomic.Int64
	r := &Receiver{}
	r.bindBus(nil, counter.Load, 2)
	counter.Store(1)
	require.Equal(t, "[1/2]", r.InFlight())
}

// plainReceiver implements ReceiverInterface without embedding Receiver: it needs no bus-specific method.
type plainReceiver struct{ executed bool }

func (p *plainReceiver) Start(ctx context.Context, _ map[string]any) (context.Context, error) {
	return ctx, nil
}
func (p *plainReceiver) Finish(context.Context)        {}
func (p *plainReceiver) Execute(context.Context) error { p.executed = true; return nil }

func TestBind_ReceiverWithoutEmbedding(t *testing.T) {
	var _ ReceiverInterface = (*plainReceiver)(nil)
	b := &Bus{}
	p := &plainReceiver{}
	require.NoError(t, b.execute(t.Context(), p, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "p"}}))
	require.True(t, p.executed)
}

// paramsReceiver draws its params itself instead of on the payload, and has no title of its own.
type paramsReceiver struct {
	Receiver
	drawn atomic.Bool
}

func (r *paramsReceiver) DrawParams() []string          { r.drawn.Store(true); return []string{"a=1"} }
func (r *paramsReceiver) Execute(context.Context) error { return nil }

func TestBind_ParamsOnReceiver(t *testing.T) {
	b := &Bus{}
	r := &paramsReceiver{}
	require.NoError(t, b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "p"}}))
	require.True(t, r.drawn.Load(), "DrawParams of the receiver itself must be used")
}
