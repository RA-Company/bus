package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	re "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// What a receiver reads from the message
// ---------------------------------------------------------------------------

func TestReceiver_Accessors(t *testing.T) {
	id := uuid.New()
	at := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	r := &Receiver{}
	_, err := r.PreloadVariables(t.Context(), map[string]any{
		"receiver": "orders", "id": id.String(), "time": at.Format(time.RFC3339Nano), "payload": `{"a":1}`,
	})
	require.NoError(t, err)
	require.Equal(t, id, r.GetID())
	require.True(t, at.Equal(r.GetTime()))
	require.Equal(t, "orders", r.GetName())
	require.Equal(t, `{"a":1}`, r.GetPayload())
}

type typedPayload struct{ A int }

func TestReceiver_NullPayloadKeepsTypedPointer(t *testing.T) {
	for _, raw := range []string{"null", " null ", ""} {
		r := &Receiver{Payload: &typedPayload{}}
		_, err := r.PreloadVariables(t.Context(), map[string]any{"receiver": "r", "payload": raw})
		require.NoError(t, err)
		p, ok := r.Payload.(*typedPayload)
		require.True(t, ok, "payload %q must keep the typed pointer, got %#v", raw, r.Payload)
		require.NotNil(t, p)
	}
}

func redisNilPayloadEndToEnd(t *testing.T) {
	env := newTestEnv(t, nil)
	var got *typedPayload
	env.b.RegisterReceiver(t.Context(), "typed", func() ReceiverInterface {
		r := &scriptReceiver{run: func(_ context.Context, s *scriptReceiver) error {
			got = s.Payload.(*typedPayload) // would panic if the pointer were lost
			return nil
		}}
		r.Payload = &typedPayload{}
		return r
	})
	_, err := env.b.AddMessage(t.Context(), "typed", nil, "")
	require.NoError(t, err)

	env.readOnce()
	require.NotNil(t, got)
	require.Zero(t, env.pendingCount())
}

func TestReceiver_NilPayloadEndToEnd(t *testing.T) { eachRedis(t, redisNilPayloadEndToEnd) }

// ---------------------------------------------------------------------------
// Start, Execute, Finish
// ---------------------------------------------------------------------------

// Finish runs after a failed Execute too (it logs the end of the run and its duration).
type finishRecorder struct {
	Receiver
	finished bool
}

func (f *finishRecorder) Execute(context.Context) error { return errors.New("fail") }

func (f *finishRecorder) Finish(ctx context.Context) { f.finished = true; f.Receiver.Finish(ctx) }

func TestExecute_FinishAfterError(t *testing.T) {
	b := &Bus{}
	r := &finishRecorder{}
	err := b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "r"}})
	require.EqualError(t, err, "fail")
	require.True(t, r.finished)
}

func TestExecute_StartErrorIsWrapped(t *testing.T) {
	b := &Bus{}
	err := b.execute(t.Context(), &finishRecorder{}, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "r", "payload": "{"}})
	require.ErrorContains(t, err, "start receiver")
	require.ErrorContains(t, err, "unmarshal payload")
}

type panicFinishRecorder struct{ finishRecorder }

func (p *panicFinishRecorder) Execute(context.Context) error { panic("boom") }

func TestExecute_FinishAfterPanic(t *testing.T) {
	b := &Bus{}
	r := &panicFinishRecorder{}
	err := b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "r"}})
	require.ErrorContains(t, err, "panicked")
	require.True(t, r.finished, "Finish must run after a panic in Execute")
	require.Zero(t, b.inFlight.Load())
}

func TestExecute_NoFinishAfterStartError(t *testing.T) {
	b := &Bus{}
	r := &finishRecorder{}
	err := b.execute(t.Context(), r, re.XMessage{ID: "1-0", Values: map[string]any{"receiver": "r", "payload": "{"}})
	require.Error(t, err)
	require.False(t, r.finished, "a receiver that did not start is not finished")
}
