package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/logging"
)

// TitleInterface names a receiver in the logs. Receiver implements it with an empty title; a type that embeds
// Receiver redefines Title to name itself.
type TitleInterface interface {
	Title() string
}

// ParamsInterface lists the parameters of a run for the start line in the logs, as "key=value" strings. A receiver
// or its payload may implement it.
type ParamsInterface interface {
	DrawParams() []string
}

// ReceiverInterface is what the bus runs for one message: Start, then Execute, then Finish. Finish runs whenever
// Start succeeded, also after an error or a panic in Execute. Embedding Receiver provides Start and Finish.
type ReceiverInterface interface {
	Start(ctx context.Context, task map[string]any) (context.Context, error)
	Finish(ctx context.Context)
	Execute(ctx context.Context) error
}

// ReceiverFactory creates the receiver for one message. It must return a new receiver (with a new payload) on
// every call: receivers run concurrently.
type ReceiverFactory func() ReceiverInterface

// busBinder is implemented by every type that embeds Receiver: the unexported method is promoted through
// embedding, so the bus finds it on the user's own type. Receivers that do not embed Receiver need nothing.
type busBinder interface {
	bindBus(self ReceiverInterface, inFlight func() int64, workersCount int64)
}

// Receiver is the base of a receiver: embed it and define Execute. Receiver has no Execute of its own, so a
// type that forgets it does not satisfy ReceiverInterface and the factory does not compile. Title and DrawParams can be redefined
// on the embedding type, at any depth: the bus calls them on the object the factory returned, so the
// outermost definition wins.
type Receiver struct {
	Payload any // Payload holds a pointer to a typed struct that is populated by PreloadVariables via json.Unmarshal.
	// Initialize it to a zero-value pointer of the expected type before the receiver is used, e.g.:
	//   r.Payload = &MyPayload{}
	// json.Unmarshal into &r.Payload dereferences the *any and fills the pointed-to struct in place.
	// If left nil, the raw JSON is unmarshalled into map[string]any instead.
	// Implementing ParamsInterface on the payload struct (or on the receiver itself) enables automatic param
	// logging in Start.
	//
	// The factory must create a new payload for every receiver: receivers run concurrently, and a payload
	// shared between them is a data race. A non-pointer value (r.Payload = MyPayload{}) is replaced by
	// map[string]any, so a type assertion to MyPayload would fail.
	start        time.Time
	task         string
	payload      string
	event        time.Time
	id           uuid.UUID
	workersCount int64
	inFlight     func() int64      // read-only view of the bus' in-flight counter
	self         ReceiverInterface // the object the factory returned, set by bindBus
}

// Start initializes the receiver by preloading variables from the task map and logging the start of execution.
//
// Parameters:
//   - ctx: The context for logging and tracing.
//   - task: A map containing task details.
//
// Returns:
//   - context.Context: The updated context with preloaded variables.
//   - error: An error if any issues occur during initialization.
func (r *Receiver) Start(ctx context.Context, task map[string]any) (context.Context, error) {
	var err error
	if ctx, err = r.PreloadVariables(ctx, task); err != nil {
		return ctx, err // logged once by the bus, with the message ids
	}

	logging.Logs.Infof(ctx, "%s Receiver %q %s is starting...", r.InFlight(), r.task, r.title())
	if params, ok := r.Payload.(ParamsInterface); ok {
		logging.Logs.Infof(ctx, "Params: { %s }", strings.Join(params.DrawParams(), ", "))
	} else if params, ok := r.self.(ParamsInterface); ok {
		logging.Logs.Infof(ctx, "Params: { %s }", strings.Join(params.DrawParams(), ", "))
	}

	return ctx, nil
}

// Finish logs the completion of the receiver execution along with the time taken.
//
// Parameters:
//   - ctx: The context for logging and tracing.
func (r *Receiver) Finish(ctx context.Context) {
	logging.Logs.Infof(ctx, "%s Receiver %q %s was finished (%.2fms).", r.InFlight(), r.task, r.title(), float64(time.Since(r.start))/float64(time.Millisecond))
}

// PreloadVariables preloads variables from the task map into the Receiver struct.
//
// Parameters:
//   - ctx: The context for logging and tracing.
//   - task: A map containing task details.
//
// Returns:
//   - error: An error if any issues occur during variable preloading.
func (r *Receiver) PreloadVariables(ctx context.Context, task map[string]any) (context.Context, error) {
	r.start = time.Now()
	if task == nil {
		return ctx, ErrorEmptyTask
	}

	if val, ok := task["receiver"]; ok {
		r.task = getString(val)
	} else {
		return ctx, ErrorInvalidReceiverName
	}

	if val, ok := task["payload"]; ok {
		r.payload = getString(val)
	} else {
		r.payload = ""
	}

	// A "null" payload (AddMessage with a nil payload) is skipped: json.Unmarshal would set Payload to nil and
	// throw away the typed pointer the factory put there.
	if p := strings.TrimSpace(r.payload); p != "" && p != "null" {
		// &r.Payload is *any. If Payload holds a typed pointer (e.g. *MyStruct),
		// json.Unmarshal dereferences through *any and fills that struct in place.
		// If Payload is nil, the result is map[string]any.
		if err := json.Unmarshal([]byte(r.payload), &r.Payload); err != nil {
			return ctx, fmt.Errorf("unmarshal payload of receiver %q: %w", r.task, err)
		}
	}

	// Bus.AddMessage stores the creation time in the "time" field.
	var err error
	if val, ok := task["time"]; ok {
		if r.event, err = time.Parse(time.RFC3339Nano, getString(val)); err != nil {
			logging.Logs.Errorf(ctx, "%s->Failed to parse event time: %v", caller(), err)
			r.event = time.Now().UTC()
		}
	} else {
		r.event = time.Now().UTC()
	}

	if val, ok := task["id"]; ok {
		r.id, err = uuid.Parse(getString(val))
		if err != nil {
			logging.Logs.Errorf(ctx, "%s->Failed to parse task ID: %v", caller(), err)
			r.id = uuid.New()
		}
	} else {
		r.id = uuid.New()
	}

	ctx = context.WithValue(ctx, logging.CtxKeyUUID, r.id.String())

	return ctx, nil
}

// bindBus is called by the bus right after the factory has created the receiver. self is the object the
// factory returned (the user's type that embeds Receiver), whose Title and DrawParams are used in the logs.
// inFlight is a read-only view of the bus' counter, so a receiver cannot change it.
func (r *Receiver) bindBus(self ReceiverInterface, inFlight func() int64, workersCount int64) {
	r.self, r.inFlight, r.workersCount = self, inFlight, workersCount
}

// GetPayload returns the payload of the receiver as a string.
//
// Returns:
//   - string: The payload of the receiver.
func (r *Receiver) GetPayload() string {
	return r.payload
}

// GetID returns the id of the message (the "id" field set by AddMessage). It is the same for every delivery of
// the message, so it is the key for making a receiver idempotent. A message without a valid id (written by
// another producer) gets a random one per delivery, which cannot serve that purpose.
func (r *Receiver) GetID() uuid.UUID {
	return r.id
}

// GetTime returns when the message was published (the "time" field set by AddMessage, UTC).
func (r *Receiver) GetTime() time.Time {
	return r.event
}

// GetName returns the name the message was published for (the receiver name passed to AddMessage).
func (r *Receiver) GetName() string {
	return r.task
}

// InFlight returns a string representation of the number of tasks currently in flight compared to the total number of workers.
//
// Returns:
//   - string: A string in the format "[current_in_flight/total_workers]".
func (r *Receiver) InFlight() string {
	if r.inFlight == nil {
		return fmt.Sprintf("[?/%d]", r.workersCount)
	}
	return fmt.Sprintf("[%d/%d]", r.inFlight(), r.workersCount)
}

// Title is the default title of a receiver in the logs (empty). Redefine it on the embedding type to name
// the receiver; a type that embeds that one can redefine it again.
func (r *Receiver) Title() string {
	return ""
}

// title is the title of the object the factory returned, so the outermost redefinition of Title is used.
// Before the receiver is bound to a bus it is the default one.
func (r *Receiver) title() string {
	if t, ok := r.self.(TitleInterface); ok {
		return t.Title()
	}
	return r.Title()
}

// caller returns "file:line->function" of the code that calls it, for log lines.
func caller() string {
	pc, file, line, _ := runtime.Caller(1)
	return fmt.Sprintf("%s:%d->%s", filepath.Base(file), line,
		strings.TrimPrefix(runtime.FuncForPC(pc).Name(), "github.com/ra-company/bus."))
}

func getString(val any) string {
	if str, ok := val.(string); ok {
		return str
	}
	return ""
}
