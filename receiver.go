package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/logging"
)

type TitleInterface interface {
	Title() string
}

type ParamsInterface interface {
	DrawParams() []string
}

type ReceiverInterface interface {
	Start(ctx context.Context, task map[string]any) (context.Context, error)
	Finish(ctx context.Context)
	Execute(ctx context.Context) error
	SetBusContext(inFlight *atomic.Int64, workersCount int64)
}

type ReceiverFactory func() ReceiverInterface

type Receiver struct {
	Self         TitleInterface // Self is an optional struct that implements TitleInterface to provide a custom title for logging.
	Payload      any            // Payload holds a pointer to a typed struct that is populated by PreloadVariables via json.Unmarshal.
	// Initialize it to a zero-value pointer of the expected type before the receiver is used, e.g.:
	//   r.Payload = &MyPayload{}
	// json.Unmarshal into &r.Payload dereferences the *any and fills the pointed-to struct in place.
	// If left nil, the raw JSON is unmarshalled into map[string]any instead.
	// Implementing ParamsInterface on the payload struct enables automatic param logging in Start.
	start        time.Time
	task         string
	payload      string
	event        time.Time
	id           uuid.UUID
	workersCount int64
	inFlight     *atomic.Int64
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
		logging.Logs.Errorf(ctx, "%s->Failed to preload variables for receiver %q: %v", Caller(), r.task, err)
		return ctx, err
	}

	logging.Logs.Infof(ctx, "%s Receiver %q %s is starting...", r.InFlight(), r.task, r.title())
	if params, ok := r.Payload.(ParamsInterface); ok {
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

// Execute is a placeholder method that should be overridden by specific receiver implementations.
//
// Parameters:
//   - ctx: The context for the receiver execution, used for cancellation and timeout.
//
// Returns:
//   - error: An error if any issues occur during execution. By default, it panics to indicate that it should be implemented by specific receivers.
func (r *Receiver) Execute(_ context.Context) error {
	panic(fmt.Sprintf("Execute not implemented for receiver %q", r.task))
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

	if r.payload != "" {
		// &r.Payload is *any. If Payload holds a typed pointer (e.g. *MyStruct),
		// json.Unmarshal dereferences through *any and fills that struct in place.
		// If Payload is nil, the result is map[string]any.
		err := json.Unmarshal([]byte(r.payload), &r.Payload)
		if err != nil {
			logging.Logs.Errorf(ctx, "%s->Failed to unmarshal payload: %v", Caller(), err)
			return ctx, err
		}
	}

	var err error
	if val, ok := task["event"]; ok {
		if r.event, err = time.Parse(time.RFC3339Nano, getString(val)); err != nil {
			logging.Logs.Errorf(ctx, "%s->Failed to parse event time: %v", Caller(), err)
			r.event = time.Now().UTC()
		}
	} else {
		r.event = time.Now().UTC()
	}

	if val, ok := task["id"]; ok {
		r.id, err = uuid.Parse(getString(val))
		if err != nil {
			logging.Logs.Errorf(ctx, "%s->Failed to parse task ID: %v", Caller(), err)
			r.id = uuid.New()
		}
	} else {
		r.id = uuid.New()
	}

	ctx = context.WithValue(ctx, logging.CtxKeyUUID, r.id.String())

	return ctx, nil
}

// SetBusContext injects the bus-level in-flight counter and total workers count
// into the receiver so that InFlight() returns meaningful values.
// Called by Bus.processMessage immediately after creating the receiver via the factory.
func (r *Receiver) SetBusContext(inFlight *atomic.Int64, workersCount int64) {
	r.inFlight = inFlight
	r.workersCount = workersCount
}

// GetPayload returns the payload of the receiver as a string.
//
// Returns:
//   - string: The payload of the receiver.
func (r *Receiver) GetPayload() string {
	return r.payload
}

// InFlight returns a string representation of the number of tasks currently in flight compared to the total number of workers.
//
// Returns:
//   - string: A string in the format "[current_in_flight/total_workers]".
func (r *Receiver) InFlight() string {
	if r.inFlight == nil {
		return fmt.Sprintf("[?/%d]", r.workersCount)
	}
	return fmt.Sprintf("[%d/%d]", r.inFlight.Load(), r.workersCount)
}

func (r *Receiver) title() string {
	if r.Self != nil {
		return r.Self.Title()
	}
	return ""
}

func getString(val any) string {
	if str, ok := val.(string); ok {
		return str
	}
	return ""
}
