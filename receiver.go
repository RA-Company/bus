package bus

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/logging"
)

type ReceiverInterface interface {
	Start(ctx context.Context, task map[string]any) (context.Context, error)
	Finish(ctx context.Context)
	Execute(ctx context.Context) error
}

type ReceiverFactory func() ReceiverInterface

type Receiver struct {
	start   time.Time
	task    string
	payload string
	event   time.Time
	id      uuid.UUID
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
		logging.Logs.Errorf(ctx, "Failed to preload variables for receiver %q: %v", r.task, err)
		return ctx, err
	}

	logging.Logs.Infof(ctx, "Receiver %q is starting...", r.task)

	return ctx, nil
}

// Finish logs the completion of the receiver execution along with the time taken.
//
// Parameters:
//   - ctx: The context for logging and tracing.
func (r *Receiver) Finish(ctx context.Context) {
	logging.Logs.Infof(ctx, "Receiver %q was finished (%.2fms).", r.task, float64(time.Since(r.start))/float64(time.Millisecond))
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

	var err error
	if val, ok := task["event"]; ok {
		if r.event, err = time.Parse(time.RFC3339Nano, getString(val)); err != nil {
			logging.Logs.Errorf(ctx, "Failed to parse event time: %v", err)
			r.event = time.Now().UTC()
		}
	} else {
		r.event = time.Now().UTC()
	}

	if val, ok := task["id"]; ok {
		r.id, err = uuid.Parse(getString(val))
		if err != nil {
			logging.Logs.Errorf(ctx, "Failed to parse task ID: %v", err)
			r.id = uuid.New()
		}
	} else {
		r.id = uuid.New()
	}

	ctx = context.WithValue(ctx, logging.CtxKeyUUID, r.id.String())

	return ctx, nil
}

// Payload returns the payload of the receiver.
//
// Returns:
//   - string: The payload of the receiver.
func (r *Receiver) Payload() string {
	return r.payload
}

func getString(val any) string {
	if str, ok := val.(string); ok {
		return str
	}
	return ""
}
