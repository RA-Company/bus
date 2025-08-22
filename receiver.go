package bus

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/logging"
)

type ReceiverInterface interface {
	Start(ctx context.Context, task map[string]any) error
	Finish()
	Execute() error
}

type Receiver struct {
	start   time.Time
	task    string
	payload string
	event   time.Time
	id      uuid.UUID
	Ctx     context.Context
}

func (dst *Receiver) Start(ctx context.Context, task map[string]any) error {
	dst.PreloadVariables(ctx, task)

	logging.Logs.Infof(dst.Ctx, "Receiver %q is starting...", dst.task)

	return nil
}

func (dst *Receiver) Finish() {
	logging.Logs.Infof(dst.Ctx, "Receiver %q was finished (%.2fms).", dst.task, float64(time.Since(dst.start))/1000000)
}

func (dst *Receiver) Execute() error {
	// Simulate receiver execution
	logging.Logs.Infof(dst.Ctx, "Executing receiver %q...", dst.task)
	time.Sleep(100 * time.Millisecond) // Simulate some work
	return nil
}

// PreloadVariables preloads variables from the task map into the Receiver struct.
//
// Parameters:
//   - ctx: The context for logging and tracing.
//   - task: A map containing task details.
//
// Returns:
//   - error: An error if any issues occur during variable preloading.
func (dst *Receiver) PreloadVariables(ctx context.Context, task map[string]any) error {
	dst.start = time.Now()
	if task == nil {
		return nil
	}

	if val, ok := task["receiver"]; ok {
		dst.task = val.(string)
	}

	if val, ok := task["payload"]; ok {
		dst.payload = val.(string)
	}

	if val, ok := task["event"]; ok {
		var err error
		dst.event, err = time.Parse(time.RFC3339Nano, val.(string))
		if err != nil {
			logging.Logs.Errorf(ctx, "Failed to parse event time: %v", err)
			return err
		}
	} else {
		dst.event = time.Now().UTC()
	}

	if val, ok := task["id"]; ok {
		var err error
		dst.id, err = uuid.Parse(val.(string))
		if err != nil {
			logging.Logs.Errorf(ctx, "Failed to parse task ID: %v", err)
			return err
		}
	} else {
		dst.id = uuid.New()
	}

	dst.Ctx = context.WithValue(ctx, logging.CtxKeyUUID, dst.id.String())

	return nil
}

// Payload returns the payload of the receiver.
//
// Returns:
//   - string: The payload of the receiver.
func (dst *Receiver) Payload() string {
	return dst.payload
}
