package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/ra-company/database/redis"
	"github.com/ra-company/logging"
	re "github.com/redis/go-redis/v9"
)

var (
	ErrorEmptyReceiverTitle    = fmt.Errorf("receiver title cannot be empty")
	ErrorReceiverNotRegistered = fmt.Errorf("receiver not registered")
)

type Bus struct {
	logging.CustomLogger
	redis        *redis.RedisClient
	stream       string                     // Stream name for the bus
	group        string                     // Consumer group name
	workersCount int                        // Number of workers to process tasks
	numRetries   int                        // Number of retries for failed tasks
	streamSize   int                        // Maximum size of the stream
	receivers    map[string]ReceiverFactory // Map of receivers by name
}

type BusConfiguration struct {
	Redis        RedisConfiguration // Configuration for Redis connection
	Stream       string             // Stream name for the bus
	Group        string             // Consumer group name
	WorkersCount int                // Number of workers to process tasks
	NumRetries   int                // Number of retries for failed tasks
	StreamSize   int                // Maximum size of the stream
}

type RedisConfiguration struct {
	Hosts           string // Comma-separated list of Redis hosts
	DB              int    // Redis database number
	Password        string // Redis password
	DoNotLogQueries bool   // Whether to log Redis queries
}

// SetLogger allows setting a custom logger for the bus.
// This is useful when you want to use a different logging mechanism
// instead of the default one provided by the package.
//
// Parameters:
//   - logger: An instance of a type that implements the logging.Logger interface.
func (dst *Bus) SetLogger(logger logging.Logger) {
	dst.CustomLogger.SetLogger(logger)
	dst.redis.SetLogger(logger)
}

// Init initializes the bus with the provided configuration.
// It sets up the Redis client, stream, group, and workers count.
// If the stream or group is not provided, it defaults to "default-stream" and "default-group".
// It also creates the Redis stream group if it does not already exist.
//
// Parameters:
//   - ctx: The context for the operation.
//   - config: The configuration for the bus, including Redis connection details, stream name, group name, workers count, and number of retries.
func (dst *Bus) Init(ctx context.Context, config *BusConfiguration) {
	dst.receivers = make(map[string]ReceiverFactory)
	dst.redis = &redis.RedisClient{
		DoNotLogQueries: config.Redis.DoNotLogQueries,
	}
	dst.redis.Start(ctx, config.Redis.Hosts, config.Redis.Password, config.Redis.DB)

	dst.stream = config.Stream
	if dst.stream == "" {
		dst.stream = "default-stream" // Default stream name if not provided
	}

	dst.group = config.Group
	if dst.group == "" {
		dst.group = "default-group" // Default group name if not provided
	}

	dst.workersCount = config.WorkersCount
	if dst.workersCount <= 0 {
		dst.workersCount = 10 // Default to 10 worker if not provided or invalid
	}
	if dst.workersCount > 100 {
		dst.Warn(ctx, "Workers count is too high (%d), setting to 100", dst.workersCount)
		dst.workersCount = 100 // Cap the workers count to 100
	}

	dst.numRetries = config.NumRetries
	if dst.numRetries <= 0 {
		dst.numRetries = 5 // Default to 5 retries if not provided or invalid
	}

	dst.streamSize = config.StreamSize
	if dst.streamSize <= 0 {
		dst.streamSize = 10000 // Default to 10000 messages in the stream if not provided or invalid
	}

	err := dst.redis.XGroupCreateMkStream(ctx, dst.stream, dst.group, "$")
	if err != nil && err != redis.ErrorGroupAlreadyExists {
		dst.Fatal(ctx, "Failed to create group %s for stream %s: %v", dst.group, dst.stream, err)
		os.Exit(1)
	}
}

// RegisterReceiver registers a new receiver with the bus
// If the receiver name is empty, it returns without any actions.
// If the receiver is already registered, it logs a warning and overwrites the existing receiver.
// If the receiver is registered successfully, it logs an info message.
// This method is useful for dynamically adding or updating receivers in the bus system.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - title: The title of the receiver to register.
//   - receiver: The ReceiverInterface implementation that defines the receiver's behavior.
func (dst *Bus) RegisterReceiver(ctx context.Context, title string, receiver ReceiverFactory) {
	if title == "" {
		dst.Error(ctx, "Receiver title cannot be empty")
		return
	}

	if dst.receivers == nil {
		dst.receivers = make(map[string]ReceiverFactory)
	}

	if _, exists := dst.receivers[title]; exists {
		dst.Warn(ctx, "Receiver %q is already registered, overwriting it", title)
	}

	dst.receivers[title] = receiver
	dst.Info(ctx, "Receiver %q registered successfully", title)
}

// Start starts the bus by launching the specified number of worker goroutines.
// Each worker will continuously process tasks from the Redis stream.
// It waits for all workers to finish before returning.
// This method is useful for starting the bus and processing tasks concurrently.
// It uses a WaitGroup to ensure all workers complete before returning.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
func (dst *Bus) Start(ctx context.Context) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	for i := 0; i < dst.workersCount; i++ {
		wg.Go(func() {
			dst.safeWorker(ctx)
		})
	}

	<-ctx.Done()
	dst.Info(ctx, "Bus stopped, waiting for workers to finish")

	wg.Wait()
	dst.Info(ctx, "All workers finished, bus stopped")
}

// AddMessage adds a new message to the bus stream.
// It generates a new UUID for the message, serializes the payload to JSON,
// and adds the message to the Redis stream with the current time.
// If the message name is empty, it returns an error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - receiver: The title of the receiver.
//   - payload: The payload for the receiver, which can be any data type.
//
// Returns:
//   - error: An error if the receiver title is empty or if adding the receiver to the stream fails.
func (dst *Bus) AddMessage(ctx context.Context, receiver string, payload any) (string, error) {
	if receiver == "" {
		return "", ErrorEmptyReceiverTitle
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	id := uuid.New().String()
	message := map[string]any{
		"receiver": receiver,                                  // Receiver title
		"id":       id,                                        // Unique ID for the message
		"payload":  string(data),                              // Serialized payload
		"time":     time.Now().UTC().Format(time.RFC3339Nano), // Current time in RFC3339Nano format
		"retry":    0,                                         // Retry count initialized to 0
	}

	_, err = dst.redis.XAdd(ctx, &re.XAddArgs{
		Stream: dst.stream,
		Values: message,
		MaxLen: int64(dst.streamSize), // Limit the stream size
	})
	if err != nil {
		dst.Error(ctx, "dst.redis.XAdd() error: %v", err)
		return "", err
	}

	dst.Info(ctx, "Message %q added to stream %q with ID %q", receiver, dst.stream, id)

	return id, nil
}

// retryMessage retries a message by adding it back to the stream with an incremented retry count.
// If the retry count exceeds the configured limit, it does not add the message back to the stream.
// This method is useful for handling failed messages and implementing retry logic.
//
// Parameters:
//   - ctx: The context for the operation.
//   - message: The message to retry, which should contain the message ID and other necessary data.
//
// Returns:
//   - error: An error if the retry operation fails, otherwise nil.
func (dst *Bus) retryMessage(ctx context.Context, message map[string]any) error {
	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}

	var err error
	retry := 0
	if _, ok := message["retry"]; ok {
		retry, err = strconv.Atoi(message["retry"].(string))
		if err != nil {
			retry = 0
		}
	}

	message["retry"] = retry + 1 // Increment retry count
	if retry > dst.numRetries {
		return nil
	}

	if _, err := dst.redis.XAdd(ctx, &re.XAddArgs{Stream: dst.stream, Values: message}); err != nil {
		dst.Error(ctx, "dst.redis.XAdd() error: %v", err)
		return err
	}

	return nil
}

// safeWorker is a wrapper around the worker function that recovers from panics.
// It ensures that if a panic occurs within the worker, it is caught and logged,
// allowing the worker to continue processing other messages.
// This method is useful for improving the robustness of the worker by preventing crashes
// due to unexpected errors.
//
// Parameters:
//   - ctx: The context for the worker, used for cancellation and timeout.
func (dst *Bus) safeWorker(ctx context.Context) {
	worker := uuid.New().String()
	ctx = context.WithValue(ctx, logging.CtxKeyUUID, worker)
	dst.Info(ctx, "Worker %q was started", worker)

	for {
		select {
		case <-ctx.Done():
			dst.Info(ctx, "Worker %q was stopped", worker)
			return
		default:
			func() {
				defer func() {
					if r := recover(); r != nil {
						dst.Error("[worker %q] panic recovered: %v \033[1m \033[31m%s\033[0m", worker, r, strings.ReplaceAll(string(debug.Stack()), "\n", " "))
					}
				}()

				dst.workerJob(ctx, worker)
			}()
		}
	}
}

// workerJob is a goroutine that continuously processes messages from the Redis stream.
// It auto-claims messages that have been idle for a specified minimum time and executes them.
// If a message fails to execute, it retries the message based on the configured retry logic.
// The worker will log its status and any errors encountered during processing.
// It will also handle graceful shutdown when the context is done.
//
// Parameters:
//   - ctx: The context for the worker, used for cancellation and timeout.
//   - bus: The Bus instance that contains the Redis client and task definitions.
func (dst *Bus) workerJob(ctx context.Context, worker string) {

	// Local panic recovery
	claims, err := dst.redis.XAutoClaim(ctx, &re.XAutoClaimArgs{
		Stream:   dst.stream,
		Group:    dst.group,
		Consumer: worker,
		MinIdle:  5 * time.Minute, // Minimum idle time for auto claim
		Count:    1,
		Start:    "0-0",
	})

	if err != nil {
		dst.Error(ctx, "Failed to auto claim message: %v", err)
		time.Sleep(5 * time.Second) // Wait before retrying
		return
	}

	if len(claims) == 1 {
		if _, ok := dst.receivers[claims[0].Values["receiver"].(string)]; ok {
			err = dst.processMessage(ctx, claims[0])
			if err != nil {
				dst.Error(ctx, "Failed to execute receiver %q: %v", claims[0].Values["receiver"], err)
			}
		}
		return
	}

	messages, err := dst.redis.XReadGroup(ctx, &re.XReadGroupArgs{
		Group:    dst.group,
		Consumer: worker,
		Streams:  []string{dst.stream, ">"},
		Block:    1 * time.Second, // Block indefinitely until a new message arrives
		Count:    1,
	})
	if err != nil {
		dst.Error(ctx, "Failed to read from stream %s: %v", dst.stream, err)
		return
	}

	if len(messages) == 0 {
		return // No messages to process
	}

	if len(messages[0].Messages) == 0 {
		return
	}

	err = dst.processMessage(ctx, messages[0].Messages[0])
	if err != nil {
		dst.Error(ctx, "Failed to process message %q with id %q: %v", messages[0].Messages[0].Values["receiver"], messages[0].Messages[0].Values["id"], err)
	}
}

// processMessage processes a message by starting the associated receiver, executing it, and finishing it.
// It retrieves the receiver name from the message, checks if the receiver is registered,
// and if so, it starts the receiver, executes it, and finishes it.
// If the receiver fails to start or execute, it retries the receiver based on the retry logic.
// After processing the receiver, it acknowledges the message in the Redis stream.
//
// Parameters:
//   - ctx: The context for the operation.
//   - bus: The Bus instance that contains the Redis client and receivers definitions.
//   - msg: The message containing the receiver data, which should include the receiver title and other necessary information.
//
// Returns:
//   - error: An error if the receiver fails to start or execute, otherwise nil.
func (dst *Bus) processMessage(ctx context.Context, msg re.XMessage) error {
	factory, ok := dst.receivers[msg.Values["receiver"].(string)]
	if !ok {
		dst.redis.XAck(ctx, dst.stream, dst.group, msg.ID)
		return ErrorReceiverNotRegistered
	}

	receiver := factory()
	err := receiver.Start(ctx, msg.Values)
	if err != nil {
		dst.Error(ctx, "Failed to start receiver %q: %v", msg.Values["receiver"], err)
		// Retry the receiver
		err = dst.retryMessage(ctx, msg.Values)
		if err != nil {
			dst.Error(ctx, "Failed to retry message %q: %v", msg.Values["id"], err)
		}
		return err
	}

	err = receiver.Execute()
	if err != nil {
		dst.Error(ctx, "Failed to execute receiver %q: %v", msg.Values["receiver"], err)
		// Retry the receiver
		err = dst.retryMessage(ctx, msg.Values)
		if err != nil {
			dst.Error(ctx, "Failed to retry message %q: %v", msg.Values["id"], err)
		}
		return err
	}

	receiver.Finish()

	dst.redis.XAck(ctx, dst.stream, dst.group, msg.ID)

	return nil
}
