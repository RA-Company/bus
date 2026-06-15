package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ErrorInvalidReceiverName   = fmt.Errorf("invalid receiver name")
	ErrorEmptyTask             = fmt.Errorf("task cannot be empty")
)

const retryFailed = math.MaxInt // A large number to indicate that a message should not be retried anymore

type Bus struct {
	logging.CustomLogger
	redis         *redis.RedisClient
	stream        string                     // Stream name for the bus
	group         string                     // Consumer group name
	workersCount  int64                      // Number of workers to process tasks (default: 10)
	numRetries    int64                      // Number of retries for failed tasks (default: 5)
	streamSize    int64                      // Maximum size of the stream (default: 10000)
	retryIdleTime int64                      // Minimum idle time in seconds before retrying a message (default: 60 seconds)
	receivers     map[string]ReceiverFactory // Map of receivers by name
	mu            sync.RWMutex               // Mutex to protect access to the receivers map
	inFlight      atomic.Int64               // Counter for the number of messages currently being processed
}

type BusConfiguration struct {
	Redis         redis.Config // Configuration for Redis connection
	Stream        string       // Stream name for the bus
	Group         string       // Consumer group name
	WorkersCount  int64        // Number of workers to process tasks
	NumRetries    int64        // Number of retries for failed tasks
	StreamSize    int64        // Maximum size of the stream
	RetryIdleTime int64        // Minimum idle time before retrying a message (in seconds)
}

// SetLogger allows setting a custom logger for the bus.
// This is useful when you want to use a different logging mechanism
// instead of the default one provided by the package.
//
// Parameters:
//   - logger: An instance of a type that implements the logging.Logger interface.
func (b *Bus) SetLogger(logger logging.Logger) {
	b.CustomLogger.SetLogger(logger)
	b.redis.SetLogger(logger)
}

// Init initializes the bus with the provided configuration.
// It sets up the Redis client, stream, group, and workers count.
// If the stream or group is not provided, it defaults to "default-stream" and "default-group".
// It also creates the Redis stream group if it does not already exist.
//
// Parameters:
//   - ctx: The context for the operation.
//   - config: The configuration for the bus, including Redis connection details, stream name, group name, workers count, and number of retries.
func (b *Bus) Init(ctx context.Context, config *BusConfiguration) error {
	b.receivers = make(map[string]ReceiverFactory)
	b.redis = &redis.RedisClient{}
	b.redis.Start(ctx, &config.Redis)

	b.stream = config.Stream
	if b.stream == "" {
		b.stream = "default-stream" // Default stream name if not provided
	}

	b.group = config.Group
	if b.group == "" {
		b.group = "default-group" // Default group name if not provided
	}

	b.workersCount = config.WorkersCount
	if b.workersCount <= 0 {
		b.workersCount = 10 // Default to 10 worker if not provided or invalid
	}
	if b.workersCount > 100 {
		b.Warn(ctx, "Workers count is too high (%d), setting to 100", b.workersCount)
		b.workersCount = 100 // Cap the workers count to 100
	}

	b.numRetries = config.NumRetries
	if b.numRetries <= 0 {
		b.numRetries = 5 // Default to 5 retries if not provided or invalid
	}

	b.streamSize = config.StreamSize
	if b.streamSize <= 0 {
		b.streamSize = 10000 // Default to 10000 messages in the stream if not provided or invalid
	}

	b.retryIdleTime = config.RetryIdleTime
	if b.retryIdleTime <= 0 {
		b.retryIdleTime = 60 // Default to 60 seconds (1 minute) if not provided or invalid
	}

	err := b.redis.XGroupCreateMkStream(ctx, b.stream, b.group, "$")
	if err != nil && !errors.Is(err, redis.ErrorGroupAlreadyExists) {
		b.Error(ctx, "Failed to create group %s for stream %s: %v", b.group, b.stream, err)
		return err
	}
	return nil
}

// RegisterReceiver registers a new receiver with the bus
// If the receiver name is empty, it returns without any actions.
// If the receiver is already registered, it logs a warning and overwrites the existing receiver.
// If the receiver is registered successfully, it logs an info message.
// This method is useful for dynamically adding or updating receivers in the bus system.
// Receivers should be registered before starting the bus, but it is possible to register new receivers after
// starting the bus as well. However, it is recommended to register all necessary receivers before starting the bus
// to avoid potential issues with processing messages that require unregistered receivers.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - title: The title of the receiver to register.
//   - receiver: The ReceiverInterface implementation that defines the receiver's behavior.
func (b *Bus) RegisterReceiver(ctx context.Context, title string, receiver ReceiverFactory) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if title == "" {
		b.Error(ctx, "Receiver title cannot be empty")
		return
	}

	if b.receivers == nil {
		b.receivers = make(map[string]ReceiverFactory)
	}

	if _, exists := b.receivers[title]; exists {
		b.Warn(ctx, "Receiver %q is already registered, overwriting it", title)
	}

	b.receivers[title] = receiver
	b.Info(ctx, "Receiver %q registered successfully", title)
}

// GetReceiver retrieves a registered receiver by its title.
// It returns the ReceiverFactory and a boolean indicating whether the receiver was found.
// If the receivers map is nil or if the receiver with the specified title does not exist, it returns nil and false.
// This method is useful for retrieving a receiver when processing messages from the bus, allowing you to execute
// the appropriate logic based on the receiver's implementation.
//
// Parameters:
//   - title: The title of the receiver to retrieve.
//
// Returns:
//   - ReceiverFactory: The factory function for creating an instance of the receiver, or nil if not found.
//   - bool: A boolean indicating whether the receiver was found (true) or not (false).
func (b *Bus) GetReceiver(title string) (ReceiverFactory, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.receivers == nil {
		return nil, false
	}

	if receiver, exists := b.receivers[title]; exists {
		return receiver, true
	}

	return nil, false
}

// ReceiversCount returns the number of registered receivers in the bus.
// It acquires a read lock to safely access the receivers map and returns the count of registered receivers.
// If the receivers map is nil, it returns 0.
// This method is useful for monitoring and debugging purposes, allowing you to check how many receivers are currently registered in the bus.
//
// Returns:
//   - int: The number of registered receivers in the bus.
func (b *Bus) ReceiversCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.receivers == nil {
		return 0
	}

	return len(b.receivers)
}

// Start starts the bus by launching the specified number of worker goroutines.
// Each worker will continuously process tasks from the Redis stream.
// It waits for all workers to finish before returning.
// This method is useful for starting the bus and processing tasks concurrently.
// It uses a WaitGroup to ensure all workers complete before returning.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
func (b *Bus) Start(ctx context.Context) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	for i := 0; i < int(b.workersCount); i++ {
		wg.Go(func() {
			b.safeWorker(ctx)
		})
	}

	<-ctx.Done()
	b.Info(ctx, "Bus stopped, waiting for workers to finish")

	wg.Wait()
	b.Info(ctx, "All workers finished, bus stopped")
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
func (b *Bus) AddMessage(ctx context.Context, receiver string, payload any) (string, error) {
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
	}

	_, err = b.redis.XAdd(ctx, &re.XAddArgs{
		Stream: b.stream,
		Values: message,
		MaxLen: int64(b.streamSize), // Limit the stream size
	})
	if err != nil {
		b.Error(ctx, "b.redis.XAdd() error: %v", err)
		return "", err
	}

	b.Info(ctx, "Message %q added to stream %q with ID %q", receiver, b.stream, id)

	return id, nil
}

// safeWorker is a wrapper around the worker function that recovers from panics.
// It ensures that if a panic occurs within the worker, it is caught and logged,
// allowing the worker to continue processing other messages.
// This method is useful for improving the robustness of the worker by preventing crashes
// due to unexpected errors.
//
// Parameters:
//   - ctx: The context for the worker, used for cancellation and timeout.
func (b *Bus) safeWorker(ctx context.Context) {
	worker := uuid.New().String()
	ctx = context.WithValue(ctx, logging.CtxKeyUUID, worker)
	b.Info(ctx, "Worker %q was started", worker)

	for {
		select {
		case <-ctx.Done():
			b.Info(ctx, "Worker %q was stopped", worker)
			return
		default:
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.inFlight.Add(-1) // Decrement in-flight counter if a panic occurs
						b.Error(ctx, "[worker %q] panic recovered: %v \033[1m \033[31m%s\033[0m", worker, r, strings.ReplaceAll(string(debug.Stack()), "\n", " "))
					}
				}()

				b.workerJob(ctx, worker)
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
//   - worker: The unique identifier for the worker, used for logging and claiming messages.
func (b *Bus) workerJob(ctx context.Context, worker string) {
	claims, err := b.redis.XAutoClaim(ctx, &re.XAutoClaimArgs{
		Stream:   b.stream,
		Group:    b.group,
		Consumer: worker,
		MinIdle:  time.Duration(b.retryIdleTime) * time.Second, // Minimum idle time for auto claim
		Count:    1,
		Start:    "0-0",
	})

	if err != nil {
		b.Error(ctx, "Failed to auto claim message: %v", err)
		// Wait before retrying and check for context cancellation to avoid tight loop on errors
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
		return
	}

	if len(claims) == 1 {
		if err = b.processMessage(ctx, claims[0]); err != nil {
			b.Error(ctx, "Failed to execute receiver %q: %v", claims[0].Values["receiver"], err)
		}
		return
	}

	messages, err := b.redis.XReadGroup(ctx, &re.XReadGroupArgs{
		Group:    b.group,
		Consumer: worker,
		Streams:  []string{b.stream, ">"},
		Block:    1 * time.Second, // Block for 1 second until a new message arrives
		Count:    1,
	})
	if err != nil {
		b.Error(ctx, "Failed to read from stream %s: %v", b.stream, err)
		return
	}

	if len(messages) == 0 {
		return // No messages to process
	}

	if len(messages[0].Messages) == 0 {
		return
	}

	err = b.processMessage(ctx, messages[0].Messages[0])
	if err != nil {
		b.Error(ctx, "Failed to process message %q with id %q: %v", messages[0].Messages[0].Values["receiver"], messages[0].Messages[0].Values["id"], err)
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
//   - msg: The message containing the receiver data, which should include the receiver title and other necessary information.
//
// Returns:
//   - error: An error if the receiver fails to start or execute, otherwise nil.
func (b *Bus) processMessage(ctx context.Context, msg re.XMessage) error {
	receiverName, ok := msg.Values["receiver"].(string)
	if !ok {
		b.redis.XAck(ctx, b.stream, b.group, msg.ID)
		b.Error(ctx, "Message %q does not contain a valid receiver name", msg.ID)
		return ErrorInvalidReceiverName
	}

	b.mu.RLock()
	factory, ok := b.receivers[receiverName]
	b.mu.RUnlock()
	if !ok {
		b.redis.XAck(ctx, b.stream, b.group, msg.ID)
		return ErrorReceiverNotRegistered
	}

	retry := b.GetRetry(ctx, msg.ID)
	if retry >= int(b.numRetries) {
		b.Warn(ctx, "Message %q exceeded retry limit (%d), discarding", msg.ID, b.numRetries)
		b.DelRetry(ctx, msg.ID)
		b.redis.XAck(ctx, b.stream, b.group, msg.ID)
		return nil
	}
	retry++
	b.SetRetry(ctx, msg.ID, retry)

	var err error
	receiver := factory()
	b.inFlight.Add(1)
	receiver.SetBusContext(&b.inFlight, b.workersCount)
	ctx, err = receiver.Start(ctx, msg.Values)
	if err != nil {
		b.inFlight.Add(-1)
		b.Error(ctx, "Failed to start receiver %q: %v", msg.Values["receiver"], err)
		return err
	}

	err = receiver.Execute(ctx)
	if err != nil {
		b.Error(ctx, "Failed to execute receiver %q: %v", msg.Values["receiver"], err)
		receiver.Finish(ctx)
		b.inFlight.Add(-1)
		return err
	}
	receiver.Finish(ctx)
	b.inFlight.Add(-1)

	b.redis.XAck(ctx, b.stream, b.group, msg.ID)
	b.DelRetry(ctx, msg.ID)

	return nil
}

// SetRetry sets the retry count for a specific message key in Redis.
// It constructs a Redis key using the stream name and the provided key,
// and sets the retry count as the value.
// If setting the retry count fails, it logs an error.
// TTL for the retry count is set to three times the product of numRetries and retryIdleTime
// to ensure it persists long enough for all retries.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The unique key for the message whose retry count is to be set.
//   - num: The retry count to set for the specified message key.
func (b *Bus) SetRetry(ctx context.Context, key string, num int) {
	// Set the retry count in Redis with a TTL in a seconds. Time multiplied by 3 to ensure that all retries have enough time to be attempted before the retry count expires.
	err := b.redis.Set(ctx, fmt.Sprintf("%s:retry:%s", b.stream, key), strconv.Itoa(num), int(b.numRetries*b.retryIdleTime*3))
	if err != nil {
		b.Error(ctx, "Failed to set retry count for key %q: %v", key, err)
	}
}

// GetRetry retrieves the retry count for a specific message key from Redis.
// It constructs a Redis key using the stream name and the provided key,
// and retrieves the retry count value.
// If retrieving the retry count fails, it logs an error and returns retryFailed for end retrying.
// If the retrieved value cannot be converted to an integer, it also returns retryFailed.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The unique key for the message whose retry count is to be retrieved.
//
// Returns:
//   - int: The retry count for the specified message key, or retryFailed if an error occurs.
func (b *Bus) GetRetry(ctx context.Context, key string) int {
	val, err := b.redis.Get(ctx, fmt.Sprintf("%s:retry:%s", b.stream, key), "0")
	if err != nil {
		b.Error(ctx, "Failed to get retry count for key %q: %v", key, err)
		return retryFailed
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return retryFailed
	}

	return num
}

// DelRetry deletes the retry count for a specific message key from Redis.
// It constructs a Redis key using the stream name and the provided key,
// and deletes the corresponding entry from Redis.
// If deleting the retry count fails, it logs an error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The unique key for the message whose retry count is to be deleted.
func (b *Bus) DelRetry(ctx context.Context, key string) {
	err := b.redis.Del(ctx, fmt.Sprintf("%s:retry:%s", b.stream, key))
	if err != nil {
		b.Error(ctx, "Failed to delete retry count for key %q: %v", key, err)
	}
}

func Caller() string {
	pc, file, line, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc)
	return fmt.Sprintf("%s:%d->%s", filepath.Base(file), line, strings.TrimPrefix(fn.Name(), "github.com/ra-company/bus/"))
}
