package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
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
	ErrorEmptyReceiverTitle    = errors.New("receiver title cannot be empty")
	ErrorReceiverNotRegistered = errors.New("receiver not registered")
	ErrorInvalidReceiverName   = errors.New("invalid receiver name")
	ErrorEmptyTask             = errors.New("task cannot be empty")
	ErrorEmptyGroupName        = errors.New("group name cannot be empty")
	ErrorNilConfiguration      = errors.New("bus configuration cannot be nil")
	ErrorAlreadyInitialized    = errors.New("bus is already initialized")
	ErrorNotInitialized        = errors.New("bus is not initialized, call Init first")
	// ErrorStopTimeout is returned by Stop and Close when running receivers did not finish before the
	// context expired. Their messages were not acknowledged, so they stay pending and are redelivered.
	ErrorStopTimeout = errors.New("bus did not stop in time")
	// ErrorHandlerTimeout is the error of an execution that ran longer than BusConfiguration.HandlerTimeout
	// and then failed (one that returns nil late is a success). It is an ordinary failure: the message stays pending and is retried (or dead-lettered when its retries
	// are exhausted). It is also the cause of the cancelled context the receiver sees.
	ErrorHandlerTimeout = errors.New("receiver did not finish in time")
)

// Bus is a message bus on a Redis stream: producers publish messages with AddMessage, and the workers started by
// Start deliver them to the receivers registered for them, at least once, within one consumer group. Initialise it
// with Init; the zero value can only be stopped.
type Bus struct {
	logging.CustomLogger
	redis            *redis.RedisClient
	stream           string                     // Stream name for the bus
	group            string                     // Consumer group name
	workersCount     int64                      // Number of workers to process tasks (default: 10)
	numRetries       int64                      // Number of retries for failed tasks (default: 5)
	streamSize       int64                      // Maximum size of the stream (default: 10000)
	retryIdleTime    int64                      // Minimum idle time in seconds before retrying a message (default: 60 seconds)
	receivers        map[string]ReceiverFactory // Map of receivers by name
	mu               sync.RWMutex               // Mutex to protect access to the receivers map
	inFlight         atomic.Int64               // Counter for the number of messages currently being processed
	showUnregistered bool                       // Whether to show warnings for unregistered receivers
	unregistered     UnregisteredPolicy         // What to do with messages for unregistered receivers
	options          map[string]ReceiverOptions // Per-receiver options by name (guarded by mu)
	deadStream       string                     // Dead-letter stream name (default: <stream>:dead)
	deadSize         int64                      // Maximum number of entries in the dead-letter stream (default: 10000)
	deadRetention    time.Duration              // How long dead-letter entries are kept (default: 30 days)
	counters         counters                   // In-process counters, see Stats

	dedupTTL   time.Duration // How long a dedup key is remembered (default: 10 minutes)
	rebirthTTL time.Duration // How long the mark of a recreated stream is kept; 0 = no mark (see rebirth.go)

	handlerTimeout time.Duration // Longest run of one receiver; 0 = unlimited (default: 10 x RetryIdleTime)
	stuck          atomic.Int64  // Receivers running past their HandlerTimeout right now

	leaseEvery time.Duration // A running message is renewed when this long has passed since its start or last renewal
	leaseMu    sync.Mutex    // Guards leases
	leases     map[leaseKey]lease

	heartbeatEvery time.Duration // How often a running instance renews its workers in the registry
	aliveTTL       time.Duration // A worker not renewed for this long is dead
	purgeIdle      time.Duration // Silence after which a consumer record without pending messages is deleted

	statsTTL time.Duration       // How long daily counters stay in Redis; 0 = counters are off
	tag      string              // Hash tag of the stream's keys (see streamTag); empty = counters are off
	namesMu  sync.Mutex          // Guards names
	names    map[string]struct{} // Receiver names counted besides the registered ones, see statName

	claimEvery time.Duration // Minimum interval between two searches for idle messages while nothing is idle
	lastClaim  atomic.Int64  // Unix nanoseconds of the last search for idle messages

	logger logging.Logger // Logger set by SetLogger, applied to the Redis client created later by Init

	readBackoff time.Duration // Pause of a worker after a failed read or claim (default: 5 seconds)

	shutdownTimeout time.Duration // How long Start waits for running receivers after ctx is cancelled
	lifeOnce        sync.Once     // Creates stopCh, allDone and forced, see initLifecycle
	lc              sync.Mutex    // Guards stopped, execCancel and the workers WaitGroup counter
	initialized     atomic.Bool   // Init has succeeded; read without a lock on every AddMessage, written under lc
	initializing    bool          // Init is running; a concurrent Init returns ErrorAlreadyInitialized (guarded by lc)
	started         bool          // Start has launched the workers; a second Start does nothing
	stopped         bool          // Stop (or the end of Start) has begun, no new workers may start
	execCancel      context.CancelFunc
	stopOnce        sync.Once
	forceOnce       sync.Once
	closeOnce       sync.Once
	workers         sync.WaitGroup
	hbDone          chan struct{} // Closed when the heartbeat has removed the workers from the registry (nil before Start)
	stopCh          chan struct{} // Closed when the bus begins to stop: workers read no new messages
	allDone         chan struct{} // Closed when every worker has returned
	forced          chan struct{} // Closed when waiting for receivers was given up and their contexts cancelled
}

// BusConfiguration is the configuration passed to Init. Zero values mean the documented defaults.
type BusConfiguration struct {
	Redis        redis.Config // Configuration for Redis connection
	Stream       string       // Stream name for the bus
	Group        string       // Consumer group name
	WorkersCount int64        // Number of workers to process tasks
	NumRetries   int64        // Number of retries for failed tasks
	// StreamSize is the size the stream is trimmed to on every AddMessage (default 10000). The trimming is
	// approximate (XADD MAXLEN ~): Redis removes whole nodes, so the stream may hold up to about a hundred
	// entries more, and trimming stays cheap. The oldest entries go first, processed or not.
	StreamSize       int64
	RetryIdleTime    int64 // How long a worker may be silent before its message is retried (in seconds); a running receiver renews it, see lease.go
	ShowUnregistered bool  // Whether to show warnings for unregistered receivers

	// Unregistered decides what happens to a message whose receiver is not registered in this process.
	// Default UnregisteredAck: acknowledged and ignored, right for a group whose instances all run the same
	// code. During a rolling deploy that adds a receiver, old instances would drop its messages: use
	// UnregisteredLeave there. See UnregisteredPolicy.
	Unregistered UnregisteredPolicy

	// DeadLetterStream is the stream that receives messages the bus gave up on (retries exhausted
	// or a receiver returned Permanent). Default: "<Stream>:dead".
	DeadLetterStream string
	// DeadLetterSize is the maximum number of entries kept in the dead-letter stream. Default: 10000.
	DeadLetterSize int64
	// DeadLetterRetention is how long entries stay in the dead-letter stream, any positive duration
	// (for example 10*24*time.Hour). Entries older than that are trimmed when a new one is written.
	// Default: 30 days.
	DeadLetterRetention time.Duration

	// StreamRebirthTTL is how long the mark of a recreated stream is kept (see rebirth.go). When the stream key is
	// deleted and created anew, the groups that notice it within this time start at the beginning of the new
	// stream and lose nothing; one that notices later starts at its end. Keep it longer than the longest time a
	// service may be down. Default: 1 hour; a negative value switches the mark off.
	StreamRebirthTTL time.Duration

	// DedupTTL is how long a deduplication key passed to AddMessage is remembered, counted from the
	// first publication with it. It must cover the longest delay between duplicates; a shorter
	// time means fewer keys in Redis. Default: 10 minutes. It has no effect without a key.
	DedupTTL time.Duration

	// StatsRetention is how long the daily counters (published, received, processed, failed, dead-lettered,
	// ignored; see Counters) are kept in Redis. Default: 30 days. A negative value switches the counters
	// off: the bus then acknowledges with a plain XACK.
	StatsRetention time.Duration

	// ShutdownTimeout is how long Start waits for running receivers after its context is cancelled
	// (or SIGINT/SIGTERM arrives): no new messages are read, receivers keep running with an
	// uncancelled context, and when the time is up their context is cancelled and Start returns.
	// Zero (default) is a hard stop: the receivers' context is cancelled at once and Start
	// waits for them to return. Stop(ctx) does not depend on this setting.
	ShutdownTimeout time.Duration

	// HandlerTimeout is the longest time one receiver may run for one message. When it is up the receiver's
	// context is cancelled (cause ErrorHandlerTimeout). If the receiver then returns an error, the run fails with
	// ErrorHandlerTimeout and the message is retried like after any other failure; if it returns nil, the work
	// is done and the message is acknowledged, only late. The receiver runs in the worker's own goroutine, so there are never more running receivers
	// than WorkersCount. Go cannot kill a goroutine: a receiver that ignores its context keeps its worker
	// until it returns (see Stats.Stuck), instead of leaving a goroutine and its connections behind.
	//
	// While a receiver runs, the bus keeps the message from being redelivered to another worker (see lease.go),
	// however long it takes, so the limit is independent of RetryIdleTime. When the limit is over, the lease is
	// not renewed any more and the message is redelivered RetryIdleTime later at the latest. Zero (default)
	// means 10 x RetryIdleTime, a negative value switches the limit off: then a receiver that hangs and ignores
	// its context holds its worker and its message for ever.
	HandlerTimeout time.Duration
}

// UnregisteredPolicy is what the bus does with a message whose receiver is not registered in this process,
// see BusConfiguration.Unregistered.
type UnregisteredPolicy int

const (
	// UnregisteredAck acknowledges the message and ignores it (default). It suits a group whose instances all
	// handle the same receivers. A message for a receiver that only a newer version of the service has is lost
	// for the group if an older instance reads it.
	UnregisteredAck UnregisteredPolicy = iota
	// UnregisteredLeave leaves the message pending without executing it. After RetryIdleTime it is redelivered,
	// possibly to an instance that has the receiver (a newer version during a rolling deploy). If nobody handles it
	// within NumRetries deliveries it goes to the dead-letter stream with reason DeadReasonUnregistered.
	// Nothing is lost, at the cost of a delay of up to RetryIdleTime per delivery.
	UnregisteredLeave
	// UnregisteredDeadLetter moves the message to the dead-letter stream at once (reason DeadReasonUnregistered).
	UnregisteredDeadLetter
)

// String returns the name of the policy.
func (p UnregisteredPolicy) String() string {
	switch p {
	case UnregisteredAck:
		return "ack"
	case UnregisteredLeave:
		return "leave"
	case UnregisteredDeadLetter:
		return "dead-letter"
	}
	return fmt.Sprintf("UnregisteredPolicy(%d)", int(p))
}

// ReceiverOptions are optional per-receiver settings, see RegisterReceiverWithOptions.
type ReceiverOptions struct {
	// NumRetries is the maximum number of executions of one message by this receiver. Zero or a
	// negative value means the group-wide BusConfiguration.NumRetries.
	NumRetries int64
}

// SetLogger allows setting a custom logger for the bus.
// This is useful when you want to use a different logging mechanism
// instead of the default one provided by the package.
//
// Parameters:
//   - logger: An instance of a type that implements the logging.Logger interface.
//
// It may be called before Init: the logger is then applied to the Redis client when Init creates it.
func (b *Bus) SetLogger(logger logging.Logger) {
	b.logger = logger
	b.CustomLogger.SetLogger(logger)
	if b.redis != nil {
		b.redis.SetLogger(logger)
	}
}

// initLifecycle creates the channels that drive stopping. It runs once, from Init or from the first call
// of Start, Stop or Close, so that a bus that was never initialised can still be stopped without a panic.
func (b *Bus) initLifecycle() {
	b.lifeOnce.Do(func() {
		b.stopCh = make(chan struct{})
		b.allDone = make(chan struct{})
		b.forced = make(chan struct{})
	})
}

// defaultReadBackoff is how long a worker pauses after a failed read or claim, so that an unreachable Redis
// does not turn the workers into a busy loop.
const defaultReadBackoff = 5 * time.Second

// pause waits for the read backoff, or less if the bus is stopping.
func (b *Bus) pause(ctx context.Context) {
	d := b.readBackoff
	if d <= 0 {
		d = defaultReadBackoff
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-b.stopCh:
	case <-ctx.Done():
	}
}

// Init initializes the bus with the provided configuration.
// It sets up the Redis client, stream, group, and workers count.
// If the stream or group is not provided, it defaults to "default-stream" and "default-group".
// It also creates the Redis stream group if it does not already exist.
// Init is called once: a second call after a successful one, or while another Init runs, returns
// ErrorAlreadyInitialized and changes nothing. After a failed Init (for example Redis is unreachable) its Redis
// connection is closed and it can be called again. Receivers registered before Init are kept.
//
// Parameters:
//   - ctx: The context for the operation.
//   - config: The configuration for the bus, including Redis connection details, stream name, group name, workers count, and number of retries.
func (b *Bus) Init(ctx context.Context, config *BusConfiguration) (err error) {
	if config == nil {
		return ErrorNilConfiguration
	}
	// The check and the claim are one step, so two concurrent calls cannot both initialise the bus.
	b.lc.Lock()
	if b.initialized.Load() || b.initializing {
		b.lc.Unlock()
		// A second Init would replace the Redis client and the configuration under running code.
		return ErrorAlreadyInitialized
	}
	b.initializing = true
	b.lc.Unlock()
	defer func() {
		if err != nil {
			// A later Init creates a new connection: do not leave this one open.
			err = errors.Join(err, b.closeRedis())
		}
		b.lc.Lock()
		b.initializing = false
		b.initialized.Store(err == nil)
		b.lc.Unlock()
	}()

	b.mu.Lock()
	if b.receivers == nil { // RegisterReceiver may have been called before Init
		b.receivers = make(map[string]ReceiverFactory)
	}
	if b.options == nil {
		b.options = make(map[string]ReceiverOptions)
	}
	b.mu.Unlock()
	b.redis = &redis.RedisClient{}
	if b.logger != nil {
		b.redis.SetLogger(b.logger)
	}
	b.redis.Start(ctx, &config.Redis)
	b.showUnregistered = config.ShowUnregistered
	switch config.Unregistered {
	case UnregisteredAck, UnregisteredLeave, UnregisteredDeadLetter:
		b.unregistered = config.Unregistered
	default:
		b.Warnf(ctx, "Unknown policy for unregistered receivers %v, using %v", config.Unregistered, UnregisteredAck)
		b.unregistered = UnregisteredAck
	}

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
		b.Warnf(ctx, "Workers count is too high (%d), setting to 100", b.workersCount)
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

	switch retryIdle := time.Duration(b.retryIdleTime) * time.Second; {
	case config.HandlerTimeout < 0:
	case config.HandlerTimeout == 0:
		b.handlerTimeout = retryIdle * 10
	default:
		b.handlerTimeout = config.HandlerTimeout
	}
	b.leaseEvery = time.Duration(b.retryIdleTime) * time.Second / 3

	b.deadStream = config.DeadLetterStream
	if b.deadStream == "" {
		b.deadStream = b.stream + ":dead"
	}
	b.deadSize = config.DeadLetterSize
	if b.deadSize <= 0 {
		b.deadSize = defaultDeadLetterSize
	}
	b.deadRetention = config.DeadLetterRetention
	if b.deadRetention <= 0 {
		b.deadRetention = defaultDeadLetterRetention
	}

	if tag, err := b.streamTag(); err == nil {
		b.tag = tag
		switch {
		case config.StatsRetention < 0:
		case config.StatsRetention == 0:
			b.statsTTL = defaultStatsRetention
		default:
			b.statsTTL = config.StatsRetention
		}
	} else if config.StatsRetention >= 0 {
		b.Warnf(ctx, "Counters are off: stream name %q does not allow keys in the stream's hash slot", b.stream)
	}
	switch {
	case config.StreamRebirthTTL < 0:
	case config.StreamRebirthTTL == 0:
		b.rebirthTTL = defaultStreamRebirthTTL
	default:
		b.rebirthTTL = config.StreamRebirthTTL
	}
	b.dedupTTL = config.DedupTTL
	if b.dedupTTL <= 0 {
		b.dedupTTL = defaultDedupTTL
	}
	if config.ShutdownTimeout > 0 {
		b.shutdownTimeout = config.ShutdownTimeout
	}
	// Idle messages are looked for at most this often when the last look found none (see workerJob).
	b.claimEvery = min(time.Duration(b.retryIdleTime)*time.Second/4, time.Second)

	b.heartbeatEvery = defaultHeartbeatEvery
	b.aliveTTL = defaultAliveTTL
	b.purgeIdle = defaultPurgeIdle

	b.initLifecycle()

	// The group starts at the end of the stream, or at its beginning when the stream is new or was recreated
	// recently (see rebirth.go).
	if _, _, err = b.createGroup(ctx, b.group, "$"); err != nil {
		return err
	}
	return nil
}

// ready returns ErrorNotInitialized unless Init has succeeded. Every public method that talks to Redis checks it,
// so a bus used before Init (or after a failed one) returns an error instead of panicking on a nil client.
func (b *Bus) ready() error {
	if !b.initialized.Load() {
		return ErrorNotInitialized
	}
	return nil
}

// closeRedis closes the Redis connection of the bus. It does nothing without one. The client is kept, so a
// call after it fails with a closed-connection error instead of a nil pointer.
func (b *Bus) closeRedis() error {
	if b.redis == nil {
		return nil
	}
	closer, ok := b.redis.Client().(io.Closer)
	if !ok {
		return nil
	}
	// A connection closed already (by a failed Init) is not an error: a cluster client does not report it
	// either, so Close behaves the same on a single node and on a cluster.
	if err := closer.Close(); err != nil && !errors.Is(err, re.ErrClosed) {
		return fmt.Errorf("close Redis connection: %w", err)
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
	b.RegisterReceiverWithOptions(ctx, title, receiver, ReceiverOptions{})
}

// RegisterReceiverWithOptions registers a receiver like RegisterReceiver and applies per-receiver
// options, for example its own limit of executions (ReceiverOptions.NumRetries) instead of the
// group-wide one. Registering the same title again replaces both the factory and the options.
func (b *Bus) RegisterReceiverWithOptions(ctx context.Context, title string, receiver ReceiverFactory, opts ReceiverOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if title == "" {
		b.Error(ctx, "Receiver title cannot be empty")
		return
	}

	if b.receivers == nil {
		b.receivers = make(map[string]ReceiverFactory)
	}
	if b.options == nil {
		b.options = make(map[string]ReceiverOptions)
	}

	if _, exists := b.receivers[title]; exists {
		b.Warnf(ctx, "Receiver %q is already registered, overwriting it", title)
	}

	b.receivers[title] = receiver
	b.options[title] = opts
	b.Infof(ctx, "Receiver %q registered successfully", title)
}

// Subscribe declares that this service handles messages for the receiver title. It is a synonym
// of RegisterReceiver that reads better in services which are consumers of events.
//
// Messages for receivers the service has not subscribed to are not an error: the bus
// acknowledges and ignores them, because a consumer group sees the whole stream.
func (b *Bus) Subscribe(ctx context.Context, title string, receiver ReceiverFactory) {
	b.RegisterReceiver(ctx, title, receiver)
}

// EnsureGroups creates the given consumer groups on the stream if they do not exist yet, creating
// the stream too. A group is created at position "0", so it receives every message that is in the
// stream, including messages a producer added before the consuming service started for the first
// time. Existing groups are left untouched. Call it on the producing side for every service that
// must not miss events; Init creates only the bus' own group, at the end of the stream ("$"). A stream it
// creates is marked as new, see rebirth.go.
func (b *Bus) EnsureGroups(ctx context.Context, groups ...string) error {
	if err := b.ready(); err != nil {
		return err
	}
	for _, group := range groups {
		if group == "" {
			return ErrorEmptyGroupName
		}
		if _, _, err := b.createGroup(ctx, group, "0"); err != nil {
			return fmt.Errorf("ensure group %q: %w", group, err)
		}
	}
	return nil
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
// It returns after the bus has stopped: when ctx is cancelled, SIGINT or SIGTERM arrives, or Stop
// is called (Start then returns once Stop has finished waiting).
//
// When ctx is cancelled, the workers stop reading new messages. With BusConfiguration.ShutdownTimeout
// set, receivers that are already running get that long to finish with an uncancelled context;
// otherwise (the default) their context is cancelled at once. Either way a message whose receiver did
// not succeed is not acknowledged and stays pending. Start can be used only once: after Stop, while the bus
// is already running, or without a successful Init it returns immediately (the last case is logged as an error).
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
func (b *Bus) Start(ctx context.Context) {
	b.initLifecycle()
	if err := b.ready(); err != nil {
		b.Errorf(ctx, "Bus cannot start: %v", err)
		return
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Receivers and reads run with a context that parent cancellation does not touch: a shutdown
	// must not abort a receiver that is allowed to finish. It is cancelled by cancelExec.
	execCtx, execCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer execCancel()

	b.lc.Lock()
	if b.stopped {
		b.lc.Unlock()
		b.Info(ctx, "Bus is already stopped, nothing to start")
		return
	}
	if b.started {
		b.lc.Unlock()
		b.Warn(ctx, "Bus is already started, the second Start does nothing")
		return
	}
	b.started = true
	b.execCancel = execCancel
	b.workers.Add(int(b.workersCount))
	heartbeatDone := make(chan struct{}) // closed when the heartbeat and the lease renewal have finished
	b.hbDone = heartbeatDone
	b.lc.Unlock()

	workers := make([]string, b.workersCount)
	for i := range workers {
		workers[i] = uuid.New().String()
		go func() {
			defer b.workers.Done()
			b.safeWorker(execCtx, workers[i])
		}()
	}
	hbDone, leaseDone := make(chan struct{}), make(chan struct{})
	go b.heartbeat(execCtx, newInstanceName(), workers, hbDone)
	go b.renewLeases(leaseDone)
	go func() {
		<-hbDone
		<-leaseDone
		close(heartbeatDone)
	}()

	select {
	case <-ctx.Done():
		// Give the signals back: a second SIGINT/SIGTERM during the graceful wait now kills the process.
		stop()
		b.Info(ctx, "Bus is stopping, waiting for workers to finish")
		if b.shutdownTimeout > 0 {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.shutdownTimeout)
			if err := b.Stop(stopCtx); err != nil {
				b.Errorf(ctx, "Bus stopped with unfinished receivers: %v", err)
			}
			cancel()
		} else {
			b.beginStop()
			b.forceStop()
			<-b.allDone
		}
	case <-b.stopCh:
		// Stop has been called and waits for the workers itself.
		select {
		case <-b.allDone:
		case <-b.forced:
		}
	}
	// The heartbeat removes this instance's workers from the registry once they are gone.
	select {
	case <-heartbeatDone:
	case <-time.After(ackTimeout):
	}
	b.Info(ctx, "Bus stopped")
}

// beginStop makes workers stop reading new messages; running receivers are not touched.
func (b *Bus) beginStop() {
	b.initLifecycle()
	b.stopOnce.Do(func() {
		b.lc.Lock()
		b.stopped = true
		b.lc.Unlock()
		close(b.stopCh)
		go func() {
			b.workers.Wait()
			close(b.allDone)
		}()
	})
}

// forceStop cancels the context of running receivers and the reads of the workers.
func (b *Bus) forceStop() {
	b.initLifecycle()
	b.forceOnce.Do(func() {
		b.lc.Lock()
		cancel := b.execCancel
		b.lc.Unlock()
		if cancel != nil {
			cancel()
		}
		close(b.forced)
	})
}

// Stop stops the bus gracefully. Workers stop reading new messages at once (a read that is already
// blocked on an empty stream returns within a second), and Stop waits until the messages being processed
// are finished and acknowledged. Messages that were not read stay in the stream for the next run, and
// a message whose receiver did not succeed is not acknowledged and stays pending.
//
// If ctx expires first, the context of the running receivers is cancelled so that they can abort,
// and Stop returns an error wrapping ErrorStopTimeout and ctx.Err() without waiting further: a receiver
// that ignores its context keeps running in the background, and its message stays pending until it is
// acknowledged by the receiver's own success or redelivered after RetryIdleTime.
//
// Stop is safe to call several times and concurrently, also before Start (Start then does nothing)
// or without Start. The bus cannot be started again after Stop.
func (b *Bus) Stop(ctx context.Context) error {
	b.beginStop()

	select {
	case <-b.allDone:
		b.waitHeartbeat(ctx)
		return nil
	case <-ctx.Done():
	}

	// A worker that finished while ctx expired is not a timeout.
	select {
	case <-b.allDone:
		b.waitHeartbeat(ctx)
		return nil
	default:
	}

	b.forceStop()
	inFlight := b.inFlight.Load()
	b.Errorf(ctx, "Stop timed out with %d message(s) still being processed, they stay pending", inFlight)
	return fmt.Errorf("%w: %d message(s) still being processed, left pending: %w", ErrorStopTimeout, inFlight, ctx.Err())
}

// waitHeartbeat waits until the heartbeat has removed this instance's workers from the registry of live
// consumers, for at most ackTimeout and not beyond ctx. Without Start there is nothing to wait for.
func (b *Bus) waitHeartbeat(ctx context.Context) {
	b.lc.Lock()
	done := b.hbDone
	b.lc.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
		return
	default:
	}
	timer := time.NewTimer(ackTimeout + time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

// Close stops the bus like Stop and then closes the Redis connection. After Close the bus cannot be
// used. The Redis connection is closed even if Stop timed out; the error of Stop is returned.
func (b *Bus) Close(ctx context.Context) error {
	err := b.Stop(ctx)
	// The registry is written through the connection that is closed next. If Stop has already used up ctx, the
	// instance still gets a second to leave it.
	wctx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		wctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
	}
	b.waitHeartbeat(wctx)
	b.closeOnce.Do(func() {
		if cerr := b.closeRedis(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	})
	return err
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
//   - dedupKey: Deduplication key; empty means none, and AddMessage then adds the message unconditionally. While the key is
//     remembered (BusConfiguration.DedupTTL, 10 minutes by default), a second message with the same key and
//     receiver is not added: AddMessage returns the id of the first message and a nil error, and
//     Stats().Deduplicated grows. The key is chosen by the producer, for example a hash of the raw body of a
//     webhook. The check and the XADD are one atomic Redis script, so it also works between hosts.
//
// Returns:
//   - string: The id of the message (of the first one for a duplicate).
//   - error: An error if the receiver title is empty or if adding the receiver to the stream fails.
func (b *Bus) AddMessage(ctx context.Context, receiver string, payload any, dedupKey string) (string, error) {
	if err := b.ready(); err != nil {
		return "", err
	}
	if receiver == "" {
		return "", ErrorEmptyReceiverTitle
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	id := uuid.New().String()
	stamp := time.Now().UTC().Format(time.RFC3339Nano) // Current time in RFC3339Nano format

	if b.tag == "" && dedupKey == "" {
		// No key can share the stream's slot: no counters and no mark of a recreated stream, a plain XADD.
		_, err = b.redis.XAdd(ctx, &re.XAddArgs{
			Stream: b.stream,
			Values: map[string]any{
				"receiver": receiver,     // Receiver title
				"id":       id,           // Unique ID for the message
				"payload":  string(data), // Serialized payload
				"time":     stamp,
			},
			MaxLen: b.streamSize, // Limit the stream size, see BusConfiguration.StreamSize
			Approx: true,
		})
	} else {
		// XADD, the deduplication check, the "published" counters and the mark of a recreated stream in one call.
		var added bool
		var first string
		first, added, err = b.publish(ctx, receiver, string(data), id, stamp, dedupKey)
		if err == nil && !added {
			b.counters.deduplicated.Add(1)
			b.Infof(ctx, "Message %q is a duplicate in stream %q, the first one has ID %q", receiver, b.stream, first)
			return first, nil
		}
	}
	if err != nil {
		return "", fmt.Errorf("add message %q to stream %q: %w", receiver, b.stream, err)
	}

	b.Infof(ctx, "Message %q added to stream %q with ID %q", receiver, b.stream, id)

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
func (b *Bus) safeWorker(ctx context.Context, worker string) {
	ctx = context.WithValue(ctx, logging.CtxKeyUUID, worker)
	ctx = context.WithValue(ctx, workerKey{}, worker)
	b.Infof(ctx, "Worker %q was started", worker)

	for {
		select {
		case <-b.stopCh:
			b.Infof(ctx, "Worker %q was stopped", worker)
			return
		case <-ctx.Done():
			b.Infof(ctx, "Worker %q was stopped", worker)
			return
		default:
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.Errorf(ctx, "[worker %q] panic recovered: %v \033[1m \033[31m%s\033[0m", worker, r, strings.ReplaceAll(string(debug.Stack()), "\n", " "))
						b.pause(ctx) // a panic that repeats on every step must not become a busy loop
					}
				}()

				b.workerJob(ctx, worker)
			}()
		}
	}
}

// workerJob is a goroutine body that processes one message from the Redis stream per call.
// It auto-claims a message that has been idle for the minimum time and executes it, else reads the next
// new message of the group. If a message fails to execute, it is retried based on the configured retry logic.
//
// Cost per message on the hot path: one XREADGROUP and one XACK. A message that was just read has been
// delivered exactly once, so its delivery counter is not requested from Redis. The search for idle messages is not run
// before every read: after a search that found nothing the next one is made at most every
// min(RetryIdleTime/4, 1s) by any worker, and while it keeps finding idle messages it runs back to back
// (see claimIdle for how they are found).
//
// Parameters:
//   - ctx: The context for the worker, used for cancellation and timeout.
//   - worker: The unique identifier for the worker, used for logging and claiming messages.
func (b *Bus) workerJob(ctx context.Context, worker string) {
	if b.claimDue() {
		msg, deliveries, found, err := b.claimIdle(ctx, worker)
		if err != nil {
			b.Errorf(ctx, "Failed to claim an idle message: %v", err)
			if isNoGroup(err) {
				b.recreateGroup(ctx)
			}
			b.pause(ctx) // avoid a tight loop on errors
			return
		}

		if found {
			b.lastClaim.Store(0) // there may be more idle messages: look again at once
			if b.abandoned(ctx, msg) {
				return
			}
			b.logProcessError(ctx, msg, b.handle(ctx, msg, deliveries))
			return
		}
	}

	messages, err := b.redis.XReadGroup(ctx, &re.XReadGroupArgs{
		Group:    b.group,
		Consumer: worker,
		Streams:  []string{b.stream, ">"},
		Block:    1 * time.Second, // Block for 1 second until a new message arrives
		Count:    1,
	})
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, re.Nil) {
			return // stopping, or nothing arrived
		}
		b.Errorf(ctx, "Failed to read from stream %s: %v", b.stream, err)
		if isNoGroup(err) {
			b.recreateGroup(ctx)
		}
		b.pause(ctx) // an unreachable Redis must not turn the workers into a busy loop
		return
	}

	if len(messages) == 0 {
		return // No messages to process
	}

	if len(messages[0].Messages) == 0 {
		return
	}

	if b.abandoned(ctx, messages[0].Messages[0]) {
		return
	}

	// XREADGROUP ">" delivers an entry for the first time, so its delivery counter is 1.
	b.logProcessError(ctx, messages[0].Messages[0], b.handle(ctx, messages[0].Messages[0], 1))
}

// abandoned reports whether the bus has given up on running receivers (Stop timed out or the shutdown
// timeout passed). A read that was already blocked when that happened (go-redis does not interrupt a
// blocked read when its context is cancelled) may still bring a message; it is not executed and stays
// pending, so it is redelivered after RetryIdleTime. Without it, a message read while Stop is
// waiting gracefully is processed and Stop waits for it.
func (b *Bus) abandoned(ctx context.Context, msg re.XMessage) bool {
	select {
	case <-b.forced:
		b.Warnf(ctx, "Bus is stopped, message %q (stream id %q) is left pending", msg.Values["id"], msg.ID)
		return true
	default:
		return false
	}
}

// claimCandidates is how many idle pending entries one search asks for. Several, because a candidate
// can be taken by another worker between the search and the claim.
const claimCandidates = 10

// claimIdle takes one message that has been pending without acknowledgement for at least RetryIdleTime
// and returns it with its delivery counter including this delivery.
//
// It does not use XAUTOCLAIM: that command looks only at the first Count*10 entries of the pending list
// (ordered by entry id, not by delivery time), so an idle message behind ten recently redelivered ones
// is not found until those are gone. XPENDING with IDLE lets Redis pick the idle entries from the whole
// list, and XCLAIM with MINIDLE takes one atomically: if another worker has taken it meanwhile, its idle time
// has been reset and the claim returns nothing, so the next candidate is tried. XCLAIM adds one to the
// delivery counter that XPENDING reported, which saves a separate read of the counter.
// found is false when there is nothing to redeliver.
func (b *Bus) claimIdle(ctx context.Context, worker string) (msg re.XMessage, deliveries int64, found bool, err error) {
	minIdle := time.Duration(b.retryIdleTime) * time.Second
	client := b.redis.Client()

	candidates, err := client.XPendingExt(ctx, &re.XPendingExtArgs{
		Stream: b.stream,
		Group:  b.group,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  claimCandidates,
	}).Result()
	if err != nil {
		if errors.Is(err, re.Nil) {
			return msg, 0, false, nil
		}
		return msg, 0, false, err
	}

	for _, c := range candidates {
		claimed, err := client.XClaim(ctx, &re.XClaimArgs{
			Stream:   b.stream,
			Group:    b.group,
			Consumer: worker,
			MinIdle:  minIdle,
			Messages: []string{c.ID},
		}).Result()
		if err != nil {
			return msg, 0, false, err
		}
		// Nothing is returned when another worker was faster, or when the entry was trimmed from the
		// stream (Redis 7 then drops it from the pending list as well).
		if len(claimed) == 1 {
			return claimed[0], c.RetryCount + 1, true, nil
		}
	}
	return msg, 0, false, nil
}

// claimDue reports whether this worker should look for idle messages now. Among concurrent workers
// only one wins a slot per interval.
func (b *Bus) claimDue() bool {
	now := time.Now().UnixNano()
	last := b.lastClaim.Load()
	return now-last >= int64(b.claimEvery) && b.lastClaim.CompareAndSwap(last, now)
}

// logProcessError is the one place where a failed message is logged, with its stream id and message id; the
// code below returns errors instead of logging them. A message for an unregistered receiver is normal and is
// not reported, unless something failed with it (for example its acknowledgement), which wraps the sentinel.
func (b *Bus) logProcessError(ctx context.Context, msg re.XMessage, err error) {
	if err == nil || err == ErrorReceiverNotRegistered { //nolint:errorlint // identity on purpose: a wrapped one carries a failure
		return
	}
	b.Errorf(ctx, "Failed to process message %q (stream id %q, message id %q): %v", msg.Values["receiver"], msg.ID, msg.Values["id"], err)
}

// handle processes a message by starting the associated receiver, executing it, and finishing it.
// knownDeliveries is the delivery counter when the caller knows it (the worker passes 1 for a message it has
// just read with XREADGROUP, and the counter from XCLAIM for a claimed one); 0 means it is read from Redis.
//
// Delivery is "at least once": the message is acknowledged only after the receiver has succeeded.
// If the receiver fails (or panics), the message stays pending in the consumer group and is
// redelivered (XPENDING IDLE + XCLAIM) after RetryIdleTime. The number of attempts is the delivery counter
// Redis keeps for every pending entry (XPENDING), so it is atomic, survives restarts of the
// workers and disappears together with the entry on XACK. A message that has been delivered more
// than the allowed number of times (ReceiverOptions.NumRetries of the receiver, else NumRetries of
// the bus) is moved to the dead-letter stream and acknowledged without being executed again.
// A receiver that returns an error marked with Permanent is not retried either: its message goes
// to the dead-letter stream at once.
//
// If the delivery counter cannot be read (for example Redis is briefly unavailable), the message is
// neither executed nor acknowledged: an error is returned and the message stays pending, so it is
// redelivered later instead of being lost. The same holds when the dead-letter write fails.
//
// It returns an error if the receiver fails to start or execute, or if the message could not be
// acknowledged, moved to the dead-letter stream or its delivery counter could not be read.
func (b *Bus) handle(ctx context.Context, msg re.XMessage, knownDeliveries int64) error {
	receiverName, ok := msg.Values["receiver"].(string)
	if !ok {
		if _, err := b.ack(ctx, msg, kindInvalid); err != nil {
			return fmt.Errorf("%w: %w", ErrorInvalidReceiverName, err)
		}
		return ErrorInvalidReceiverName
	}

	b.mu.RLock()
	factory, ok := b.receivers[receiverName]
	opts := b.options[receiverName]
	b.mu.RUnlock()
	if !ok {
		return b.handleUnregistered(ctx, msg, receiverName, knownDeliveries)
	}

	deliveries, pending, err := b.deliveries(ctx, msg, knownDeliveries)
	if err != nil || !pending {
		return err
	}

	limit := b.numRetries
	if opts.NumRetries > 0 {
		limit = opts.NumRetries
	}
	if deliveries > limit {
		b.Warnf(ctx, "Message %q exceeded retry limit (%d deliveries, limit %d), moving it to the dead-letter stream", msg.ID, deliveries, limit)
		return b.discard(ctx, msg, DeadReasonRetriesExhausted,
			fmt.Sprintf("retry limit exceeded: delivered %d times, limit %d", deliveries, limit), deliveries)
	}
	if deliveries > 1 {
		b.counters.retried.Add(1)
	}

	endLease := b.leaseStart(ctx, msg.ID)
	err = b.executeLimited(ctx, factory(), msg)
	endLease()
	if err != nil {
		b.counters.failed.Add(1)

		if IsPermanent(err) {
			// The error itself is logged by the worker (logProcessError); here only what happens to the message.
			b.Warnf(ctx, "Receiver %q failed permanently, message %q is moved to the dead-letter stream", receiverName, msg.ID)
			// The failed execution is counted together with the move to the dead-letter stream.
			if derr := b.discard(ctx, msg, DeadReasonPermanent, err.Error(), deliveries, retriedIf(deliveries, kindFailed)...); derr != nil {
				return errors.Join(err, derr)
			}
		} else {
			b.recordFailure(ctx, msg, retriedIf(deliveries, kindFailed)...)
		}
		return err
	}

	acked, err := b.ack(ctx, msg, retriedIf(deliveries, kindProcessed)...)
	if err != nil {
		return err
	}
	if !acked {
		// Another run of the message (redelivered after HandlerTimeout, or an at-least-once duplicate) has
		// acknowledged it already: this success is not counted again.
		b.counters.duplicates.Add(1)
		b.Infof(ctx, "Message %q had been acknowledged by another run already", msg.ID)
		return nil
	}
	b.counters.processed.Add(1)

	return nil
}

// deliveries returns the delivery counter of msg: knownDeliveries when positive, else read from Redis.
// pending is false (with a nil error) when the message is no longer pending; a read failure is returned as
// an error, and the message is then left pending for redelivery.
func (b *Bus) deliveries(ctx context.Context, msg re.XMessage, knownDeliveries int64) (deliveries int64, pending bool, err error) {
	if knownDeliveries > 0 {
		return knownDeliveries, true, nil
	}
	deliveries, pending, err = b.deliveryCount(ctx, msg.ID)
	if err != nil {
		return 0, false, fmt.Errorf("read delivery count of message %s, it stays pending for redelivery: %w", msg.ID, err)
	}
	if !pending {
		// Somebody else has already acknowledged (or the stream has trimmed) this message.
		b.Warnf(ctx, "Message %q is no longer pending, skipping", msg.ID)
	}
	return deliveries, pending, nil
}

// handleUnregistered applies the UnregisteredPolicy to a message whose receiver is not registered here. It
// returns ErrorReceiverNotRegistered (not logged as an error by the worker) unless something failed in Redis.
func (b *Bus) handleUnregistered(ctx context.Context, msg re.XMessage, receiverName string, knownDeliveries int64) error {
	if b.showUnregistered {
		b.Warnf(ctx, "Receiver %q is not registered, policy %v", receiverName, b.unregistered)
	}

	switch b.unregistered {
	case UnregisteredLeave:
		deliveries, pending, err := b.deliveries(ctx, msg, knownDeliveries)
		if err != nil {
			return err
		}
		if !pending {
			return ErrorReceiverNotRegistered
		}
		if deliveries > b.numRetries {
			b.Warnf(ctx, "Message %q for unregistered receiver %q was not handled in %d deliveries, moving it to the dead-letter stream", msg.ID, receiverName, deliveries)
			if err := b.discard(ctx, msg, DeadReasonUnregistered,
				fmt.Sprintf("receiver %q is not registered in any instance that read it (%d deliveries)", receiverName, deliveries),
				deliveries, kindUnregistered); err != nil {
				return err
			}
			return ErrorReceiverNotRegistered
		}
		// Not acknowledged: after RetryIdleTime another instance (one that has the receiver) may take it.
		b.recordFailure(ctx, msg, kindUnregistered)
		return ErrorReceiverNotRegistered

	case UnregisteredDeadLetter:
		deliveries, pending, err := b.deliveries(ctx, msg, knownDeliveries)
		if err != nil {
			return err
		}
		if !pending {
			return ErrorReceiverNotRegistered
		}
		if err := b.discard(ctx, msg, DeadReasonUnregistered,
			fmt.Sprintf("receiver %q is not registered", receiverName), deliveries, kindUnregistered); err != nil {
			return err
		}
		return ErrorReceiverNotRegistered

	default: // UnregisteredAck
		// A consumer group sees the whole stream, so events this service has not subscribed to
		// are normal: they are acknowledged and ignored.
		if _, err := b.ack(ctx, msg, kindUnregistered); err != nil {
			return fmt.Errorf("%w: %w", ErrorReceiverNotRegistered, err)
		}
		return ErrorReceiverNotRegistered
	}
}

// executeLimited is execute with the HandlerTimeout. The receiver runs in the worker's own goroutine, so the
// number of receivers running at once never exceeds WorkersCount, whatever they do. When the time is up its
// context is cancelled (cause ErrorHandlerTimeout) and the run is counted as timed out at once; a run that
// has exceeded the limit and returns an error fails with ErrorHandlerTimeout and is retried; one that returns nil
// late has done its work, so the message is acknowledged. A receiver that ignores its context cannot be killed in Go, so it keeps its
// worker until it returns; Stats.Stuck shows how many such runs there are right now.
// If ctx itself is cancelled (shutdown), it is not a timeout.
func (b *Bus) executeLimited(ctx context.Context, receiver ReceiverInterface, msg re.XMessage) error {
	if b.handlerTimeout <= 0 {
		return b.execute(ctx, receiver, msg)
	}

	tctx, cancel := context.WithTimeoutCause(ctx, b.handlerTimeout, ErrorHandlerTimeout)
	defer cancel()

	fired := make(chan struct{}) // closed when the deadline has been handled
	stop := context.AfterFunc(tctx, func() {
		defer close(fired)
		if errors.Is(context.Cause(tctx), ErrorHandlerTimeout) {
			b.counters.timedOut.Add(1)
			b.stuck.Add(1)
			b.leaseEnd(workerOf(ctx), msg.ID) // a receiver past its limit does not keep the message from being redelivered
			// A warning: the failure itself is logged once, when the receiver returns.
			b.Warnf(ctx, "Receiver %q did not finish message %q within %s, its context is cancelled", msg.Values["receiver"], msg.ID, b.handlerTimeout)
		}
	})

	err := b.execute(tctx, receiver, msg)

	if stop() { // the deadline did not come before the receiver returned
		return err
	}
	<-fired
	if !errors.Is(context.Cause(tctx), ErrorHandlerTimeout) {
		return err // ctx was cancelled by the bus shutting down
	}
	b.stuck.Add(-1)
	if err == nil {
		// The work is done, only late: running it again would do it twice. The message is acknowledged; if it
		// was redelivered meanwhile, the other run is a duplicate, as with any at-least-once delivery.
		b.Warnf(ctx, "Receiver %q finished message %q after its limit %s; it succeeded, so the message is acknowledged", msg.Values["receiver"], msg.ID, b.handlerTimeout)
		return nil
	}
	return fmt.Errorf("%w: receiver %v, message %s, limit %s: %w", ErrorHandlerTimeout, msg.Values["receiver"], msg.ID, b.handlerTimeout, err)
}

// execute starts the receiver, runs it and finishes it. A panic inside the receiver is recovered
// and returned as an error, so the message stays pending and is retried like after any failure.
func (b *Bus) execute(ctx context.Context, receiver ReceiverInterface, msg re.XMessage) (err error) {
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			b.Errorf(ctx, "Receiver %q panic recovered: %v \033[1m \033[31m%s\033[0m", msg.Values["receiver"], r, strings.ReplaceAll(string(debug.Stack()), "\n", " "))
			err = fmt.Errorf("receiver %v panicked: %v", msg.Values["receiver"], r)
		}
	}()

	// Errors are not logged here: the worker logs every failed message once, in logProcessError,
	// together with its stream id and message id.
	if bb, ok := receiver.(busBinder); ok { // every receiver that embeds Receiver
		bb.bindBus(receiver, b.inFlight.Load, b.workersCount)
	}
	ctx, err = receiver.Start(ctx, msg.Values)
	if err != nil {
		return fmt.Errorf("start receiver: %w", err)
	}
	defer receiver.Finish(ctx) // also after a panic in Execute

	return receiver.Execute(ctx)
}

// ackTimeout limits how long an acknowledgement or a dead-letter write may take.
const ackTimeout = 5 * time.Second

// ack acknowledges a message in the consumer group and counts the outcome (kinds, see stats.go) in the
// same call. A failure is counted and returned (the worker logs it): the message then stays pending and
// will be redelivered.
// The acknowledgement deliberately ignores cancellation of ctx (bus shutdown), otherwise a receiver
// that has just finished successfully would be executed again after the restart.
//
// acked is false when the message had been acknowledged already by another run (see Stats.Duplicates).
func (b *Bus) ack(ctx context.Context, msg re.XMessage, kinds ...string) (acked bool, err error) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	if b.statsOn() {
		acked, err = b.runOutcome(ackCtx, msg, true, kinds)
	} else {
		var n int64
		n, err = b.redis.XAck(ackCtx, b.stream, b.group, msg.ID)
		acked = n == 1
	}
	if err != nil {
		b.counters.ackErrors.Add(1)
		return false, fmt.Errorf("acknowledge message %s in group %q, it stays pending and will be redelivered: %w", msg.ID, b.group, err)
	}
	return acked, nil
}

// deliveryCount returns how many times Redis has delivered the pending entry id to the group
// (XREADGROUP counts the first delivery, every XCLAIM adds one), including the current one.
// pending is false when the entry is not in the pending list any more.
// An error means that the counter could not be read, which is different from an exhausted limit.
func (b *Bus) deliveryCount(ctx context.Context, id string) (deliveries int64, pending bool, err error) {
	res, err := b.redis.Client().XPendingExt(ctx, &re.XPendingExtArgs{
		Stream: b.stream,
		Group:  b.group,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		if errors.Is(err, re.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(res) == 0 || res[0].ID != id {
		return 0, false, nil
	}
	return res[0].RetryCount, true, nil
}
