package bus

import (
	"os"
	"strings"
	"testing"

	"github.com/ra-company/database/redis"
	"github.com/ra-company/env"
)

// Every test that talks to Redis runs twice, as subtests "single" and "cluster".
// A mode whose environment is not configured is skipped, so the same `go test ./...`
// works with a single node, with a cluster, or with both.
//
// Environment (the names used by .test.env):
//   - REDIS_HOST     single node, "host:port"            (REDIS_HOSTS is accepted as an alias)
//   - REDIS_CLUSTER  comma-separated cluster nodes
//   - REDIS_PASSWORD password for both modes             (REDIS_PWD is accepted as an alias)
//   - REDIS_DB       database number for a single node, default 0
const (
	modeSingle  = "single"
	modeCluster = "cluster"
)

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// redisConfigFor returns the connection settings of a mode and whether it is configured.
func redisConfigFor(mode string) (redis.Config, bool) {
	cfg := redis.Config{
		Password: firstEnv("REDIS_PASSWORD", "REDIS_PWD"),
		DB:       env.GetEnvInt("REDIS_DB", 0),
	}
	switch mode {
	case modeSingle:
		cfg.Hosts = firstEnv("REDIS_HOST", "REDIS_HOSTS")
		if strings.Contains(cfg.Hosts, ",") { // a list is a cluster, not a single node
			cfg.Hosts = ""
		}
	case modeCluster:
		cfg.Hosts = firstEnv("REDIS_CLUSTER")
		if cfg.Hosts == "" {
			if h := firstEnv("REDIS_HOSTS"); strings.Contains(h, ",") {
				cfg.Hosts = h
			}
		}
		if !strings.Contains(cfg.Hosts, ",") { // the client treats a single address as a single node
			cfg.Hosts = ""
		}
	}
	return cfg, cfg.Hosts != ""
}

// eachRedis runs fn as subtests "single" and "cluster", skipping unconfigured modes.
func eachRedis(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	for _, mode := range []string{modeSingle, modeCluster} {
		t.Run(mode, func(t *testing.T) {
			if _, ok := redisConfigFor(mode); !ok {
				t.Skipf("%s Redis is not configured (REDIS_HOST / REDIS_CLUSTER)", mode)
			}
			fn(t)
		})
	}
}

// redisMode returns the mode of the surrounding eachRedis subtest.
func redisMode(t *testing.T) string {
	t.Helper()
	for _, seg := range strings.Split(t.Name(), "/") {
		if seg == modeSingle || seg == modeCluster {
			return seg
		}
	}
	t.Fatalf("test %q must run inside eachRedis", t.Name())
	return ""
}

// redisConfig returns the settings of the mode the test runs in, skipping the test if
// that mode is not configured.
func redisConfig(t *testing.T) redis.Config {
	t.Helper()
	cfg, ok := redisConfigFor(redisMode(t))
	if !ok {
		t.Skipf("%s Redis is not configured", redisMode(t))
	}
	return cfg
}

func redisStart(t *testing.T) (*redis.RedisClient, *redis.Config) {
	t.Helper()
	cfg := redisConfig(t)
	rdb := &redis.RedisClient{}
	rdb.Start(t.Context(), &cfg)
	return rdb, &cfg
}

func Test(t *testing.T) { eachRedis(t, redisLegacy) }

func TestAddMessage_NonSerializablePayload(t *testing.T) {
	eachRedis(t, redisAddMessage_NonSerializablePayload)
}

func TestAddMessage_Success(t *testing.T) { eachRedis(t, redisAddMessage_Success) }

func TestBus_Init_Defaults(t *testing.T) { eachRedis(t, redisBus_Init_Defaults) }

func TestBus_RetryManagement(t *testing.T) { eachRedis(t, redisBus_RetryManagement) }

func TestProcessMessage_InvalidReceiverField(t *testing.T) {
	eachRedis(t, redisProcessMessage_InvalidReceiverField)
}

func TestProcessMessage_UnregisteredReceiver(t *testing.T) {
	eachRedis(t, redisProcessMessage_UnregisteredReceiver)
}

func TestProcessMessage_RetryExceeded(t *testing.T) { eachRedis(t, redisProcessMessage_RetryExceeded) }

func TestProcessMessage_StartError(t *testing.T) { eachRedis(t, redisProcessMessage_StartError) }

func TestProcessMessage_ExecuteError(t *testing.T) { eachRedis(t, redisProcessMessage_ExecuteError) }

func TestBus_GetRetry_CorruptValue(t *testing.T) { eachRedis(t, redisBus_GetRetry_CorruptValue) }

func TestDelivery_AckOnlyAfterSuccess(t *testing.T) { eachRedis(t, redisDelivery_AckOnlyAfterSuccess) }

func TestDelivery_RetryAfterFailure(t *testing.T) { eachRedis(t, redisDelivery_RetryAfterFailure) }

func TestDelivery_RetryLimit(t *testing.T) { eachRedis(t, redisDelivery_RetryLimit) }

func TestDelivery_StreamTrimmedToMaxLen(t *testing.T) {
	eachRedis(t, redisDelivery_StreamTrimmedToMaxLen)
}

func TestDelivery_UnregisteredReceiverIsAcknowledged(t *testing.T) {
	eachRedis(t, redisDelivery_UnregisteredReceiverIsAcknowledged)
}

func TestDelivery_PanicInExecute(t *testing.T) { eachRedis(t, redisDelivery_PanicInExecute) }

func TestDefect_CounterReadFailureKeepsMessagePending(t *testing.T) {
	eachRedis(t, redisDefect_CounterReadFailureKeepsMessagePending)
}

func TestDefect_RetryCountDoesNotDependOnAuxiliaryKeys(t *testing.T) {
	eachRedis(t, redisDefect_RetryCountDoesNotDependOnAuxiliaryKeys)
}

func TestDefect_ReceiverGetsMessageTime(t *testing.T) {
	eachRedis(t, redisDefect_ReceiverGetsMessageTime)
}

func TestDefect_AckErrorsAreLogged(t *testing.T) { eachRedis(t, redisDefect_AckErrorsAreLogged) }

func TestDefect_AckSurvivesContextCancellation(t *testing.T) {
	eachRedis(t, redisDefect_AckSurvivesContextCancellation)
}
