package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Default values used when environment variables are not set.
// Named constants instead of magic numbers — if we need to change the default
// port, we change it in one place, not hunt through the code.
const (
	defaultPort                = 8082
	defaultKafkaTopic          = "logs.raw"
	defaultRateLimitPerSecond  = 1000
	defaultLogLevel            = "INFO"
	defaultReadTimeoutSeconds  = 5
	defaultWriteTimeoutSeconds = 10
)

// Config holds all runtime configuration for the service.
// Every value comes from an environment variable — nothing is hardcoded
// in business logic. This satisfies
type Config struct {
	Port                int      // HTTP port to listen on
	KafkaBrokers        []string // one or more "host:port" Kafka broker addresses
	KafkaTopic          string   // Kafka topic to publish raw logs to
	RateLimitPerSecond  int      // max HTTP requests per second (token bucket)
	LogLevel            string   // DEBUG | INFO | WARN | ERROR
	ReadTimeoutSeconds  int      // max seconds to read the full HTTP request
	WriteTimeoutSeconds int      // max seconds to write the full HTTP response
}

// LoadFromEnv reads every config value from environment variables.
// Missing variables fall back to the defaults defined above.
// This is called once at startup — do not call it per-request.
func LoadFromEnv() *Config {
	return &Config{
		Port:                getEnvInt("PORT", defaultPort),
		KafkaBrokers:        getEnvStringSlice("KAFKA_BOOTSTRAP_SERVERS", ","),
		KafkaTopic:          getEnvString("KAFKA_TOPIC", defaultKafkaTopic),
		RateLimitPerSecond:  getEnvInt("RATE_LIMIT_PER_SECOND", defaultRateLimitPerSecond),
		LogLevel:            getEnvString("LOG_LEVEL", defaultLogLevel),
		ReadTimeoutSeconds:  getEnvInt("READ_TIMEOUT_SECONDS", defaultReadTimeoutSeconds),
		WriteTimeoutSeconds: getEnvInt("WRITE_TIMEOUT_SECONDS", defaultWriteTimeoutSeconds),
	}
}

// Validate checks that required fields are present and values are in valid ranges.
// Returns an error describing exactly what is wrong — the caller logs it and exits.
// "Fail fast" means we'd rather crash at startup with a clear message than silently
// misbehave at runtime.
func (c *Config) Validate() error {
	// KafkaBrokers is required — without it we cannot publish any messages.
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required and must not be empty")
	}
	// Guard against values like "kafka:9092,,kafka2:9092" (empty broker after split).
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS contains an empty broker address")
		}
	}
	// Port 0 means "pick any free port" — not safe for a server with a known address.
	// Port > 65535 is outside the valid TCP range.
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.RateLimitPerSecond <= 0 {
		return fmt.Errorf("RATE_LIMIT_PER_SECOND must be positive, got %d", c.RateLimitPerSecond)
	}
	return nil
}

// getEnvString reads a string environment variable.
// Returns defaultVal if the variable is unset or empty.
func getEnvString(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt reads an integer environment variable.
// Returns defaultVal if the variable is unset, empty, or not a valid integer.
// We silently fall back to the default rather than failing — Validate will
// catch truly invalid configs (e.g., PORT=0).
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	// strconv.Atoi converts a decimal string to int. Returns (int, error).
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal // non-integer value → fall back to default
	}
	return parsed
}

// getEnvStringSlice reads a delimited string and splits it into a slice.
// Example: KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092" → ["kafka1:9092","kafka2:9092"]
// Empty entries after splitting (e.g., trailing comma) are filtered out.
func getEnvStringSlice(key, sep string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil // nil slice means "not set" — Validate() will catch this
	}
	parts := strings.Split(val, sep)
	// make([]string, 0, len(parts)) pre-allocates capacity to avoid repeated re-allocation
	// as we append. The length is 0 (empty) but capacity is len(parts).
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
