package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Default values for every config field.
// Named constants prevent magic numbers scattered through the codebase.
const (
	defaultPort                 = 8084
	defaultInputTopic           = "logs.raw.clean"
	defaultOutputTopic          = "logs.enriched"
	defaultDLQTopic             = "logs.dlq"
	defaultBatchSize            = 100
	defaultFlushIntervalSeconds = 5
	defaultLogLevel             = "INFO"
	defaultConsumerGroupID      = "log-consumer-group"
	defaultReadTimeoutSeconds   = 5
	defaultWriteTimeoutSeconds  = 10
	defaultMaxRetries            = 3
)

// Config holds all runtime configuration for the log-consumer service.
// Every field comes from an environment variable — nothing is hardcoded
// in business logic. This satisfies
type Config struct {
	Port                 int      // HTTP port for /health and /metrics
	KafkaBrokers         []string // one or more "host:port" Kafka broker addresses
	InputTopic           string   // Kafka topic to consume from (logs.raw.clean)
	OutputTopic          string   // Kafka topic to publish enriched logs to (logs.enriched)
	DLQTopic             string   // Dead-letter queue topic for failed batches (logs.dlq)
	PostgresURL          string   // Full PostgreSQL connection string with credentials
	BatchSize            int      // Max records per flush batch
	FlushIntervalSeconds int      // Max seconds between flushes even if batch is not full
	LogLevel             string   // DEBUG | INFO | WARN | ERROR
	ConsumerGroupID      string   // Kafka consumer group ID for offset tracking
	ReadTimeoutSeconds   int      // HTTP server read timeout
	WriteTimeoutSeconds  int      // HTTP server write timeout
	MaxRetries           int      // Number of BulkInsert retries before DLQ
}

// LoadFromEnv reads all config values from environment variables.
// Missing variables fall back to the typed defaults defined above.
// Called once at startup — never per-request.
func LoadFromEnv() *Config {
	return &Config{
		Port:                 getEnvInt("PORT", defaultPort),
		KafkaBrokers:         getEnvStringSlice("KAFKA_BOOTSTRAP_SERVERS", ","),
		InputTopic:           getEnvString("KAFKA_INPUT_TOPIC", defaultInputTopic),
		OutputTopic:          getEnvString("KAFKA_OUTPUT_TOPIC", defaultOutputTopic),
		DLQTopic:             getEnvString("KAFKA_DLQ_TOPIC", defaultDLQTopic),
		PostgresURL:          getEnvString("POSTGRES_URL", ""),
		BatchSize:            getEnvInt("BATCH_SIZE", defaultBatchSize),
		FlushIntervalSeconds: getEnvInt("FLUSH_INTERVAL_SECONDS", defaultFlushIntervalSeconds),
		LogLevel:             getEnvString("LOG_LEVEL", defaultLogLevel),
		ConsumerGroupID:      getEnvString("CONSUMER_GROUP_ID", defaultConsumerGroupID),
		ReadTimeoutSeconds:   getEnvInt("READ_TIMEOUT_SECONDS", defaultReadTimeoutSeconds),
		WriteTimeoutSeconds:  getEnvInt("WRITE_TIMEOUT_SECONDS", defaultWriteTimeoutSeconds),
		MaxRetries:           getEnvInt("MAX_RETRIES", defaultMaxRetries),
	}
}

// Validate checks that all required fields are present and values are in range.
// Returns a descriptive error for the first problem found — fail fast at startup.
func (c *Config) Validate() error {
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required and must not be empty")
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS contains an empty broker address")
		}
	}
	if c.PostgresURL == "" {
		return fmt.Errorf("POSTGRES_URL is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be positive, got %d", c.BatchSize)
	}
	if c.FlushIntervalSeconds <= 0 {
		return fmt.Errorf("FLUSH_INTERVAL_SECONDS must be positive, got %d", c.FlushIntervalSeconds)
	}
	if c.MaxRetries <= 0 {
		return fmt.Errorf("MAX_RETRIES must be positive, got %d", c.MaxRetries)
	}
	return nil
}

// getEnvString reads a string environment variable, returning defaultVal if unset.
func getEnvString(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt reads an integer environment variable.
// Returns defaultVal if the variable is unset, empty, or not a valid integer.
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	// strconv.Atoi converts a decimal string to int — returns error on non-numeric input.
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return parsed
}

// getEnvStringSlice splits a delimiter-separated environment variable into a slice.
// Empty tokens (e.g. from a trailing comma) are filtered out.
func getEnvStringSlice(key, sep string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, sep)
	// Pre-allocate with len(parts) capacity to avoid repeated slice growth.
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
