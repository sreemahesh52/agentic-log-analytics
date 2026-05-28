package config_test

import (
	"os"
	"testing"

	"github.com/agentic-log-analytics/log-ingestion/config"
)

func TestLoadFromEnv_UsesDefaultsWhenEnvMissing(t *testing.T) {
	os.Unsetenv("PORT")
	os.Unsetenv("KAFKA_TOPIC")
	os.Unsetenv("RATE_LIMIT_PER_SECOND")
	os.Unsetenv("LOG_LEVEL")

	cfg := config.LoadFromEnv()

	if cfg.Port != 8082 {
		t.Errorf("expected default Port=8082, got %d", cfg.Port)
	}
	if cfg.KafkaTopic != "logs.raw" {
		t.Errorf("expected default KafkaTopic=logs.raw, got %s", cfg.KafkaTopic)
	}
	if cfg.RateLimitPerSecond != 1000 {
		t.Errorf("expected default RateLimitPerSecond=1000, got %d", cfg.RateLimitPerSecond)
	}
	if cfg.LogLevel != "INFO" {
		t.Errorf("expected default LogLevel=INFO, got %s", cfg.LogLevel)
	}
}

func TestValidate_ReturnsErrorWhenKafkaBrokersEmpty(t *testing.T) {
	cfg := &config.Config{
		Port:               8082,
		KafkaBrokers:       nil,
		RateLimitPerSecond: 1000,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty KafkaBrokers, got nil")
	}
}

func TestValidate_ReturnsNilWhenConfigValid(t *testing.T) {
	cfg := &config.Config{
		Port:               8082,
		KafkaBrokers:       []string{"localhost:9092"},
		RateLimitPerSecond: 1000,
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected nil error for valid config, got %v", err)
	}
}

func TestValidate_ReturnsErrorWhenPortInvalid(t *testing.T) {
	cfg := &config.Config{
		Port:               0,
		KafkaBrokers:       []string{"localhost:9092"},
		RateLimitPerSecond: 1000,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for Port=0, got nil")
	}
}

func TestLoadFromEnv_ParsesKafkaBrokersFromEnv(t *testing.T) {
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
	defer os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")

	cfg := config.LoadFromEnv()

	if len(cfg.KafkaBrokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(cfg.KafkaBrokers))
	}
	if cfg.KafkaBrokers[0] != "broker1:9092" {
		t.Errorf("expected broker1:9092, got %s", cfg.KafkaBrokers[0])
	}
	if cfg.KafkaBrokers[1] != "broker2:9092" {
		t.Errorf("expected broker2:9092, got %s", cfg.KafkaBrokers[1])
	}
}
