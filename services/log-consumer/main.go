package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/agentic-log-analytics/log-consumer/config"
	"github.com/agentic-log-analytics/log-consumer/kafka"
	"github.com/agentic-log-analytics/log-consumer/metrics"
	"github.com/agentic-log-analytics/log-consumer/postgres"
)

const (
	// gracefulShutdownTimeout is how long we wait for in-flight operations to
	// complete after SIGTERM before force-closing. 10s leaves room within
	// Kubernetes' default 30s grace period.
	gracefulShutdownTimeout = 10 * time.Second

	serviceName = "log-consumer"
)

// cleanLogMessage mirrors the JSON shape published by the security middleware
// to logs.raw.clean. Fields match CleanLogMessage in Python models.py exactly.
// This struct is used only for JSON deserialization — the canonical data type
// flowing through the rest of the service is postgres.LogRecord.
type cleanLogMessage struct {
	Service            string                 `json:"service"`
	Level              string                 `json:"level"`
	Message            string                 `json:"message"`
	TraceID            string                 `json:"trace_id"`
	Metadata           map[string]interface{} `json:"metadata"`
	Timestamp          string                 `json:"timestamp"` // ISO 8601 with offset
	InjectionAttempted bool                   `json:"injection_attempted"`
	PIIFieldsRedacted  []string               `json:"pii_fields_redacted"`
}

func main() {
	// -health-check flag: when the Docker HEALTHCHECK runs "/app/server -health-check",
	// we skip normal startup and just probe /health.
	healthCheck := flag.Bool("health-check", false, "perform health check and exit")
	// flag.Parse reads os.Args and populates the registered flags above.
	flag.Parse()

	// *healthCheck dereferences the pointer to read the bool value.
	if *healthCheck {
		os.Exit(runHealthCheck())
	}

	// --- Structured logging: configured first ---
	// Build a temporary logger before config is loaded, then rebuild with the
	// configured log level once config is available.
	logger := buildLogger("INFO")

	// --- Configuration: fail fast if required vars are missing ---
	cfg := config.LoadFromEnv()
	logger = buildLogger(cfg.LogLevel)

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid_configuration", "error", err, "service", serviceName)
		os.Exit(1)
	}

	// --- PostgreSQL connection pool ---
	// context.Background for pool creation — not tied to any request lifecycle.
	pool, err := createPool(context.Background(), cfg.PostgresURL, logger)
	if err != nil {
		logger.Error("postgres_pool_failed", "error", err, "service", serviceName)
		os.Exit(1)
	}
	// defer pool.Close releases all pool connections when main exits.
	defer pool.Close()

	// --- Kafka consumer and publisher ---
	consumer, err := kafka.NewKafkaConsumer(cfg.KafkaBrokers, cfg.InputTopic, cfg.ConsumerGroupID)
	if err != nil {
		logger.Error("kafka_consumer_failed", "error", err, "service", serviceName)
		os.Exit(1)
	}
	defer consumer.Close()

	publisher, err := kafka.NewKafkaPublisher(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("kafka_publisher_failed", "error", err, "service", serviceName)
		os.Exit(1)
	}
	defer publisher.Close()

	// --- Prometheus metrics ---
	m := metrics.New()

	// --- Repository and BatchWriter ---
	repo := postgres.NewLogRepository(pool)
	batchWriter := postgres.NewBatchWriter(
		repo,
		publisher,
		cfg.OutputTopic,
		cfg.DLQTopic,
		cfg.InputTopic,
		cfg.BatchSize,
		time.Duration(cfg.FlushIntervalSeconds)*time.Second,
		cfg.MaxRetries,
		logger,
		m,
	)

	// StartFlushTimer launches the goroutine that flushes every FlushIntervalSeconds.
	// Must be called before the consumer loop so the timer is running before we
	// start adding records.
	batchWriter.StartFlushTimer()

	// --- HTTP server for /health and /metrics ---
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		handleHealth(w, consumer, publisher, logger)
	})
	// promhttp.Handler serves the default Prometheus registry as text.
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeoutSeconds) * time.Second,
	}

	// --- Start HTTP server in a goroutine ---
	// go func launches a goroutine because ListenAndServe blocks forever.
	// Without the goroutine, main would never reach the signal-listening code.
	go func() {
		logger.Info("http_server_starting", "port", cfg.Port, "service", serviceName)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http_server_failed", "error", err, "service", serviceName)
			os.Exit(1)
		}
	}()

	// --- Start consumer loop in a goroutine ---
	// context.WithCancel creates a cancellable context. cancelConsumer is called
	// on shutdown to unblock the FetchMessage call and exit the loop cleanly.
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	// defer cancelConsumer frees the context even if we return early.
	defer cancelConsumer()

	// go func runs the consumer loop concurrently with the signal listener below.
	// Without the goroutine, main would block forever in the consumer loop.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone) // signals main() that the loop has exited
		runConsumerLoop(consumerCtx, consumer, batchWriter, logger)
	}()

	logger.Info("service_started", "service", serviceName, "port", cfg.Port)

	// --- Wait for shutdown signal ---
	// Buffered channel (size 1) prevents the signal from being dropped if
	// we're not immediately ready to receive it.
	quit := make(chan os.Signal, 1)
	// signal.Notify redirects SIGTERM (Kubernetes stop) and SIGINT (Ctrl+C)
	// into our quit channel instead of the default handler.
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	// <-quit blocks main here until a signal arrives.
	<-quit

	logger.Info("shutdown_signal_received", "service", serviceName)

	// --- Graceful shutdown sequence ---
	// 1. Cancel the consumer context — FetchMessage returns with an error,
	//    the consumer loop exits, and consumerDone is closed.
	cancelConsumer()
	<-consumerDone // wait for the consumer loop to exit

	// 2. Stop the batch writer — flushes any remaining pending records.
	//    Must happen AFTER the consumer loop exits so no new records arrive
	//    during the final flush.
	batchWriter.Stop()

	// 3. Shut down the HTTP server gracefully.
	// context.WithTimeout creates a deadline so Shutdown doesn't wait forever.
	httpCtx, httpCancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	// defer httpCancel frees the context timer even if Shutdown returns early.
	defer httpCancel()

	if err := srv.Shutdown(httpCtx); err != nil {
		logger.Error("http_shutdown_failed", "error", err, "service", serviceName)
	}

	logger.Info("service_stopped_cleanly", "service", serviceName)
}

// runConsumerLoop reads messages from Kafka, parses them, and adds them to
// the BatchWriter. It runs until ctx is cancelled (shutdown signal received).
// Offset commit semantics:
//   The Kafka offset is committed immediately after the record is safely in
//   the BatchWriter's pending slice. If the service crashes after commit but
//   before the batch is flushed to PostgreSQL, the record is lost.
//   The alternative — commit only after flush — requires tracking per-message
//   offsets alongside each LogRecord, which adds significant complexity.
//   For this service, the accepted trade-off is occasional record loss on
//   crash-mid-batch in favour of simpler code. The DLQ handles persistent DB
//   failures; transient crashes are the admitted gap.
func runConsumerLoop(
	ctx context.Context,
	consumer kafka.KafkaConsumer,
	batchWriter *postgres.BatchWriter,
	logger *slog.Logger,
) {
	logger.Info("consumer_loop_started")

	for {
		// FetchMessage blocks until a message is available or ctx is cancelled.
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			// ctx.Err != nil means cancelConsumer was called — normal shutdown.
			if ctx.Err() != nil {
				logger.Info("consumer_loop_context_cancelled")
				return
			}
			// Any other error is a transient Kafka issue — log and retry.
			logger.Warn("fetch_message_failed", "error", err)
			continue
		}

		// --- Parse the raw Kafka bytes into a LogRecord ---
		record, err := parseMessage(msg.Value)
		if err != nil {
			// A malformed message cannot be fixed by redelivery — commit and skip.
			logger.Warn("parse_message_failed", "error", err)
			if commitErr := consumer.CommitMessage(ctx, msg); commitErr != nil {
				logger.Warn("commit_failed_after_parse_error", "error", commitErr)
			}
			continue
		}

		// --- Add to batch ---
		// Add is synchronous: if the batch is full, it flushes before returning.
		batchWriter.Add(*record)

		// --- Commit Kafka offset ---
		// Committed after Add confirms the record is in the pending slice.
		// This is the at-least-once pattern: on crash before commit, Kafka
		// redelivers; on crash before flush, data may be lost from the batch.
		if err := consumer.CommitMessage(ctx, msg); err != nil {
			// Commit failure is logged but not fatal — Kafka will redeliver
			// the message on the next consumer restart, causing a duplicate.
			// The deduplication in BatchWriter.flush handles this case.
			logger.Warn("commit_message_failed", "error", err)
		}
	}
}

// parseMessage deserializes a Kafka message value into a LogRecord.
// tenant_id is extracted from the metadata map (injected by the API gateway).
// Timestamp is parsed from ISO 8601 string.
func parseMessage(data []byte) (*postgres.LogRecord, error) {
	var msg cleanLogMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal cleanLogMessage: %w", err)
	}

	// --- Parse timestamp to time.Time ---
	// time.RFC3339 handles ISO 8601 strings with timezone offsets ("+00:00", "Z").
	ts, err := time.Parse(time.RFC3339, msg.Timestamp)
	if err != nil {
		// Fall back to RFC3339Nano for sub-second precision timestamps.
		ts, err = time.Parse(time.RFC3339Nano, msg.Timestamp)
		if err != nil {
			// If parsing fails entirely, use current UTC time rather than drop the record.
			// This is recoverable: the record gets an approximate timestamp.
			ts = time.Now().UTC()
		}
	}
	ts = ts.UTC()

	// --- Extract tenant_id from metadata ---
	// The API gateway injects tenant_id into metadata when forwarding to log-ingestion.
	// It propagates through logs.raw → security middleware → logs.raw.clean unchanged.
	tenantID, _ := extractStringFromMap(msg.Metadata, "tenant_id")

	return &postgres.LogRecord{
		TenantID:           tenantID,
		Service:            msg.Service,
		Level:              msg.Level,
		Message:            msg.Message,
		TraceID:            msg.TraceID,
		Timestamp:          ts,
		Metadata:           msg.Metadata,
		InjectionAttempted: msg.InjectionAttempted,
	}, nil
}

// extractStringFromMap safely reads a string value from a map[string]interface{}.
// Returns ("", false) if the key is missing or the value is not a string.
func extractStringFromMap(m map[string]interface{}, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	val, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := val.(string)
	return s, ok
}

// createPool builds a pgxpool.Pool for the given PostgreSQL URL.
func createPool(ctx context.Context, postgresURL string, logger *slog.Logger) (*pgxpool.Pool, error) {
	// pgxpool.ParseConfig parses the connection string and returns a config struct
	// that can be modified before connecting.
	poolConfig, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres URL: %w", err)
	}

	poolConfig.ConnConfig.RuntimeParams["timezone"] = "UTC"

	// Pool sizing: min_size=2 keeps two warm connections ready for bursts;
	// max_size=10 caps total connections to avoid overwhelming PostgreSQL.
	poolConfig.MinConns = 2
	poolConfig.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating pgxpool: %w", err)
	}

	// Verify the pool can actually connect by pinging the database.
	// context.WithTimeout bounds the ping to 10 seconds. Without it, pool.Ping
	// uses context.Background which never cancels — a network-unreachable DB
	// would block here for the OS TCP timeout (~2 minutes) with no log output.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	logger.Info("postgres_pool_connected", "service", serviceName)
	return pool, nil
}

// handleHealth writes a JSON health response, checking Kafka broker reachability.
func handleHealth(w http.ResponseWriter, consumer kafka.KafkaConsumer, publisher kafka.KafkaPublisher, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")

	consumerErr := consumer.HealthCheck()
	publisherErr := publisher.HealthCheck()

	if consumerErr != nil || publisherErr != nil {
		logger.Warn("health_check_degraded",
			"consumer_error", consumerErr,
			"publisher_error", publisherErr,
		)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status":"degraded","kafka":"unreachable"}`)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","kafka":"connected"}`)
}

// buildLogger creates a JSON-format structured logger at the given level.
// slog.JSONHandler writes each entry as a single JSON line — machine-parseable.
func buildLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// runHealthCheck is called when the binary runs with -health-check flag.
// Returns 0 (healthy) or 1 (unhealthy) as the process exit code.
// Docker uses this exit code to determine container health status.
func runHealthCheck() int {
	resp, err := http.Get("http://localhost:8084/health")
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		return 1
	}
	// resp.Body must be closed to return the connection to the pool.
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health check returned status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}
